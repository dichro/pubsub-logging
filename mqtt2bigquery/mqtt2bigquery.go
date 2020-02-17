package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/dichro/pubsub-logging/mqtt2bigquery/parser"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpAddr   = flag.String("http_listen", ":8080", "address to listen on for http requests (addr:port)")
	mqttAddr   = flag.String("mqtt_address", "tcp://mqtt:1883", "address of MQTT broker source")
	mqttClient = flag.String("mqtt_client_id", "mqtt2bigquery", "client ID for MQTT subscription")
	mqttTopic  = flag.String("mqtt_topic", "", "source MQTT topic")
	mqttQoS    = flag.Int("mqtt_qos", 1, "qos to subscribe to topic with")
	gcpProject = flag.String("gcp_project", "", "destination GCP BigQuery project ID")
	bqTable    = flag.String("bq_table", "", "destination BigQuery table (format: dataset.table)")
	batchDelay = flag.Duration("batch_max_delay", 5*time.Minute, "maximum delay allowed to batch log entries")
	batchSize  = flag.Int("batch_max_size", 5000, "maximum number of log entries in batch")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *gcpProject)
	if err != nil {
		glog.Fatal(err)
	}
	datasetTable := strings.SplitN(*bqTable, ".", 2)
	md, err := client.Dataset(datasetTable[0]).Table(datasetTable[1]).Metadata(ctx)
	if err != nil {
		glog.Fatal(err)
	}
	parser, err := parser.NewRecord(md.Schema)
	if err != nil {
		glog.Exit(err)
	}
	opts := paho.NewClientOptions()
	opts.AddBroker(*mqttAddr)
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(false)
	opts.SetOrderMatters(false)
	opts.SetClientID(*mqttClient)
	mqtt := paho.NewClient(opts)
	glog.Infof("connecting to broker %q", *mqttAddr)
	if token := mqtt.Connect(); token.Wait() && token.Error() != nil {
		glog.Fatal(token.Error())
	}

	b := NewBuffer(md.Schema, parser)
	go b.Stream(ctx, client, *bqTable, *batchSize, *batchDelay)
	glog.Infof("subscribing to topic %q", *mqttTopic)
	token := mqtt.Subscribe(*mqttTopic, byte(*mqttQoS), b.Add)
	token.Wait()
	if err := token.Error(); err != nil {
		glog.Fatal(err)
	}
	glog.Infof("awaiting data")
	http.Handle("/metrics", promhttp.Handler())
	glog.Fatal(http.ListenAndServe(*httpAddr, nil))
}

type Buffer struct {
	schema bigquery.Schema
	parser *parser.Record
	ch     chan []bigquery.Value
}

func NewBuffer(schema bigquery.Schema, parser *parser.Record) *Buffer {
	return &Buffer{
		schema: schema,
		parser: parser,
		ch:     make(chan []bigquery.Value),
	}
}

// why is this a single-threaded call, hmm?
func (b *Buffer) Add(c paho.Client, m paho.Message) {
	js := make(map[string]interface{})
	glog.V(1).Infof("received message %s", string(m.Payload()))
	if err := json.NewDecoder(bytes.NewReader(m.Payload())).Decode(&js); err != nil {
		glog.Error(err)
		return
	}
	glog.V(1).Infof("extracted messages %#v", js)
	msg, err := b.parser.ParseAsRecord(js)
	if err != nil {
		glog.Error(err)
		return
	}
	b.ch <- msg
	glog.V(1).Info("done with message")
}

func (b *Buffer) Stream(ctx context.Context, bq *bigquery.Client, table string, maxBatch int, maxDelay time.Duration) {
	tableParts := strings.Split(table, ".")
	ins := bq.Dataset(tableParts[0]).Table(tableParts[1]).Inserter()
	batchArray := make([]*bigquery.ValuesSaver, 0, maxBatch)
	var timeout *time.Ticker
	var ch <-chan time.Time
	for {
		// set up a new slice that will reuse the existing array
		batch := batchArray[0:0]
		first := true
		for {
			select {
			case msg := <-b.ch:
				if first {
					timeout = time.NewTicker(maxDelay)
					ch = timeout.C
					first = false
				}
				batch = append(batch, &bigquery.ValuesSaver{
					Schema: b.schema,
					Row:    msg,
				})
				if len(batch) < cap(batch) {
					continue
				}
			case <-ch:
				// drop out
			}
			timeout.Stop()
			glog.Infof("sending batch size %d of max %d to %s", len(batch), cap(batch), table)
			// TODO(miki): make a rowID out of MQTT message ID and distinct fields in message.
			if err := ins.Put(ctx, batch); err != nil {
				glog.Error(err)
			}
			break
		}
	}
}
