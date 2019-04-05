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
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpAddr   = flag.String("http_listen", ":8080", "address to listen on for http requests (addr:port)")
	mqttAddr   = flag.String("mqtt_address", "tcp://mqtt:1883", "address of MQTT broker")
	mqttClient = flag.String("mqtt_client_id", "mqtt2bigquery", "client ID for MQTT subscription")
	mqttTopic  = flag.String("mqtt_topic", "syslog/raw/json", "MQTT topic for raw syslog messages")
	gcpProject = flag.String("gcp_project", "", "GCP BigQuery project ID to stream logs to")
	bqTable    = flag.String("bq_table", "", "BigQuery table for syslog data (format: dataset.table)")
	batchDelay = flag.Duration("batch_max_delay", 5*time.Minute, "maximum delay allowed to batch log entries")
	batchSize  = flag.Int("batch_max_size", 5000, "maximum number of log entries in batch")
)

type Message struct {
	ReceivedTimestamp time.Time

	Client          string    `json:"client"`
	ClientTimestamp time.Time `json:"timestamp"`
	ClientHostname  string    `json:"hostname"`

	Tag     string `json:"tag"`
	Message string `json:"content"`

	Priority int `json:"priority"`
	Severity int `json:"severity"`
	Facility int `json:"facility"`
}

func main() {
	flag.Parse()
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

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *gcpProject)
	if err != nil {
		glog.Fatal(err)
	}
	b := NewBuffer()
	go b.Stream(ctx, client, *bqTable, *batchSize, *batchDelay)
	glog.Infof("subscribing to topic %q", *mqttTopic)
	token := mqtt.Subscribe(*mqttTopic, 1, b.Add)
	token.Wait()
	if err := token.Error(); err != nil {
		glog.Fatal(err)
	}

	glog.Infof("awaiting data")
	http.Handle("/metrics", promhttp.Handler())
	glog.Fatal(http.ListenAndServe(*httpAddr, nil))
}

type Buffer struct {
	ch chan Message
}

func NewBuffer() *Buffer {
	return &Buffer{
		ch: make(chan Message),
	}
}

// why is this a single-threaded call, hmm?
func (b *Buffer) Add(c paho.Client, m paho.Message) {
	var msg Message
	glog.V(1).Infof("received message %s", string(m.Payload()))
	if err := json.NewDecoder(bytes.NewReader(m.Payload())).Decode(&msg); err != nil {
		glog.Error(err)
		return
	}
	glog.V(1).Infof("extracted messages %#v", msg)
	b.ch <- msg
	glog.V(1).Info("done with message")
}

func (b *Buffer) Stream(ctx context.Context, bq *bigquery.Client, table string, maxBatch int, maxDelay time.Duration) {
	tableParts := strings.Split(table, ".")
	ins := bq.Dataset(tableParts[0]).Table(tableParts[1]).Inserter()
	batchArray := make([]Message, 0, maxBatch)
	for {
		// set up a new slice that will reuse the existing array
		batch := batchArray[0:0]
		// wait for first message of the batch
		batch = append(batch, <-b.ch)
		timeout := time.NewTicker(maxDelay)
		for {
			select {
			case msg := <-b.ch:
				batch = append(batch, msg)
				if len(batch) < cap(batch) {
					continue
				}
			case <-timeout.C:
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
