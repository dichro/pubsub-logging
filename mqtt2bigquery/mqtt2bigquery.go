package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpAddr  = flag.String("http_listen", ":8080", "address to listen on for http requests (addr:port)")
	mqttAddr  = flag.String("mqtt_address", "tcp://mqtt:1883", "address of MQTT broker")
	mqttTopic = flag.String("mqtt_topic", "syslog/raw/json", "MQTT topic for raw syslog messages")
	bqProject = flag.String("bq_project", "", "GCP BigQuery project ID to stream logs to")
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
	mqtt := paho.NewClient(opts)
	glog.Infof("connecting to broker %q", *mqttAddr)
	if token := mqtt.Connect(); token.Wait() && token.Error() != nil {
		glog.Fatal(token.Error())
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, *bqProject)
	if err != nil {
		glog.Fatal(err)
	}
	b := NewBuffer()
	go b.Stream(ctx, client, 500, time.Minute)
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

func (b *Buffer) Stream(ctx context.Context, bq *bigquery.Client, maxBatch int, maxDelay time.Duration) {
	ins := bq.Dataset("logs").Table("syslog").Inserter()
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
			glog.Infof("sending batch size %d of max %d", len(batch), cap(batch))
			if err := ins.Put(ctx, batch); err != nil {
				glog.Error(err)
			}
			break
		}
	}
}
