// This program receives dnstap messages and publishes them as JSON on an MQTT topic.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/dichro/pubsub-logging/pub"
	"github.com/dnstap/golang-dnstap"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	dnstapAddr = flag.String("dnstap_listen", ":8000", "TCP address to listen for dnstap messages on")
	httpAddr   = flag.String("http_listen", ":8080", "address to listen on for http requests (addr:port)")
	mqttAddr   = flag.String("mqtt_address", "tcp://mqtt:1883", "address of MQTT broker")
	mqttTopic  = flag.String("mqtt_topic", "dnstap/raw/json", "MQTT topic to publish dnstap messages")

	messageCount = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "dnstap",
		Name:      "received",
		Help:      "count of dnstap messages received",
	})
	dropCount = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "dnstap",
		Name:      "discard",
		Help:      "count of dnstap messages discarded",
	})
)

func init() {
	prometheus.MustRegister(messageCount)
	prometheus.MustRegister(dropCount)
}

func main() {
	flag.Parse()
	defer glog.Flush()
	l, err := net.Listen("tcp", *dnstapAddr)
	if err != nil {
		glog.Exit(err)
	}
	ch := make(chan []byte)
	go dnstap.NewFrameStreamSockInput(l).ReadInto(ch)

	opts := paho.NewClientOptions()
	opts.AddBroker(*mqttAddr)
	opts.SetAutoReconnect(true)
	mqtt := paho.NewClient(opts)
	if token := mqtt.Connect(); token.Wait() && token.Error() != nil {
		glog.Fatal(token.Error())
	}

	go decode(pub.New(mqtt, 1, false), ch)
	http.Handle("/metrics", promhttp.Handler())
	glog.Fatal(http.ListenAndServe(*httpAddr, nil))
}

func decode(p *pub.Publisher, ch <-chan []byte) {
	defer glog.Exit("done")
	for buf := range ch {
		messageCount.Inc()
		var msg dnstap.Dnstap
		if err := proto.Unmarshal(buf, &msg); err != nil {
			glog.Error(err)
			continue
		}
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(msg); err != nil {
			dropCount.Inc()
			glog.Error(err)
			continue
		}
		go p.Publish(*mqttTopic, buf.Bytes())
		if glog.V(1) {
			fmt.Println(time.Now())
			fmt.Printf("%#v\n", msg)
		}
	}
}
