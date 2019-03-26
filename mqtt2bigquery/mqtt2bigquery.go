package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/http"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpAddr  = flag.String("http_listen", ":8080", "address to listen on for http requests (addr:port)")
	mqttAddr  = flag.String("mqtt_address", "tcp://mqtt:1883", "address of MQTT broker")
	mqttTopic = flag.String("mqtt_topic", "syslog/raw/json", "MQTT topic for raw syslog messages")
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

	glog.Infof("subscribing to topic %q", *mqttTopic)
	token := mqtt.Subscribe(*mqttTopic, 1, handler)
	token.Wait()
	if err := token.Error(); err != nil {
		glog.Fatal(err)
	}

	glog.Infof("awaiting data")
	http.Handle("/metrics", promhttp.Handler())
	glog.Fatal(http.ListenAndServe(*httpAddr, nil))
}

func handler(c paho.Client, m paho.Message) {
	var log Message
	if err := json.NewDecoder(bytes.NewReader(m.Payload())).Decode(&log); err != nil {
		glog.Error(err)
	}
	glog.V(1).Infof("received message %s", string(m.Payload()))
	glog.V(1).Infof("extracted messages %#v", log)
}
