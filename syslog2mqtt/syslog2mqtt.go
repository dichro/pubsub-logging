package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sort"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	syslog "gopkg.in/mcuadros/go-syslog.v2"
)

var (
	messageCount = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "syslog",
		Name:      "received",
		Help:      "count of syslog messages received",
	})
	dropCount = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "syslog",
		Name:      "discard",
		Help:      "count of syslog messages discarded",
	})
	publishers = prometheus.NewGauge(prometheus.GaugeOpts{
		Subsystem: "mqtt",
		Name:      "publish_outstanding",
		Help:      "count of outstanding publish calls",
	})
	publishLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "mqtt",
		Name:      "publish_latency",
		Help:      "latency of publish calls",
	}, []string{"result"})

	syslogAddr = flag.String("syslog_listen", ":514", "address to listen on for syslog messages (addr:port)")
	httpAddr   = flag.String("http_listen", ":8080", "address to listen on for http requests (addr:port)")
	mqttAddr   = flag.String("mqtt_address", "tcp://mqtt:1883", "address of MQTT broker")
	mqttTopic  = flag.String("mqtt_topic", "syslog/raw/json", "MQTT topic to publish raw syslog messages")
)

func init() {
	prometheus.MustRegister(messageCount)
	prometheus.MustRegister(dropCount)
	prometheus.MustRegister(publishers)
	prometheus.MustRegister(publishLatency)
}

func main() {
	flag.Parse()
	defer glog.Flush()
	ch := make(syslog.LogPartsChannel)
	h := syslog.NewChannelHandler(ch)

	s := syslog.NewServer()
	s.SetFormat(syslog.Automatic)
	s.SetHandler(h)
	if err := s.ListenUDP(*syslogAddr); err != nil {
		glog.Fatal(err)
	}
	if err := s.Boot(); err != nil {
		glog.Fatal(err)
	}

	opts := paho.NewClientOptions()
	opts.AddBroker(*mqttAddr)
	opts.SetAutoReconnect(true)
	mqtt := paho.NewClient(opts)
	if token := mqtt.Connect(); token.Wait() && token.Error() != nil {
		glog.Fatal(token.Error())
	}

	go func() {
		for msg := range ch {
			messageCount.Inc()
			msg["ReceivedTimestamp"] = time.Now()
			var buf bytes.Buffer
			if err := json.NewEncoder(&buf).Encode(msg); err != nil {
				dropCount.Inc()
				glog.Error(err)
				continue
			}
			go func() {
				publishers.Inc()
				defer publishers.Dec()
				start := time.Now()
				token := mqtt.Publish(*mqttTopic, 1, false, buf.Bytes())
				token.Wait()
				elapsed := time.Now().Sub(start)
				result := "OK"
				if err := token.Error(); err != nil {
					glog.Error(err)
					result = "error"
				}
				publishLatency.WithLabelValues(result).Observe(float64(elapsed / time.Second))
			}()
			if glog.V(1) {
				fmt.Println(time.Now())
				keys := make([]string, 0, len(msg))
				for key := range msg {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, k := range keys {
					fmt.Printf("  %s: %q\n", k, msg[k])
				}
			}
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	glog.Fatal(http.ListenAndServe(*httpAddr, nil))
}
