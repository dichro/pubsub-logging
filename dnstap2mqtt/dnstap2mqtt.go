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
	dnstap "github.com/dnstap/golang-dnstap"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	dnstapAddr  = flag.String("dnstap_listen", ":8000", "TCP address to listen for dnstap messages on")
	httpAddr    = flag.String("http_listen", ":8080", "address to listen on for http requests (addr:port)")
	mqttAddr    = flag.String("mqtt_address", "tcp://mqtt:1883", "address of MQTT broker")
	rawTopic    = flag.String("mqtt_topic_raw", "dnstap/raw/json", "MQTT topic to publish raw dnstap messages")
	cookedTopic = flag.String("mqtt_topic_cooked", "dnstap/cooked/json", "MQTT topic to publish more useful dnstap messages")

	messageCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: "dnstap",
		Name:      "received",
		Help:      "count of dnstap messages received",
	}, []string{"result"})
)

func init() {
	prometheus.MustRegister(messageCount)
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

type DNSTap struct {
	SocketFamily   *dnstap.SocketFamily
	SocketProtocol *dnstap.SocketProtocol
	Message        dns.Msg
	Timestamp      time.Time
}

func decode(p *pub.Publisher, ch <-chan []byte) {
	defer glog.Exit("done")
	for buf := range ch {
		var msg dnstap.Dnstap
		if err := proto.Unmarshal(buf, &msg); err != nil {
			glog.Error(err)
			messageCount.WithLabelValues("unmarshal").Inc()
			continue
		}
		if len(*rawTopic) > 0 {
			var buf bytes.Buffer
			if err := json.NewEncoder(&buf).Encode(msg); err != nil {
				messageCount.WithLabelValues("encode-raw").Inc()
				glog.Error(err)
				continue
			}
			go p.Publish(*rawTopic, buf.Bytes())
		}
		dt := DNSTap{
			SocketFamily:   msg.Message.SocketFamily,
			SocketProtocol: msg.Message.SocketProtocol,
		}
		var msgBuf []byte
		switch msg.Message.GetType() {
		case dnstap.Message_AUTH_QUERY, dnstap.Message_CLIENT_QUERY, dnstap.Message_FORWARDER_QUERY,
			dnstap.Message_RESOLVER_QUERY, dnstap.Message_STUB_QUERY, dnstap.Message_TOOL_QUERY:
			msgBuf = msg.Message.GetQueryMessage()
			dt.Timestamp = time.Unix(int64(msg.Message.GetQueryTimeSec()), int64(msg.Message.GetQueryTimeNsec()))
		case dnstap.Message_AUTH_RESPONSE, dnstap.Message_CLIENT_RESPONSE, dnstap.Message_FORWARDER_RESPONSE,
			dnstap.Message_RESOLVER_RESPONSE, dnstap.Message_STUB_RESPONSE, dnstap.Message_TOOL_RESPONSE:
			msgBuf = msg.Message.GetResponseMessage()
			dt.Timestamp = time.Unix(int64(msg.Message.GetResponseTimeSec()), int64(msg.Message.GetResponseTimeNsec()))
		default:
			messageCount.WithLabelValues("type-unknown").Inc()
			continue
		}

		if err := dt.Message.Unpack(msgBuf); err != nil {
			glog.Error(err)
			messageCount.WithLabelValues("unpack-query").Inc()
			continue
		}
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(dt); err != nil {
			glog.Error(err)
			messageCount.WithLabelValues("encode-cooked").Inc()
			continue
		}
		go p.Publish(*cookedTopic, buf.Bytes())
		if glog.V(1) {
			fmt.Println(time.Now())
			fmt.Printf("%#v\n", msg)
		}
		messageCount.WithLabelValues("OK").Inc()
	}
}
