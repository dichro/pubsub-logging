package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/http"
	"os"

	"github.com/bio-routing/tflow2/config"
	"github.com/bio-routing/tflow2/netflow"
	"github.com/bio-routing/tflow2/nfserver"
	"github.com/bio-routing/tflow2/srcache"
	"github.com/dichro/pubsub-logging/pub"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	paho "github.com/eclipse/paho.mqtt.golang"
)

var (
	agentName = flag.String("agent_name", "agent", "name of Netflow agent")
	agentIP   = flag.String("agent_ip", "", "ip of Netflow agent")
	agentSR   = flag.Int64("agent_sample_rate", 1, "sampling rate for Netflow agent")
	nfAddr    = flag.String("netflow_listen", ":2055", "[address]:port to listen for Netflow v9 packets on")
	httpAddr  = flag.String("http_listen", ":8080", "[address]:port to listen on for http requests")
	mqttAddr  = flag.String("mqtt_address", "tcp://mqtt:1883", "address of MQTT broker")
	mqttTopic = flag.String("mqtt_topic", "ipfix/raw/json", "MQTT topic to publish raw IPFIX messages")
)

func main() {
	flag.Parse()
	enabled := true
	agents := []config.Agent{
		{Name: *agentName, IPAddress: *agentIP, SampleRate: uint64(*agentSR)},
	}
	nameByIP := make(map[string]string)
	for _, a := range agents {
		nameByIP[a.IPAddress] = a.Name
	}
	logrus.SetOutput(os.Stderr)
	logrus.SetLevel(logrus.TraceLevel)
	s := nfserver.New(10, &config.Config{
		NetflowV9: &config.Server{
			Enabled: &enabled,
			Listen:  *nfAddr,
		},
		BGPAugmentation: &config.BGPAugment{},
		Agents:          agents,
		AgentsNameByIP:  nameByIP,
	}, srcache.New(agents))

	opts := paho.NewClientOptions()
	opts.AddBroker(*mqttAddr)
	opts.SetAutoReconnect(true)
	mqtt := paho.NewClient(opts)
	if token := mqtt.Connect(); token.Wait() && token.Error() != nil {
		logrus.Fatal(token.Error())
	}
	logrus.Info("connected")
	go decode(pub.New(mqtt, 1, false), s.Output)

	http.Handle("/metrics", promhttp.Handler())
	logrus.Fatal(http.ListenAndServe(*httpAddr, nil))
}

var (
	messageCount = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "ipfix",
		Name:      "received",
		Help:      "count of messages received",
	})
	dropCount = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: "ipfix",
		Name:      "discard",
		Help:      "count of messages discarded",
	})
)

func init() {
	prometheus.MustRegister(messageCount)
	prometheus.MustRegister(dropCount)
}

func decode(p *pub.Publisher, ch <-chan *netflow.Flow) {
	for msg := range ch {
		messageCount.Inc()
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(msg); err != nil {
			dropCount.Inc()
			logrus.Error(err)
			continue
		}
		go p.Publish(*mqttTopic, buf.Bytes())
	}
}
