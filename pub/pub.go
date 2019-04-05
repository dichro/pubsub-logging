// Package pub contains utilities for MQTT publishers.
package pub

import (
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	register   sync.Once
	publishers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: "mqtt",
		Name:      "publish_outstanding",
		Help:      "count of outstanding publish calls",
	}, []string{"topic"})
	publishLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: "mqtt",
		Name:      "publish_latency",
		Help:      "latency of publish calls",
	}, []string{"topic", "result"})
)

// Publisher wraps a paho MQTT client with publishing-related Prometheus metrics.
type Publisher struct {
	client   paho.Client
	qos      byte
	retained bool
}

// New returns a new Publisher configured with the provided qos and retention.
func New(client paho.Client, qos byte, retained bool) *Publisher {
	register.Do(func() {
		prometheus.MustRegister(publishers)
		prometheus.MustRegister(publishLatency)
	})
	return &Publisher{
		client:   client,
		qos:      qos,
		retained: retained,
	}
}

// Publish will block until the given message is published on the given topic.
// This can and should be called in a new goroutine; errors will be
// glog.Error()'d.
func (p *Publisher) Publish(topic string, message []byte) {
	publishers.WithLabelValues(topic).Inc()
	defer publishers.WithLabelValues(topic).Dec()
	start := time.Now()
	token := p.client.Publish(topic, p.qos, p.retained, message)
	token.Wait()
	elapsed := time.Now().Sub(start)
	result := "OK"
	if err := token.Error(); err != nil {
		glog.Error(err)
		result = "error"
	}
	publishLatency.WithLabelValues(topic, result).Observe(float64(elapsed / time.Second))
}
