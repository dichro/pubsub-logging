// This program receives dnstap messages and publishes them as JSON on an MQTT topic.
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"

	"github.com/dnstap/golang-dnstap"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	dnstapAddr = flag.String("dnstap_listen", ":8000", "TCP address to listen for dnstap messages on")
	httpAddr   = flag.String("http_listen", ":8080", "address to listen on for http requests (addr:port)")
)

func main() {
	flag.Parse()
	defer glog.Flush()
	l, err := net.Listen("tcp", *dnstapAddr)
	if err != nil {
		glog.Exit(err)
	}
	ch := make(chan []byte)
	go decode(ch)
	go dnstap.NewFrameStreamSockInput(l).ReadInto(ch)
	http.Handle("/metrics", promhttp.Handler())
	glog.Fatal(http.ListenAndServe(*httpAddr, nil))
}

func decode(ch <-chan []byte) {
	defer glog.Exit("done")
	for buf := range ch {
		var msg dnstap.Dnstap
		if err := proto.Unmarshal(buf, &msg); err != nil {
			glog.Error(err)
			continue
		}
		fmt.Printf("%#v\n", msg)
		fmt.Printf("%#v\n", msg.Message)
	}
}
