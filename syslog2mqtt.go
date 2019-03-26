package main

import (
	"fmt"

	"github.com/golang/glog"
	syslog "gopkg.in/mcuadros/go-syslog.v2"
)

func main() {
	ch := make(syslog.LogPartsChannel)
	h := syslog.NewChannelHandler(ch)

	s := syslog.NewServer()
	s.SetFormat(syslog.Automatic)
	s.SetHandler(h)
	if err := s.ListenUDP(":1514"); err != nil {
		glog.Fatal(err)
	}
	if err := s.Boot(); err != nil {
		glog.Fatal(err)
	}

	go func() {
		for msg := range ch {
			fmt.Println(msg)
		}
	}()

	s.Wait()
}
