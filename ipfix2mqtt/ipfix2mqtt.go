package main

import (
	"flag"
	"os"

	"github.com/bio-routing/tflow2/config"
	"github.com/bio-routing/tflow2/nfserver"
	"github.com/bio-routing/tflow2/srcache"
	"github.com/sirupsen/logrus"
)

var (
	agentName = flag.String("agent_name", "agent", "name of Netflow agent")
	agentIP   = flag.String("agent_ip", "", "ip of Netflow agent")
	agentSR   = flag.Int64("agent_sample_rate", 1, "sampling rate for agent")
	listen    = flag.String("listen", ":2055", "[address]:port to listen for Netflow v9 packets on")
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
			Listen:  *listen,
		},
		BGPAugmentation: &config.BGPAugment{},
		Agents:          agents,
		AgentsNameByIP:  nameByIP,
	}, srcache.New(agents))
	for f := range s.Output {
		nfserver.Dump(f)
	}
}
