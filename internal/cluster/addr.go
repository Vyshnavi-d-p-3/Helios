package cluster

import (
	"fmt"
	"net"
	"strings"
)

// RaftBindAdvertise splits listen vs replica/advertise addresses for hashicorp/raft TCP transport.
// If raftAddr is ":7000", bind listens on all interfaces and advertise defaults to "127.0.0.1:7000".
func RaftBindAdvertise(raftAddr string) (bind, advertise string, err error) {
	raftAddr = strings.TrimSpace(raftAddr)
	if raftAddr == "" {
		return "", "", fmt.Errorf("empty raft addr")
	}
	if strings.HasPrefix(raftAddr, ":") {
		return raftAddr, "127.0.0.1" + raftAddr, nil
	}
	host, port, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", "", fmt.Errorf("raft addr: %w", err)
	}
	_ = host
	return ":" + port, raftAddr, nil
}

// HTTPAdvertise returns the host:port clients and followers use to reach this node's HTTP API.
// When HTTP is ":8080", the host is taken from raftAdvertise so Docker/service DNS names match.
func HTTPAdvertise(httpAddr, raftAdvertise string) (string, error) {
	httpAddr = strings.TrimSpace(httpAddr)
	raftAdvertise = strings.TrimSpace(raftAdvertise)
	rh, _, err := net.SplitHostPort(raftAdvertise)
	if err != nil {
		return "", fmt.Errorf("raft advertise: %w", err)
	}
	var hh, hp string
	if strings.HasPrefix(httpAddr, ":") {
		hh = ""
		hp = strings.TrimPrefix(httpAddr, ":")
	} else {
		hh, hp, err = net.SplitHostPort(httpAddr)
		if err != nil {
			return "", fmt.Errorf("http addr: %w", err)
		}
	}
	if hh == "" || hh == "0.0.0.0" {
		hh = rh
	}
	if hh == "" {
		hh = "127.0.0.1"
	}
	return net.JoinHostPort(hh, hp), nil
}
