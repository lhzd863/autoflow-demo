package module

import (
	"net"
)

type MetaInstancePortBean1 struct {
	Ip           string       `json:"ip"`
	Port         string       `json:"port"`
	FlowId       string       `json:"flowid"`
	HttpListener net.Listener `json:"httplistener"`
	Enable       string       `json:"enable"`
}
