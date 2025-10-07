package client

import (
	"net"
	"sync"
)

type Client struct {
	Connection       net.Conn
	ID               string
	Mutex            sync.Mutex
	SubscribedTopics []string
}
