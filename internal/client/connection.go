package client

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ukpabik/HermesMQ/internal/protocol"
)

type Client struct {
	Connection       net.Conn
	ID               string
	Mutex            sync.Mutex
	SubscribedTopics map[string]struct{}
	ReadChannel      chan protocol.Payload
	StopReadChannel  chan bool
}

func sendBytes(payload protocol.Payload, cl *Client) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("unable to marshal payload into bytes: %v", err)
	}
	payloadBytes = append(payloadBytes, '\n')

	totalBytes, err := cl.Connection.Write(payloadBytes)
	if err != nil || totalBytes == 0 {
		return fmt.Errorf("unable to write bytes: %v", err)
	}

	return nil
}

func (c *Client) Subscribe(topicName string) error {
	c.Mutex.Lock()
	if c.SubscribedTopics == nil {
		c.SubscribedTopics = make(map[string]struct{})
	}
	c.SubscribedTopics[topicName] = struct{}{}
	defer c.Mutex.Unlock()

	payload := &protocol.Payload{
		Action:    "subscribe",
		Topic:     topicName,
		Timestamp: time.Now().UTC(),
	}

	if err := sendBytes(*payload, c); err != nil {
		return fmt.Errorf("unable to send payload bytes to server: %v", err)
	}

	if len(c.SubscribedTopics) == 1 {
		c.startReaders()
	}

	return nil
}

func (c *Client) Unsubscribe(topicName string) error {
	c.Mutex.Lock()
	delete(c.SubscribedTopics, topicName)
	defer c.Mutex.Unlock()

	payload := &protocol.Payload{
		Action:    "unsubscribe",
		Topic:     topicName,
		Timestamp: time.Now().UTC(),
	}

	if err := sendBytes(*payload, c); err != nil {
		return fmt.Errorf("unable to send payload bytes to server: %v", err)
	}

	if len(c.SubscribedTopics) == 0 {
		close(c.StopReadChannel)
	}

	return nil
}

func (c *Client) HasSubscribed(topicName string) bool {
	c.Mutex.Lock()
	_, ok := c.SubscribedTopics[topicName]
	c.Mutex.Unlock()
	return ok
}

func (c *Client) startReaders() {
	if c.ReadChannel == nil {
		c.ReadChannel = make(chan protocol.Payload)
	}

	if c.StopReadChannel == nil {
		c.StopReadChannel = make(chan bool)
	}

	go c.tcpReadLoop()
	go c.chanReadLoop()
}
