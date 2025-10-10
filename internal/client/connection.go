package client

import (
	"encoding/json"
	"errors"
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
}

var (
	ErrNotConnected      = errors.New("client not connected")
	ErrAlreadySubscribed = errors.New("already subscribed to topic")
)

func sendBytes(payload protocol.Payload, cl *Client) error {
	if cl == nil || cl.Connection == nil {
		return fmt.Errorf("client or connection is nil")
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	payloadBytes = append(payloadBytes, '\n')

	cl.Mutex.Lock()
	defer cl.Mutex.Unlock()

	if err := cl.Connection.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}

	written, err := cl.Connection.Write(payloadBytes)
	if err != nil {
		return fmt.Errorf("write to connection: %w", err)
	}
	if written != len(payloadBytes) {
		return fmt.Errorf("incomplete write: wrote %d/%d bytes", written, len(payloadBytes))
	}

	return nil
}

func (c *Client) Subscribe(topicName string) error {
	c.Mutex.Lock()
	if c.SubscribedTopics == nil {
		c.SubscribedTopics = make(map[string]struct{})
	}
	if _, exists := c.SubscribedTopics[topicName]; exists {
		c.Mutex.Unlock()
		return nil
	}

	shouldStartReaders := len(c.SubscribedTopics) == 0
	c.SubscribedTopics[topicName] = struct{}{}
	c.Mutex.Unlock()

	payload := &protocol.Payload{
		Action:    "subscribe",
		Topic:     topicName,
		Timestamp: time.Now().UTC(),
	}

	if err := sendBytes(*payload, c); err != nil {
		c.Mutex.Lock()
		delete(c.SubscribedTopics, topicName)
		c.Mutex.Unlock()
		return fmt.Errorf("subscribe to %s: %w", topicName, err)
	}

	if shouldStartReaders {
		c.startReaders()
	}

	return nil
}

func (c *Client) Unsubscribe(topicName string) error {
	c.Mutex.Lock()
	if _, exists := c.SubscribedTopics[topicName]; !exists {
		c.Mutex.Unlock()
		return nil
	}
	delete(c.SubscribedTopics, topicName)
	c.Mutex.Unlock()

	payload := &protocol.Payload{
		Action:    "unsubscribe",
		Topic:     topicName,
		Timestamp: time.Now().UTC(),
	}

	if err := sendBytes(*payload, c); err != nil {
		return fmt.Errorf("unsubscribe from %s: %w", topicName, err)
	}

	return nil
}

func (c *Client) startReaders() {
	if c.ReadChannel == nil {
		c.ReadChannel = make(chan protocol.Payload)
	}

	go c.tcpReadLoop()
	go c.chanReadLoop()
}

func (c *Client) IsConnected() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.Connection != nil
}

func (c *Client) Close() error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.Connection == nil {
		return nil
	}

	err := c.Connection.Close()
	c.Connection = nil
	c.SubscribedTopics = nil

	if c.ReadChannel != nil {
		close(c.ReadChannel)
		c.ReadChannel = nil
	}

	return err
}
