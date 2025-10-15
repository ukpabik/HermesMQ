package client

import (
	"bufio"
	"encoding/json"
	"log"

	"github.com/ukpabik/HermesMQ/internal/protocol"
)

func (c *Client) tcpReadLoop() {
	defer c.readersStopped.Done()

	c.Mutex.Lock()
	conn := c.Connection
	stopCh := c.stopReaders
	c.Mutex.Unlock()

	if conn == nil || stopCh == nil {
		return
	}

	reader := bufio.NewScanner(conn)
	reader.Split(bufio.ScanLines)

	for reader.Scan() {
		select {
		case <-stopCh:
			return
		default:
		}

		line := reader.Bytes()
		var payload protocol.Payload
		if err := json.Unmarshal(line, &payload); err != nil {
			log.Printf("invalid server message: %v, \n%v", err, string(line))
			continue
		}

		c.Mutex.Lock()
		readCh := c.ReadChannel
		c.Mutex.Unlock()

		if readCh == nil {
			return
		}

		select {
		case readCh <- payload:
		case <-stopCh:
			return
		}
	}

	if err := reader.Err(); err != nil {
		log.Printf("socket read error: %v", err)
	}
}

func (c *Client) chanReadLoop() {
	defer c.readersStopped.Done()
	for {
		select {
		case <-c.stopReaders:
			return
		case msg, ok := <-c.ReadChannel:
			if !ok {
				return
			}
			c.handleMessage(msg)
		}
	}
}

func (c *Client) handleMessage(msg protocol.Payload) {
	switch msg.Type {
	case protocol.ACK:
		log.Printf("✅ ACK [%s]: %v", msg.Topic, msg.Body)
	case protocol.Error:
		log.Printf("❌ ERROR [%s]: %v", msg.Topic, msg.Body)
	case protocol.Data:
		log.Printf("📨 MESSAGE [%s] from %s: %v", msg.Topic, msg.SenderID, msg.Body)
	default:
		log.Printf("📩 [%s]: %v", msg.Topic, msg.Body)
	}
}
