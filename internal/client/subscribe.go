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
