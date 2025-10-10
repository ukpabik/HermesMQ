package client

import (
	"bufio"
	"encoding/json"
	"log"

	"github.com/ukpabik/HermesMQ/internal/protocol"
)

func (c *Client) tcpReadLoop() {
	reader := bufio.NewScanner(c.Connection)
	reader.Split(bufio.ScanLines)

	for reader.Scan() {
		line := reader.Bytes()
		var payload protocol.Payload
		if err := json.Unmarshal(line, &payload); err != nil {
			log.Printf("invalid server message: %v", err)
			continue
		}

		c.ReadChannel <- payload
	}

	if err := reader.Err(); err != nil {
		log.Printf("socket read error: %v", err)
	}

	close(c.ReadChannel)
}

func (c *Client) chanReadLoop() {
	for val := range c.ReadChannel {
		log.Printf("received payload from topic %s", val.Topic)
	}
	log.Println("read loop stopped")
}
