package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ukpabik/HermesMQ/internal/client"
	"github.com/ukpabik/HermesMQ/internal/protocol"
)

type Topic struct {
	Name        string
	Subscribers map[string]*client.Client
	Mutex       sync.Mutex
}

func (t *Topic) AddClient(c *client.Client) (bool, error) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.Subscribers[c.ID] = c
	_, ok := t.Subscribers[c.ID]
	if !ok {
		return false, fmt.Errorf("error adding client to topic")
	}
	return true, nil
}

func (t *Topic) removeClient(c *client.Client) (bool, error) {
	delete(t.Subscribers, c.ID)
	_, ok := t.Subscribers[c.ID]
	if ok {
		return false, fmt.Errorf("error removing client from topic")
	}
	return true, nil
}

func (t *Topic) Broadcast(payload protocol.Payload, sender *client.Client) error {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling broadcast payload: %v", err)
	}

	for id, sub := range t.Subscribers {
		if sender != nil && id == sender.ID {
			continue
		}

		go func(sub *client.Client, subID string) {
			sub.Connection.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_, err := sub.Connection.Write(data)
			if err != nil {
				log.Printf("error writing to subscriber %s: %v", id, err)
			}
		}(sub, id)
	}
	return nil
}
