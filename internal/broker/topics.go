package broker

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/ukpabik/HermesMQ/internal/client"
	"github.com/ukpabik/HermesMQ/internal/protocol"
)

type Topic struct {
	Name        string
	Subscribers map[string]*client.Client
	Mutex       sync.Mutex
}

func (t *Topic) AddClient(c *client.Client) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.Subscribers[c.ID] = c
}

func (t *Topic) removeClient(c *client.Client) (bool, error) {
	delete(t.Subscribers, c.ID)
	return true, nil
}

func (t *Topic) Broadcast(payload protocol.Payload, sender *client.Client) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()

	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("error marshaling broadcast payload: %v", err)
		return
	}

	for id, sub := range t.Subscribers {
		if sender != nil && id == sender.ID {
			continue
		}

		_, err := sub.Connection.Write(data)
		if err != nil {
			log.Printf("error writing to subscriber %s: %v", id, err)
			continue
		}
	}
}
