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

func (t *Topic) addClient(c *client.Client) (bool, error) {
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
	t.Mutex.Lock()
	defer t.Mutex.Unlock()

	delete(t.Subscribers, c.ID)
	isEmpty := len(t.Subscribers) == 0

	return isEmpty, nil
}

func (t *Topic) Broadcast(payload protocol.Payload, senderID string) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling broadcast payload: %v", err)
	}
	data = append(data, '\n')

	t.Mutex.Lock()
	subscribers := make([]*client.Client, 0, len(t.Subscribers))
	for id, sub := range t.Subscribers {
		if senderID != "" && id == senderID {
			continue
		}
		subscribers = append(subscribers, sub)
	}
	t.Mutex.Unlock()

	sema := make(chan struct{}, 10)

	var wg sync.WaitGroup
	for _, sub := range subscribers {
		wg.Add(1)

		sema <- struct{}{}
		go func(cl *client.Client) {
			defer wg.Done()
			defer func() { <-sema }()

			cl.Mutex.Lock()
			conn := cl.Connection
			cl.Mutex.Unlock()

			if conn == nil {
				return
			}

			if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				log.Printf("set deadline error for %s: %v", cl.ID, err)
				return
			}

			if _, err := conn.Write(data); err != nil {
				log.Printf("write error to %s: %v", cl.ID, err)
			}
		}(sub)
	}

	wg.Wait()
	return nil
}
