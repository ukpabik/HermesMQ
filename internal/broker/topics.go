package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ukpabik/HermesMQ/internal/client"
	"github.com/ukpabik/HermesMQ/internal/protocol"
	"github.com/ukpabik/HermesMQ/internal/redis"
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

func (t *Topic) Broadcast(msg *protocol.Payload, excludeClientID string) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}

	t.Mutex.Lock()
	subscribers := make([]*client.Client, 0, len(t.Subscribers))
	for _, sub := range t.Subscribers {
		if sub.ID == excludeClientID {
			continue
		}
		subscribers = append(subscribers, sub)
	}
	t.Mutex.Unlock()

	if len(subscribers) == 0 {
		log.Printf("no subscribers for topic %s", t.Name)
		return nil
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	data = append(data, '\n')

	var wg sync.WaitGroup
	errorChan := make(chan error, len(subscribers))

	for _, sub := range subscribers {
		wg.Add(1)
		go func(s *client.Client) {
			defer wg.Done()

			s.Mutex.Lock()
			defer s.Mutex.Unlock()

			if s.Connection == nil {
				errorChan <- fmt.Errorf("subscriber %s has nil connection", s.ID)
				return
			}

			if err := s.Connection.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				errorChan <- fmt.Errorf("set deadline for %s: %w", s.ID, err)
				return
			}

			written, err := s.Connection.Write(data)
			if err != nil {
				errorChan <- fmt.Errorf("write to subscriber %s: %w", s.ID, err)
				return
			}

			if written != len(data) {
				errorChan <- fmt.Errorf("incomplete write to %s: %d/%d bytes", s.ID, written, len(data))
			}

			go func() {
				if err := redis.StoreOffset(msg.Topic, s.ID, int64(time.Now().UnixNano())); err != nil {
					log.Printf("failed to update offset for %s: %v", s.ID, err)
				}
			}()
		}(sub)
	}

	wg.Wait()
	close(errorChan)

	var errors []error
	for err := range errorChan {
		if err != nil {
			errors = append(errors, err)
			log.Printf("broadcast error: %v", err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("broadcast had %d errors", len(errors))
	}

	log.Printf("successfully broadcast to %d subscribers on topic %s", len(subscribers), t.Name)
	return nil
}
