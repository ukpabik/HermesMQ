package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ukpabik/HermesMQ/internal/client"
	"github.com/ukpabik/HermesMQ/internal/protocol"
)

type Broker struct {
	Port string

	Listener           *net.TCPListener
	CurrentConnections map[string]*client.Client
	Mutex              sync.Mutex
	TopicMap           map[string]*Topic
	MessageQueue       *PriorityMessageQueue
}

const (
	publishState     = "publish"
	subscribeState   = "subscribe"
	unsubscribeState = "unsubscribe"
)

func InitializeBroker(port string) *Broker {
	connectionsMap := make(map[string]*client.Client)
	topicMap := make(map[string]*Topic)
	mq := NewPriorityMessageQueue()
	return &Broker{
		Port:               port,
		TopicMap:           topicMap,
		CurrentConnections: connectionsMap,
		MessageQueue:       mq,
	}
}

func (b *Broker) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", b.Port)
	if err != nil {
		return fmt.Errorf("unable to start broker at port %s: %w", b.Port, err)
	}

	b.Listener = listener.(*net.TCPListener)
	defer b.Listener.Close()

	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		b.Listener.Close()
	}()

	go b.DequeueLoop(ctx)
	go b.ReadMessageLoop(ctx)

	for {
		cl, err := b.Listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				log.Printf("broker shutting down...")
				wg.Wait()
				b.cleanupConnections()
				return nil
			default:
				log.Printf("unable to accept client: %v", err)
				continue
			}
		}

		clientId := uuid.New().String()

		// TODO: Add more to this once user grows....
		user := &client.Client{
			ID:         clientId,
			Connection: cl,
		}

		b.Mutex.Lock()
		b.CurrentConnections[clientId] = user
		b.Mutex.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.handleClientConnection(user)
		}()
	}
}

func (b *Broker) handleClientConnection(cl *client.Client) {
	defer cl.Close()

	scanner := bufio.NewScanner(cl.Connection)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		line := scanner.Bytes()

		clientResp := &protocol.Payload{}
		err := json.Unmarshal(line, &clientResp)
		if err != nil {
			log.Printf("invalid JSON from client %s: %v", cl.ID, err)
			continue
		}

		log.Printf("received from client %s: %+v", cl.ID, clientResp)

		switch clientResp.Action {
		case publishState:
			go func(payload protocol.Payload) {
				if err := b.handleClientPublish(payload, cl); err != nil {
					log.Printf("❌ [%s] publish error: %v", cl.ID, err)
					b.sendACK(fmt.Sprintf("publish failed: %v", err), "error", payload.Topic, cl)
				}
			}(*clientResp)

		case subscribeState:
			go func(payload protocol.Payload) {
				if err := b.handleClientSubscribe(payload, cl); err != nil {
					log.Printf("❌ [%s] subscribe error: %v", cl.ID, err)
					b.sendACK(fmt.Sprintf("subscribe failed: %v", err), "error", payload.Topic, cl)
				}
			}(*clientResp)

		case unsubscribeState:
			go func(payload protocol.Payload) {
				if err := b.handleClientUnsubscribe(payload, cl); err != nil {
					log.Printf("❌ [%s] unsubscribe error: %v", cl.ID, err)
					b.sendACK(fmt.Sprintf("unsubscribe failed: %v", err), "error", payload.Topic, cl)
				}
			}(*clientResp)

		default:
			log.Printf("⚠️  [%s] unknown action: %s", cl.ID, clientResp.Action)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("scanner error for client %s: %v", cl.ID, err)
	}

	b.handleClientDisconnection(cl)
}

func parseTopicName(topicName string) (string, error) {

	strippedTopicName := strings.TrimSpace(topicName)

	if strippedTopicName == "" {
		return "", fmt.Errorf("empty topic name")
	}

	return strippedTopicName, nil
}

func (b *Broker) handleClientPublish(payload protocol.Payload, cl *client.Client) error {
	if cl == nil {
		return fmt.Errorf("client is nil")
	}

	parsedName, err := parseTopicName(payload.Topic)
	if err != nil {
		return fmt.Errorf("name malformed: %v", err)
	}

	b.Mutex.Lock()
	topic, ok := b.TopicMap[parsedName]
	if !ok {
		log.Printf("topic doesn't exist... creating topic")
		newMap := make(map[string]*client.Client)
		topic = &Topic{
			Name:        parsedName,
			Subscribers: newMap,
		}
		b.TopicMap[parsedName] = topic
	}
	topicName := topic.Name
	b.Mutex.Unlock()
	b.MessageQueue.Enqueue(&payload)

	go func() {
		if err := b.sendACK("Successfully published!", publishState, topicName, cl); err != nil {
			log.Printf("unable to send ACK to client: %v", err)
		}
	}()

	return nil
}

func (b *Broker) handleClientSubscribe(payload protocol.Payload, cl *client.Client) error {
	if cl == nil {
		return fmt.Errorf("client is nil")
	}

	parsedName, err := parseTopicName(payload.Topic)
	if err != nil {
		return fmt.Errorf("name malformed: %v", err)
	}

	b.Mutex.Lock()
	topic, ok := b.TopicMap[parsedName]
	if !ok {
		log.Printf("topic doesn't exist... creating topic")
		newMap := make(map[string]*client.Client)
		topic = &Topic{
			Name:        parsedName,
			Subscribers: newMap,
		}
		b.TopicMap[parsedName] = topic
	}
	topicName := topic.Name
	b.Mutex.Unlock()

	if ok, err := topic.addClient(cl); err != nil || !ok {
		return fmt.Errorf("unable to subscribe to topic: %v", err)
	}

	go func() {
		if err := b.sendACK("Successfully subscribed!", subscribeState, topicName, cl); err != nil {
			log.Printf("unable to send ACK to client: %v", err)
		}
	}()

	return nil
}

func (b *Broker) handleClientUnsubscribe(payload protocol.Payload, cl *client.Client) error {
	if cl == nil {
		return fmt.Errorf("client is nil")
	}

	parsedName, err := parseTopicName(payload.Topic)
	if err != nil {
		return fmt.Errorf("name malformed: %v", err)
	}

	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	topic, ok := b.TopicMap[parsedName]
	if !ok {
		return fmt.Errorf("topic does not exist")
	}

	if ok, err := topic.removeClient(cl); err != nil || !ok {
		return fmt.Errorf("unable to unsubscribe to topic: %v", err)
	}

	go func() {
		if err := b.sendACK("Successfully unsubscribed!", unsubscribeState, topic.Name, cl); err != nil {
			log.Printf("unable to send ACK to client: %v", err)
		}
	}()

	return nil
}

func (b *Broker) handleClientDisconnection(cl *client.Client) {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	for _, topic := range b.TopicMap {
		if ok, err := topic.removeClient(cl); !ok || err != nil {
			log.Printf("unable to remove client from topic %v: %v", topic.Name, err)
		}
	}

	delete(b.CurrentConnections, cl.ID)
}

func (b *Broker) cleanupConnections() {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	for id, cl := range b.CurrentConnections {
		log.Printf("closing connection for client %s", id)
		cl.Connection.Close()
		delete(b.CurrentConnections, id)
	}
}

func (b *Broker) sendACK(body, action, topic string, cl *client.Client) error {
	ackPayload := &protocol.Payload{
		Action:    action + "_ack",
		Topic:     topic,
		Body:      body,
		Timestamp: time.Now().UTC(),
	}

	data, err := json.Marshal(ackPayload)
	if err != nil {
		return fmt.Errorf("unable to marshal payload: %v", err)
	}

	cl.Mutex.Lock()
	defer cl.Mutex.Unlock()
	data = append(data, '\n')

	cl.Connection.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = cl.Connection.Write(data)
	if err != nil {
		return fmt.Errorf("error writing to subscriber %s: %v", cl.ID, err)
	}

	return nil
}

func (b *Broker) DequeueLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(b.MessageQueue.ReadChannel)
			return
		case <-ticker.C:
			payload, ok := b.MessageQueue.Dequeue()
			if ok {
				b.MessageQueue.ReadChannel <- *payload
			}
		}
	}
}

func (b *Broker) ReadMessageLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case val := <-b.MessageQueue.ReadChannel:
			log.Printf("received message: %v", val.Body)

			b.Mutex.Lock()
			topic, exists := b.TopicMap[val.Topic]
			b.Mutex.Unlock()

			if exists {
				topic.Broadcast(val, nil)
			}
		}
	}
}
