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
}

const (
	publishState     = "publish"
	subscribeState   = "subscribe"
	unsubscribeState = "unsubscribe"
)

func InitializeBroker(port string) *Broker {
	connectionsMap := make(map[string]*client.Client)
	topicMap := make(map[string]*Topic)
	return &Broker{
		Port:               port,
		TopicMap:           topicMap,
		CurrentConnections: connectionsMap,
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
	defer cl.Connection.Close()

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
			log.Printf("publish received from client %s", cl.ID)
			go b.handleClientPublish(*clientResp, cl)
		case subscribeState:
			log.Printf("subscribe received from client %s", cl.ID)
			go b.handleClientSubscribe(*clientResp, cl)
		case unsubscribeState:
			log.Printf("unsubscribe received from client %s", cl.ID)
			go b.handleClientUnsubscribe(*clientResp, cl)
		default:
			log.Printf("undefined action state received from client %s: %v", cl.ID, clientResp.Action)
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
	defer b.Mutex.Unlock()
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

	log.Printf("publishing message to topic: %s", parsedName)
	if err := topic.Broadcast(payload, cl); err != nil {
		return fmt.Errorf("error while broadcasting: %v", err)
	}

	// TODO: Send ACK to client

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
	defer b.Mutex.Unlock()
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

	if ok, err := topic.addClient(cl); err != nil || !ok {
		return fmt.Errorf("unable to subscribe to topic: %v", err)
	}

	// TODO: Send ACK to client

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
		return fmt.Errorf("unable to subscribe to topic: %v", err)
	}

	// TODO: Send ACK to client

	return nil
}

func (b *Broker) handleClientDisconnection(cl *client.Client) {
	// Remove from all topics
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	for _, topic := range b.TopicMap {
		topic.Mutex.Lock()
		if ok, err := topic.removeClient(cl); !ok || err != nil {
			log.Printf("unable to remove client from topic %v: %v", topic.Name, err)
		}
		topic.Mutex.Unlock()
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
