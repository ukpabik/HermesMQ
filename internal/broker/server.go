package broker

import (
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
	readBufferSize   = 1024
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

func (b *Broker) handleClientConnection(user *client.Client) {
	// Add the client to the list of current connections
	defer user.Connection.Close()

	readBuffer := make([]byte, readBufferSize)
	for {
		byteAmt, err := user.Connection.Read(readBuffer)
		if err != nil {
			log.Printf("unable to read bytes from client: %v", err)
			b.handleClientDisconnection(user)
			break
		}
		readBuffer = readBuffer[:byteAmt]

		clientResp := &protocol.Payload{}
		err = json.Unmarshal(readBuffer, &clientResp)
		if err != nil {
			log.Printf("unable to marshal client response to json: %v", err)
		}

		// TODO: Handle client actions
		switch clientResp.Action {
		case publishState:
			log.Printf("publish received")
			go b.handleClientPublish(*clientResp, user)
		case subscribeState:
			log.Printf("subscribe received")
		case unsubscribeState:
			log.Printf("unsubscribe received")
		default:
			log.Printf("undefined action state received: %v", clientResp.Action)
		}
	}
}

func parseTopicName(topicName string) (string, error) {

	strippedTopicName := strings.TrimSpace(topicName)

	if strippedTopicName == "" {
		return "", fmt.Errorf("empty topic name")
	}

	return strippedTopicName, nil
}

func (b *Broker) handleClientPublish(payload protocol.Payload, user *client.Client) {
	if user == nil {
		log.Printf("client is nil")
		return
	}

	parsedName, err := parseTopicName(payload.Topic)
	if err != nil {
		log.Printf("name malformed: %v", err)
		return
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
	b.Mutex.Unlock()

	log.Printf("publishing message to topic: %s", parsedName)
	topic.Broadcast(payload, user)
}

func (b *Broker) handleClientDisconnection(user *client.Client) {
	// Remove from all topics
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	for _, topic := range b.TopicMap {
		topic.Mutex.Lock()
		if ok, err := topic.removeClient(user); !ok || err != nil {
			log.Printf("unable to remove client from topic %v: %v", topic.Name, err)
		}
		topic.Mutex.Unlock()
	}

	delete(b.CurrentConnections, user.ID)
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
