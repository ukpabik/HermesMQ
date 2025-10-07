package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ukpabik/HermesMQ/internal/client"
)

type Broker struct {
	Port string

	Listener           *net.TCPListener
	CurrentConnections map[string]*client.Client
	Mutex              sync.Mutex
	TopicMap           map[string]*Topic
}

type ClientResponse struct {
	Action    string    `json:"action"`
	Topic     string    `json:"topic"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

const (
	readBufferSize = 1024
	publishState   = "publish"
	subscribeState = "subscribe"
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

		// TODO: Add more to this once client grows....
		client := &client.Client{
			ID:         clientId,
			Connection: cl,
		}

		b.Mutex.Lock()
		b.CurrentConnections[clientId] = client
		b.Mutex.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.handleClientConnection(client)
		}()
	}
}

func (b *Broker) handleClientConnection(client *client.Client) {
	// Add the client to the list of current connections
	defer client.Connection.Close()

	readBuffer := make([]byte, readBufferSize)
	for {
		byteAmt, err := client.Connection.Read(readBuffer)
		if err != nil {
			// TODO: Handle client disconnect
			log.Printf("unable to read bytes from client: %v", err)
			b.handleClientDisconnection(client)
			break
		}
		readBuffer = readBuffer[:byteAmt]

		clientResp := &ClientResponse{}
		err = json.Unmarshal(readBuffer, &clientResp)
		if err != nil {
			log.Printf("unable to marshal client response to json: %v", err)
		}

		// TODO: Handle client actions
		switch clientResp.Action {
		case publishState:
			log.Printf("publish received")
		case subscribeState:
			log.Printf("subscribe received")
		default:
			log.Printf("undefined action state received: %v", clientResp.Action)
		}
	}
}

func (b *Broker) handleClientDisconnection(client *client.Client) {
	// Remove from all topics
	b.Mutex.Lock()
	defer b.Mutex.Unlock()
	for _, topic := range b.TopicMap {
		topic.Mutex.Lock()
		for i, cl := range topic.Connections {
			if cl.ID == client.ID {
				topic.Connections = slices.Delete(topic.Connections, i, i)
				break
			}
		}
		topic.Mutex.Unlock()
	}

	delete(b.CurrentConnections, client.ID)
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
