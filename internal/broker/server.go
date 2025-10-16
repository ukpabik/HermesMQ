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
	"github.com/ukpabik/HermesMQ/internal/db"
	"github.com/ukpabik/HermesMQ/internal/protocol"
	"github.com/ukpabik/HermesMQ/internal/redis"
)

type Broker struct {
	Port string

	Listener           *net.TCPListener
	CurrentConnections map[string]*clientWrapper
	Mutex              sync.Mutex
	TopicMap           map[string]*Topic
	MessageQueue       *PriorityMessageQueue
}

type clientWrapper struct {
	Client *client.Client
	Ctx    context.Context
	Cancel context.CancelFunc
}

const (
	publishState     = "publish"
	subscribeState   = "subscribe"
	unsubscribeState = "unsubscribe"
	errorState       = "error"

	clientReadTimeout  = 30 * time.Second
	clientWriteTimeout = 5 * time.Second
	shutdownTimeout    = 10 * time.Second
)

func InitializeBroker(port string) *Broker {
	connectionsMap := make(map[string]*clientWrapper)
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
		log.Println("shutdown signal received, closing listener...")
		b.Listener.Close()
	}()

	go b.DequeueLoop(ctx)
	go b.ReadMessageLoop(ctx)

	for {
		cl, err := b.Listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				log.Println("broker shutting down...")

				b.signalAllClientsShutdown()

				b.forceCloseAllConnections()

				shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
				defer cancel()

				done := make(chan struct{})
				go func() {
					wg.Wait()
					close(done)
				}()

				select {
				case <-done:
					log.Println("all client handlers finished gracefully")
				case <-shutdownCtx.Done():
					log.Println("shutdown timeout reached, forcing cleanup")
				}

				b.cleanupConnections()
				return nil
			default:
				log.Printf("unable to accept client: %v", err)
				continue
			}
		}

		clientId := uuid.New().String()

		user, err := client.InitializeClient(clientId, cl)
		if err != nil {
			log.Printf("error initializing client: %v", err)
			cl.Close()
			continue
		}

		clientCtx, clientCancel := context.WithCancel(ctx)
		wrapper := &clientWrapper{
			Client: user,
			Ctx:    clientCtx,
			Cancel: clientCancel,
		}

		b.Mutex.Lock()
		b.CurrentConnections[clientId] = wrapper
		b.Mutex.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.handleClientConnection(wrapper)
		}()
	}
}

func (b *Broker) handleClientConnection(wrapper *clientWrapper) {
	cl := wrapper.Client
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in client connection handler for %s: %v", cl.ID, r)
		}
		b.handleClientDisconnection(wrapper)
	}()

	if err := cl.Connection.SetReadDeadline(time.Now().Add(clientReadTimeout)); err != nil {
		log.Printf("failed to set read deadline for client %s: %v", cl.ID, err)
		return
	}

	scanner := bufio.NewScanner(cl.Connection)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		select {
		case <-wrapper.Ctx.Done():
			log.Printf("client %s context cancelled, stopping handler", cl.ID)
			return
		default:
		}

		if err := cl.Connection.SetReadDeadline(time.Now().Add(clientReadTimeout)); err != nil {
			log.Printf("failed to reset read deadline for client %s: %v", cl.ID, err)
			return
		}

		line := scanner.Bytes()

		clientResp := &protocol.Payload{}
		if err := json.Unmarshal(line, &clientResp); err != nil {
			log.Printf("invalid JSON from client %s: %v", cl.ID, err)
			if err := b.sendACK(wrapper, fmt.Sprintf("Invalid JSON: %v", err), errorState, ""); err != nil {
				log.Printf("failed to send error ACK: %v", err)
			}
			continue
		}
		clientResp.SenderID = cl.ID

		log.Printf("received from client %s: %+v", cl.ID, clientResp)

		var err error
		switch clientResp.Action {
		case publishState:
			err = b.handleClientPublish(wrapper, *clientResp)
		case subscribeState:
			err = b.handleClientSubscribe(wrapper, *clientResp)
		case unsubscribeState:
			err = b.handleClientUnsubscribe(wrapper, *clientResp)
		default:
			log.Printf("unknown action '%s' from client %s", clientResp.Action, cl.ID)
			err = b.sendACK(wrapper, fmt.Sprintf("Unknown action: %s", clientResp.Action), errorState, clientResp.Topic)
		}

		if err != nil {
			log.Printf("handler error for client %s: %v", cl.ID, err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("scanner error for client %s: %v", cl.ID, err)
	}
}

func parseTopicName(topicName string) (string, error) {
	strippedTopicName := strings.TrimSpace(topicName)

	if strippedTopicName == "" {
		return "", fmt.Errorf("empty topic name")
	}

	return strippedTopicName, nil
}

func (b *Broker) handleClientPublish(wrapper *clientWrapper, payload protocol.Payload) error {
	cl := wrapper.Client
	if cl == nil {
		if err := b.sendACK(wrapper, "Client is nil", errorState, payload.Topic); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("client is nil")
	}

	select {
	case <-wrapper.Ctx.Done():
		if err := b.sendACK(wrapper, "Client disconnected", errorState, payload.Topic); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("client disconnected")
	default:
	}

	parsedName, err := parseTopicName(payload.Topic)
	if err != nil {
		if err := b.sendACK(wrapper, fmt.Sprintf("Invalid topic name: %v", err), errorState, payload.Topic); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("name malformed: %v", err)
	}

	b.Mutex.Lock()
	topic, ok := b.TopicMap[parsedName]
	if !ok {
		log.Printf("topic doesn't exist... creating topic %s", parsedName)
		topic = &Topic{
			Name:        parsedName,
			Subscribers: make(map[string]*client.Client),
		}
		b.TopicMap[parsedName] = topic
	}
	topicName := topic.Name
	b.Mutex.Unlock()

	b.MessageQueue.Enqueue(&payload)
	if err := redis.StorePayload(payload); err != nil {
		log.Printf("unable to store payload in redis: %v", err)
	}

	if err := db.AddMessageToDB(payload); err != nil {
		log.Printf("unable to store payload in db: %v", err)
	}

	if err := b.sendACK(wrapper, "Successfully published!", publishState, topicName); err != nil {
		log.Printf("unable to send ACK to client %s: %v", cl.ID, err)
		return err
	}

	return nil
}

func (b *Broker) handleClientSubscribe(wrapper *clientWrapper, payload protocol.Payload) error {
	cl := wrapper.Client
	if cl == nil {
		if err := b.sendACK(wrapper, "Client is nil", errorState, payload.Topic); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("client is nil")
	}

	select {
	case <-wrapper.Ctx.Done():
		if err := b.sendACK(wrapper, "Client disconnected", errorState, payload.Topic); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("client disconnected")
	default:
	}

	parsedName, err := parseTopicName(payload.Topic)
	if err != nil {
		if err := b.sendACK(wrapper, fmt.Sprintf("Invalid topic name: %v", err), errorState, payload.Topic); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("name malformed: %v", err)
	}

	b.Mutex.Lock()
	topic, ok := b.TopicMap[parsedName]
	if !ok {
		log.Printf("topic doesn't exist... creating topic %s", parsedName)
		topic = &Topic{
			Name:        parsedName,
			Subscribers: make(map[string]*client.Client),
		}
		b.TopicMap[parsedName] = topic
	}
	topicName := topic.Name
	b.Mutex.Unlock()

	if ok, err := topic.addClient(cl); err != nil || !ok {
		if err := b.sendACK(wrapper, fmt.Sprintf("Failed to subscribe: %v", err), errorState, topicName); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("unable to subscribe to topic: %v", err)
	}

	if err := b.sendACK(wrapper, "Successfully subscribed!", subscribeState, topicName); err != nil {
		log.Printf("unable to send ACK to client %s: %v", cl.ID, err)
		return err
	}

	if err := redis.StoreOffset(topicName, cl.ID, 0); err != nil {
		log.Printf("warning: failed to store offset: %v", err)
	}

	return nil
}

func (b *Broker) handleClientUnsubscribe(wrapper *clientWrapper, payload protocol.Payload) error {
	cl := wrapper.Client
	if cl == nil {
		if err := b.sendACK(wrapper, "Client is nil", errorState, payload.Topic); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("client is nil")
	}

	select {
	case <-wrapper.Ctx.Done():
		if err := b.sendACK(wrapper, "Client disconnected", errorState, payload.Topic); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("client disconnected")
	default:
	}

	parsedName, err := parseTopicName(payload.Topic)
	if err != nil {
		if err := b.sendACK(wrapper, fmt.Sprintf("Invalid topic name: %v", err), errorState, payload.Topic); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("name malformed: %v", err)
	}

	b.Mutex.Lock()
	topic, ok := b.TopicMap[parsedName]
	b.Mutex.Unlock()

	if !ok {
		if err := b.sendACK(wrapper, "Topic does not exist", errorState, parsedName); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return fmt.Errorf("topic does not exist")
	}

	isEmpty, err := topic.removeClient(cl)
	if err != nil {
		if err := b.sendACK(wrapper, fmt.Sprintf("Failed to unsubscribe: %v", err), errorState, topic.Name); err != nil {
			log.Printf("unable to send ACK: %v", err)
		}
		return err
	}

	if isEmpty {
		b.Mutex.Lock()
		delete(b.TopicMap, parsedName)
		b.Mutex.Unlock()
	}

	if err := b.sendACK(wrapper, "Successfully unsubscribed!", unsubscribeState, parsedName); err != nil {
		log.Printf("unable to send ACK to client %s: %v", cl.ID, err)
		return err
	}

	return nil
}

func (b *Broker) handleClientDisconnection(wrapper *clientWrapper) {
	cl := wrapper.Client

	wrapper.Cancel()

	time.Sleep(100 * time.Millisecond)

	b.Mutex.Lock()
	for _, topic := range b.TopicMap {
		if ok, err := topic.removeClient(cl); !ok || err != nil {
			log.Printf("unable to remove client from topic %v: %v", topic.Name, err)
		}
	}

	delete(b.CurrentConnections, cl.ID)
	b.Mutex.Unlock()

	cl.Close()
	log.Printf("client %s disconnected and cleaned up", cl.ID)
}

func (b *Broker) signalAllClientsShutdown() {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	for _, wrapper := range b.CurrentConnections {
		wrapper.Cancel()
	}
}

func (b *Broker) forceCloseAllConnections() {
	b.Mutex.Lock()
	connections := make([]net.Conn, 0, len(b.CurrentConnections))
	for _, wrapper := range b.CurrentConnections {
		if wrapper.Client != nil && wrapper.Client.Connection != nil {
			connections = append(connections, wrapper.Client.Connection)
		}
	}
	b.Mutex.Unlock()

	for _, conn := range connections {
		conn.Close()
	}
}

func (b *Broker) cleanupConnections() {
	b.Mutex.Lock()
	clients := make([]*client.Client, 0, len(b.CurrentConnections))
	for _, wrapper := range b.CurrentConnections {
		clients = append(clients, wrapper.Client)
	}
	b.CurrentConnections = make(map[string]*clientWrapper)
	b.Mutex.Unlock()

	for _, cl := range clients {
		_ = cl.Close()
	}

	log.Println("all connections cleaned up")
}

func (b *Broker) sendACK(wrapper *clientWrapper, body, action, topic string) error {
	if wrapper == nil || wrapper.Client == nil {
		return fmt.Errorf("wrapper or client is nil")
	}

	cl := wrapper.Client

	var payloadType protocol.Type
	if action == errorState {
		payloadType = protocol.Error
	} else {
		payloadType = protocol.ACK
	}

	select {
	case <-wrapper.Ctx.Done():
		return fmt.Errorf("client disconnected, skipping ACK")
	default:
	}

	ackPayload := &protocol.Payload{
		Action:    action + "_ack",
		Type:      payloadType,
		Topic:     topic,
		Body:      body,
		Timestamp: time.Now().UTC(),
		SenderID:  "broker",
	}

	data, err := json.Marshal(ackPayload)
	if err != nil {
		return fmt.Errorf("unable to marshal payload: %v", err)
	}

	cl.Mutex.Lock()
	defer cl.Mutex.Unlock()

	if cl.Connection == nil {
		return fmt.Errorf("connection already closed")
	}

	data = append(data, '\n')

	cl.Connection.SetWriteDeadline(time.Now().Add(clientWriteTimeout))
	_, err = cl.Connection.Write(data)
	if err != nil {
		return fmt.Errorf("error writing to subscriber %s: %v", cl.ID, err)
	}

	return nil
}

func (b *Broker) DequeueLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(b.MessageQueue.ReadChannel)
			log.Println("dequeue loop stopped")
			return

		case <-b.MessageQueue.Notify():
			for {
				payload, ok := b.MessageQueue.Dequeue()
				if !ok {
					break
				}
				select {
				case <-ctx.Done():
					close(b.MessageQueue.ReadChannel)
					log.Println("dequeue loop stopped")
					return
				case b.MessageQueue.ReadChannel <- *payload:
				}
			}
		}
	}
}

func (b *Broker) ReadMessageLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("read message loop stopped")
			return
		case val, ok := <-b.MessageQueue.ReadChannel:
			if !ok {
				log.Println("message queue channel closed")
				return
			}
			log.Printf("received message: %v", val.Body)

			b.Mutex.Lock()
			topic, exists := b.TopicMap[val.Topic]
			b.Mutex.Unlock()

			if exists {
				if err := topic.Broadcast(&val, val.SenderID); err != nil {
					log.Printf("unable to broadcast: %v", err)
				}
			}
		}
	}
}
