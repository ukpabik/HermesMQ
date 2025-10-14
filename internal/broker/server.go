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
		log.Printf("cleaning up client %s", cl.ID)
		b.handleClientDisconnection(wrapper)
	}()

	cl.Connection.SetReadDeadline(time.Now().Add(clientReadTimeout))

	scanner := bufio.NewScanner(cl.Connection)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		select {
		case <-wrapper.Ctx.Done():
			log.Printf("client %s context cancelled, stopping handler", cl.ID)
			return
		default:
		}

		cl.Connection.SetReadDeadline(time.Now().Add(clientReadTimeout))

		line := scanner.Bytes()

		clientResp := &protocol.Payload{}
		err := json.Unmarshal(line, &clientResp)
		if err != nil {
			log.Printf("invalid JSON from client %s: %v", cl.ID, err)
			continue
		}
		clientResp.SenderID = cl.ID

		log.Printf("received from client %s: %+v", cl.ID, clientResp)

		switch clientResp.Action {
		case publishState:
			go func(payload protocol.Payload) {
				if err := b.handleClientPublish(wrapper, payload); err != nil {
					log.Printf("❌ [%s] publish error: %v", cl.ID, err)
					b.sendACK(wrapper, fmt.Sprintf("publish failed: %v", err), "error", payload.Topic)
				}
			}(*clientResp)

		case subscribeState:
			go func(payload protocol.Payload) {
				if err := b.handleClientSubscribe(wrapper, payload); err != nil {
					log.Printf("❌ [%s] subscribe error: %v", cl.ID, err)
					b.sendACK(wrapper, fmt.Sprintf("subscribe failed: %v", err), "error", payload.Topic)
				}
			}(*clientResp)

		case unsubscribeState:
			go func(payload protocol.Payload) {
				if err := b.handleClientUnsubscribe(wrapper, payload); err != nil {
					log.Printf("❌ [%s] unsubscribe error: %v", cl.ID, err)
					b.sendACK(wrapper, fmt.Sprintf("unsubscribe failed: %v", err), "error", payload.Topic)
				}
			}(*clientResp)

		default:
			log.Printf("⚠️  [%s] unknown action: %s", cl.ID, clientResp.Action)
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
		return fmt.Errorf("client is nil")
	}

	select {
	case <-wrapper.Ctx.Done():
		return fmt.Errorf("client disconnected")
	default:
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
		if err := b.sendACK(wrapper, "Successfully published!", publishState, topicName); err != nil {
			log.Printf("unable to send ACK to client: %v", err)
		}
	}()

	return nil
}

func (b *Broker) handleClientSubscribe(wrapper *clientWrapper, payload protocol.Payload) error {
	cl := wrapper.Client
	if cl == nil {
		return fmt.Errorf("client is nil")
	}

	select {
	case <-wrapper.Ctx.Done():
		return fmt.Errorf("client disconnected")
	default:
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
		if err := b.sendACK(wrapper, "Successfully subscribed!", subscribeState, topicName); err != nil {
			log.Printf("unable to send ACK to client: %v", err)
		}
	}()

	return nil
}

func (b *Broker) handleClientUnsubscribe(wrapper *clientWrapper, payload protocol.Payload) error {
	cl := wrapper.Client
	if cl == nil {
		return fmt.Errorf("client is nil")
	}

	select {
	case <-wrapper.Ctx.Done():
		return fmt.Errorf("client disconnected")
	default:
	}

	parsedName, err := parseTopicName(payload.Topic)
	if err != nil {
		return fmt.Errorf("name malformed: %v", err)
	}

	b.Mutex.Lock()
	topic, ok := b.TopicMap[parsedName]
	b.Mutex.Unlock()

	if !ok {
		return fmt.Errorf("topic does not exist")
	}

	isEmpty, err := topic.removeClient(cl)
	if err != nil {
		return err
	}
	if isEmpty {
		b.Mutex.Lock()
		delete(b.TopicMap, parsedName)
		b.Mutex.Unlock()
	}

	go func() {
		if err := b.sendACK(wrapper, "Successfully unsubscribed!", unsubscribeState, topic.Name); err != nil {
			log.Printf("unable to send ACK to client: %v", err)
		}
	}()

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
	cl := wrapper.Client
	if cl == nil || cl.Connection == nil {
		return fmt.Errorf("client or connection is nil")
	}

	select {
	case <-wrapper.Ctx.Done():
		return fmt.Errorf("client disconnected, skipping ACK")
	default:
	}

	ackPayload := &protocol.Payload{
		Action:    action + "_ack",
		Topic:     topic,
		Body:      body,
		Timestamp: time.Now().UTC(),
		SenderID:  "",
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
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(b.MessageQueue.ReadChannel)
			log.Println("dequeue loop stopped")
			return
		case <-ticker.C:
			payload, ok := b.MessageQueue.Dequeue()
			if ok {
				select {
				case b.MessageQueue.ReadChannel <- *payload:
				case <-ctx.Done():
					return
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
				if err := topic.Broadcast(val, val.SenderID); err != nil {
					log.Printf("unable to broadcast: %v", err)
				}
			}
		}
	}
}
