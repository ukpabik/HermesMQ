package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ukpabik/HermesMQ/internal/client"
)

func main() {
	conn, err := net.Dial("tcp", ":8080")
	if err != nil {
		log.Fatalf("unable to connect to broker: %v", err)
	}

	cl, err := client.InitializeClient(fmt.Sprintf("client-%d", time.Now().Unix()), conn)
	if err != nil {
		log.Fatalf("unable to initialize client: %v", err)
	}
	defer cl.Close()

	log.Println("✅ connected to broker")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	disconnectChan := make(chan struct{})

	go func() {
		<-sigChan
		log.Println("\n🛑 shutting down...")
		cancel()
		cl.Close()
		os.Exit(0)
	}()

	printHelp()

	inputChan := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			case inputChan <- scanner.Text():
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("scanner error: %v", err)
		}
	}()

	fmt.Print("> ")

	for {
		select {
		case <-ctx.Done():
			log.Println("👋 goodbye!")
			return

		case <-disconnectChan:
			log.Println("⚠️  disconnected from broker")
			return

		case input, ok := <-inputChan:
			if !ok {
				return
			}

			input = strings.TrimSpace(input)
			if input == "" {
				fmt.Print("> ")
				continue
			}

			if !handleCommand(cl, input) {
				return
			}
			fmt.Print("> ")
		}
	}
}

func handleCommand(cl *client.Client, input string) bool {
	parts := strings.SplitN(input, " ", 3)
	command := strings.ToLower(parts[0])

	switch command {
	case "sub", "subscribe":
		if len(parts) < 2 {
			fmt.Println("❌ usage: sub <topic>")
			return true
		}
		topic := parts[1]
		if err := cl.Subscribe(topic); err != nil {
			fmt.Printf("❌ subscribe failed: %v\n", err)
		} else {
			fmt.Printf("✅ subscribed to '%s'\n", topic)
		}

	case "unsub", "unsubscribe":
		if len(parts) < 2 {
			fmt.Println("❌ usage: unsub <topic>")
			return true
		}
		topic := parts[1]
		if err := cl.Unsubscribe(topic); err != nil {
			fmt.Printf("❌ unsubscribe failed: %v\n", err)
		} else {
			fmt.Printf("✅ unsubscribed from '%s'\n", topic)
		}

	case "pub", "publish":
		if len(parts) < 3 {
			fmt.Println("❌ usage: pub <topic> <message>")
			return true
		}
		topic := parts[1]
		message := parts[2]
		if err := cl.Publish(topic, message); err != nil {
			fmt.Printf("❌ publish failed: %v\n", err)
		} else {
			fmt.Printf("✅ published to '%s'\n", topic)
		}

	case "list":
		cl.Mutex.Lock()
		topics := make([]string, 0, len(cl.SubscribedTopics))
		for topic := range cl.SubscribedTopics {
			topics = append(topics, topic)
		}
		cl.Mutex.Unlock()

		if len(topics) == 0 {
			fmt.Println("📭 no subscriptions")
		} else {
			fmt.Println("📬 subscribed topics:")
			for _, topic := range topics {
				fmt.Printf("  - %s\n", topic)
			}
		}

	case "help":
		printHelp()

	case "quit", "exit":
		log.Println("👋 goodbye!")
		return false

	default:
		fmt.Printf("❌ unknown command: %s (type 'help' for usage)\n", command)
	}

	return true
}

func printHelp() {
	fmt.Println("\n📖 HermesMQ Client Commands:")
	fmt.Println("  sub <topic>           - Subscribe to a topic")
	fmt.Println("  unsub <topic>         - Unsubscribe from a topic")
	fmt.Println("  pub <topic> <message> - Publish a message to a topic")
	fmt.Println("  list                  - List subscribed topics")
	fmt.Println("  help                  - Show this help")
	fmt.Println("  quit                  - Exit client")
	fmt.Println()
}
