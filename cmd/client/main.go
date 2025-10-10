package main

import (
	"bufio"
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

	cl := &client.Client{
		Connection: conn,
		ID:         fmt.Sprintf("client-%d", time.Now().Unix()),
	}
	defer cl.Close()

	log.Println("✅ connected to broker")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\n🛑 shutting down...")
		cl.Close()
		os.Exit(0)
	}()

	go func() {
		if cl.ReadChannel == nil {
			return
		}
		for msg := range cl.ReadChannel {
			fmt.Printf("\n📨 [%s] %v\n", msg.Topic, msg.Body)
			fmt.Print("> ")
		}
	}()

	// Interactive CLI
	printHelp()
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.SplitN(input, " ", 3)
		command := strings.ToLower(parts[0])

		switch command {
		case "sub", "subscribe":
			if len(parts) < 2 {
				fmt.Println("❌ usage: sub <topic>")
				continue
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
				continue
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
				continue
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
			if len(cl.SubscribedTopics) == 0 {
				fmt.Println("📭 no subscriptions")
			} else {
				fmt.Println("📬 subscribed topics:")
				for topic := range cl.SubscribedTopics {
					fmt.Printf("  - %s\n", topic)
				}
			}
			cl.Mutex.Unlock()

		case "help":
			printHelp()

		case "quit", "exit":
			log.Println("👋 goodbye!")
			return

		default:
			fmt.Printf("❌ unknown command: %s (type 'help' for usage)\n", command)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("scanner error: %v", err)
	}
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
