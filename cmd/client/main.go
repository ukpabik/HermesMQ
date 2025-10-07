package main

import (
	"log"
	"net"

	"github.com/ukpabik/HermesMQ/internal/client"
)

func main() {
	client := &client.Client{}

	netter, err := net.Dial("tcp", ":8080")
	if err != nil {
		log.Fatalf("unable to connect")
	}
	client.Connection = netter
	defer client.Connection.Close()
	log.Println("connected to broker ✅")

	// TODO: Handle client stuff
}
