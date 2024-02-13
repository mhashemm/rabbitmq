package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	// Connects opens an AMQP connection from the credentials in the URL.
	conn, err := amqp.Dial("amqp://user:bitnami@localhost:5672/")
	if err != nil {
		log.Fatalf("connection.open: %s", err)
	}
	defer conn.Close()

	c, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel.open: %s", err)
	}

	q, err := c.QueueDeclare("hello", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("queue.declare: %v", err)
	}

	err = c.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("basic.qos: %v", err)
	}

	msgs, err := c.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("basic.consume: %v", err)
	}

	stall := make(chan struct{})

	seconds := 15
	go func() {
		for msg := range msgs {
			m := map[string]any{}
			json.Unmarshal(msg.Body, &m)
			// fmt.Println(m["number"], len(m["data"].(string)))
			fmt.Println(m["number"])
			time.Sleep(time.Second * time.Duration(seconds))
			msg.Ack(false)
		}
		stall <- struct{}{}
	}()
	<-stall
}
