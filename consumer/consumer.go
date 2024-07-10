package main

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

	q, err := c.QueueDeclare("hello", true, false, false, false, amqp.Table{
		"x-consumer-timeout": 180000,
	})
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

	for msg := range msgs {
		log.Println(string(msg.Body))
		time.Sleep(time.Minute * 5)
		msg.Ack(false)
	}

	fmt.Println(c.IsClosed())
	fmt.Println(conn.IsClosed())
}
