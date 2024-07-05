package main

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
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

	// using pool here is wrong but it's so cool
	// DO NOT USE IT YOU DUMBASS
	p := sync.Pool{
		New: func() any {
			c, err := conn.Channel()
			if err != nil {
				return nil // no channel
			}
			// https://github.com/golang/go/issues/23216#issuecomment-353477328
			runtime.SetFinalizer(c, (*amqp.Channel).Close)
			return c
		},
	}

	_c := p.Get()

	if _c == nil {
		log.Fatalf("channel.open: %s", err)
	}

	c := _c.(*amqp.Channel)

	defer p.Put(c)
	q, err := c.QueueDeclare(
		"hello", // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	err = c.Confirm(false)

	count := 0
	notify := c.NotifyPublish(make(chan amqp.Confirmation))
	for true {
		msg := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			ContentType:  "text/plain",
			Body:         []byte(fmt.Sprintf("msg number: %d", count)),
		}
		err = c.Publish("", q.Name, true, false, msg)
		if err != nil {
			log.Fatalf("basic.publish: %v", err)
		}
		res := <-notify
		if !res.Ack {
			log.Fatalln(res)
		}
		log.Println(fmt.Sprintf("published: %d", count))
		count += 1
		time.Sleep(time.Second * time.Duration(rand.Int63n(3)))
	}
}
