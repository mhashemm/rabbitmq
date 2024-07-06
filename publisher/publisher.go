package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Connects opens an AMQP connection from the credentials in the URL.
	conn, _ := amqp.Dial("amqp://user:bitnami@localhost:5672/")
	defer conn.Close()

	c, _ := conn.Channel()
	defer c.Close()
	_ = c.Confirm(false)

	q, _ := c.QueueDeclare(
		"hello", // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	wg := sync.WaitGroup{}

	for i := 0; i < 1e6; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			msg := amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
				ContentType:  "text/plain",
				Body:         []byte(fmt.Sprintf("msg number: %d", i)),
				MessageId:    strconv.Itoa(i),
			}
			confirm, err := c.PublishWithDeferredConfirm("", q.Name, true, false, msg)
			if err != nil {
				log.Fatalf("basic.publish: %v", err)
			}
			if confirm.Wait() {
				log.Println(fmt.Sprintf("published: %d", i), confirm.DeliveryTag)
			} else {
				log.Fatal("suca", confirm.DeliveryTag)
			}
			// time.Sleep(time.Second * time.Duration(rand.Int63n(3)))
		}()
	}
	wg.Wait()
}
