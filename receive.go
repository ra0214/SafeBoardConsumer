package main

import (
	"log"

	"github.com/go-resty/resty/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	client := resty.New()

	conn, err := amqp.Dial("amqp://guest:guest@98.83.255.101:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declarar el exchange amq.topic
	err = ch.ExchangeDeclare(
		"amq.topic", // name
		"topic",     // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// Declarar la cola movimiento_brusco
	q, err := ch.QueueDeclare(
		"movimiento_brusco", // name
		true,               // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Vincular la cola con el exchange y el routing key
	err = ch.QueueBind(
		q.Name,                     // queue name
		"sensor.movimiento.brusco", // routing key
		"amq.topic",                // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// Consumir mensajes de la cola
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)

			log.Printf("Enviando solicitud POST con los siguientes datos: ")
			log.Printf("URL: http://localhost:8080/movement/")
			log.Printf("Headers: %v", map[string]string{"Content-Type": "text/plain"})
			log.Printf("Body: %s", d.Body)

			for i := 0; i < 3; i++ {
				resp, err := client.R().
					SetHeader("Content-Type", "text/plain").
					SetBody(d.Body).
					Post("http://localhost:8080/movement/")

				if err == nil && resp.StatusCode() < 500 {
					log.Printf("Request POST exitoso, Código de estado: %d", resp.StatusCode())
					break
				}

				log.Printf("Intento %d fallido, reintentando...", i+1)
			}
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
