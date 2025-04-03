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
	q1, err := ch.QueueDeclare(
		"movimiento_brusco", // name
		true,                // durable
		false,               // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Vincular la cola movimiento_brusco con el exchange
	err = ch.QueueBind(
		q1.Name,                    // queue name
		"sensor.movimiento.brusco", // routing key
		"amq.topic",                // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// Declarar la nueva cola conteo_personas
	q2, err := ch.QueueDeclare(
		"conteo_personas", // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Vincular la cola conteo_personas con el exchange
	err = ch.QueueBind(
		q2.Name,                  // queue name
		"sensor.conteo.personas", // routing key
		"amq.topic",              // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	// Consumir mensajes de la cola movimiento_brusco
	msgs1, err := ch.Consume(
		q1.Name, // queue
		"",      // consumer
		true,    // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	failOnError(err, "Failed to register a consumer for movimiento_brusco")

	// Consumir mensajes de la cola conteo_personas
	msgs2, err := ch.Consume(
		q2.Name, // queue
		"",      // consumer
		true,    // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	failOnError(err, "Failed to register a consumer for conteo_personas")

	var forever chan struct{}

	// Procesar mensajes de movimiento_brusco
	go func() {
		for d := range msgs1 {
			log.Printf("Received a message from movimiento_brusco: %s", d.Body)

			for i := 0; i < 3; i++ {
				resp, err := client.R().
					SetHeader("Content-Type", "text/plain").
					SetBody(d.Body).
					Post("http://localhost:8080/movement/")

				if err == nil && resp.StatusCode() < 500 {
					log.Printf("Request POST exitoso a /movement/, Código de estado: %d", resp.StatusCode())
					break
				}

				log.Printf("Intento %d fallido para /movement/, reintentando...", i+1)
			}
		}
	}()

	// Procesar mensajes de conteo_personas
	go func() {
		for d := range msgs2 {
			log.Printf("Received a message from conteo_personas: %s", d.Body)

			for i := 0; i < 3; i++ {
				resp, err := client.R().
					SetHeader("Content-Type", "text/plain").
					SetBody(d.Body).
					Post("http://localhost:8080/peopleGoUp/")

				if err == nil && resp.StatusCode() < 500 {
					log.Printf("Request POST exitoso a /person-count/, Código de estado: %d", resp.StatusCode())
					break
				}

				log.Printf("Intento %d fallido para /person-count/, reintentando...", i+1)
			}
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
