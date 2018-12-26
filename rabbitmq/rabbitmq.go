package rabbitmq

import (
	"encoding/json"
	"fmt"

	cony "github.com/assembla/cony"
	amqp "github.com/streadway/amqp"
)

// Client - Client
type Client *cony.Client

// InitClient - InitClient
func InitClient(host string, port int, username, password, vHost, queueName, exchangeName, routingKey, rejectQueueName, rejectExchangeName, rejectRoutingKey string) (Client, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/%s", username, password, host, port, vHost)

	client := cony.NewClient(
		cony.URL(url),
		cony.Backoff(cony.DefaultBackoff),
	)

	var (
		rejectQueue    *cony.Queue
		rejectExchange cony.Exchange
		rejectBinding  cony.Binding
		queueArgs      amqp.Table
		declaration    []cony.Declaration
	)

	if rejectQueueName != "" {
		rejectQueue = &cony.Queue{
			Name:       rejectQueueName,
			Durable:    true,
			AutoDelete: false,
			Exclusive:  false,
			Args:       nil,
		}
		rejectExchange = cony.Exchange{
			Name:       rejectExchangeName,
			Kind:       "direct",
			Durable:    true,
			AutoDelete: false,
		}
		rejectBinding = cony.Binding{
			Queue:    rejectQueue,
			Exchange: rejectExchange,
			Key:      rejectExchangeName,
		}
		queueArgs = amqp.Table{
			"x-dead-letter-exchange":    rejectExchangeName,
			"x-dead-letter-routing-key": rejectRoutingKey,
		}
		declaration = append(declaration, cony.DeclareExchange(rejectExchange))
		declaration = append(declaration, cony.DeclareQueue(rejectQueue))
		declaration = append(declaration, cony.DeclareBinding(rejectBinding))
	}

	queue := &cony.Queue{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		Args:       queueArgs,
	}

	exchange := cony.Exchange{
		Name:       exchangeName,
		Kind:       "direct",
		Durable:    true,
		AutoDelete: false,
	}
	binding := cony.Binding{
		Queue:    queue,
		Exchange: exchange,
		Key:      routingKey,
	}

	declaration = append(declaration, cony.DeclareExchange(exchange))
	declaration = append(declaration, cony.DeclareQueue(queue))
	declaration = append(declaration, cony.DeclareBinding(binding))

	client.Declare(declaration)
	// Set Qos(config.Queue.PrefetchCount, 0, true)

	return Client(client), nil
}

// InitConsumer - InitConsumer
func InitConsumer(client *cony.Client, queueName string) *cony.Consumer {
	queue := &cony.Queue{Name: queueName}

	consumer := cony.NewConsumer(queue)
	(*client).Consume(consumer)
	return consumer
}

// InitPublisher - InitPublisher
func InitPublisher(client *cony.Client, exchangeName, routingKey string) *cony.Publisher {
	publisher := cony.NewPublisher(exchangeName, routingKey)
	(*client).Publish(publisher)
	return publisher
}

// Publish - Publish
func Publish(publisher *cony.Publisher, queueName, exchange, routingKey string, headers amqp.Table, message interface{}) error {
	m, err := json.Marshal(message)
	if err != nil {
		return err
	}

	go (*publisher).Publish(amqp.Publishing{
		ContentType: "application/json",
		Headers:     headers,
		Body:        m,
	})
	return err
}
