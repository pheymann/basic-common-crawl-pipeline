package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueueName = "commoncrawl"
)

type MessageQueueChannel interface {
	Close() error
	BasicPublish(exchange, routingKey string, body []byte) error
	BasicConsume(queue string, handler func(delivery amqp.Delivery)) error
	BasicAck(tag uint64, multiple bool) error
	SetQoS(prefetchCount, prefetchSize int, global bool) error
	ConfirmChannel() chan amqp.Confirmation
}

type RabbitMQChannel struct {
	conn           *amqp.Connection
	channel        *amqp.Channel
	confirmChannel chan amqp.Confirmation
}

func NewRabbitMQChannel(port int) (*RabbitMQChannel, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@localhost:%d/", port))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Enable publisher confirms
	err = ch.Confirm(false)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to enable publisher confirms: %w", err)
	}

	confirmChannel := make(chan amqp.Confirmation, 1)
	ch.NotifyPublish(confirmChannel)

	_, err = ch.QueueDeclare(
		QueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	return &RabbitMQChannel{
		conn:           conn,
		channel:        ch,
		confirmChannel: confirmChannel,
	}, nil
}

func (c *RabbitMQChannel) Close() error {
	if err := c.channel.Close(); err != nil {
		return fmt.Errorf("failed to close channel: %w", err)
	}
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	close(c.confirmChannel)
	return nil
}

func (c *RabbitMQChannel) BasicPublish(exchange, routingKey string, body []byte) error {
	return c.channel.PublishWithContext(context.Background(),
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
}

func (c *RabbitMQChannel) BasicConsume(queue string, handler func(delivery amqp.Delivery)) error {
	msgs, err := c.channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	go func() {
		for d := range msgs {
			handler(d)
		}
	}()

	return nil
}

func (c *RabbitMQChannel) BasicAck(tag uint64, multiple bool) error {
	return c.channel.Ack(tag, multiple)
}

func (c *RabbitMQChannel) SetQoS(prefetchCount, prefetchSize int, global bool) error {
	return c.channel.Qos(prefetchCount, prefetchSize, global)
}

func (c *RabbitMQChannel) ConfirmChannel() chan amqp.Confirmation {
	return c.confirmChannel
}

func PublishBatch(ch MessageQueueChannel, batch interface{}) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %w", err)
	}

	// Set up retry parameters
	maxRetries := 3
	retryCount := 0

	for retryCount < maxRetries {
		err = ch.BasicPublish("", QueueName, body)
		if err != nil {
			// Check if it's a connection-related error and we can recover from it
			if amqpErr, ok := err.(*amqp.Error); ok && amqpErr.Recover {
				// TODO pheymann: check if this is correct
				// if amqpErr.Code == 320 || // CONNECTION_FORCED
				// 	amqpErr.Code == 402 || // CHANNEL_ERROR
				// 	amqpErr.Code == 501 || // FRAME_ERROR
				// 	amqpErr.Code == 541 { // INTERNAL_ERROR
				// }

				retryCount++
				if retryCount == maxRetries {
					return fmt.Errorf("failed to publish after %d retries due to connection issues: %w", maxRetries, err)
				}
				continue
			}
			// For all other errors, return immediately
			return fmt.Errorf("failed to publish: %w", err)
		}

		// Wait for confirmation
		if confirmed := <-ch.ConfirmChannel(); confirmed.Ack {
			return nil
		}

		retryCount++
		if retryCount == maxRetries {
			return fmt.Errorf("message not confirmed by broker after %d retries", maxRetries)
		}
	}

	return nil
}
