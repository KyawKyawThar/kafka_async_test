package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"time"
)

const (
	broker1Address = "localhost:9093"
	//broker2Address = "localhost:9094"
	//broker3Address = "localhost:9095"
	topic = "message-logs"
)

//docker exec -it 09a180b3978e /bin/sh
//# kafka-topics.sh --version
//2.8.1 (Commit:839b886f9b732b15)

func main() {
	// Setup configuration for the producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionSnappy   // Optional: Use compression for performance
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush messages every 500ms

	config.Version = sarama.V2_8_1_0

	// Enable SASL/PLAIN authentication
	//config.Net.SASL.Enable = true
	//config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	//config.Net.SASL.User = "kafka_user"
	//config.Net.SASL.Password = "kafka_password"

	// Enable TLS if using encryption (optional but recommended for SASL/PLAIN)
	//config.Net.TLS.Enable = true
	//config.Net.TLS.Config = &tls.Config{
	//	InsecureSkipVerify: true, // Set to false in production; true for local testing only
	//}
	// Connect to the Kafka broker (replace with your Kafka broker's address)
	brokerList := []string{"localhost:9093"}

	// Create a new async Kafka producer
	producer, err := sarama.NewAsyncProducer(brokerList, config)

	if err != nil {
		log.Fatalln("Failed to start Kafka producer:", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln("Failed to close Kafka producer:", err)
		}
	}()

	// Handle errors and success messages in separate goroutines

	go func() {
		for err := range producer.Errors() {
			log.Printf("Failed to produce message: %v\n", err)
		}
	}()

	go func() {
		for success := range producer.Successes() {
			log.Printf("Message sent successfully to partition %d, offset %d\n", success.Partition, success.Offset)
		}
	}()

	for i := 0; i < 10; i++ {

		msg := fmt.Sprintf("Asynchronous kafka message: %d\n", i)

		producerMsg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		}

		producer.Input() <- producerMsg

		log.Printf("Produced message: %s\n", msg)
		time.Sleep(1000 * time.Millisecond)

	}

	time.Sleep(2 * time.Second)
}
