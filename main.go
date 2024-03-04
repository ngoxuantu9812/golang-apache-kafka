package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2/log"
	_ "github.com/lib/pq"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
)

const keyServerAddr = "keyServerAddr"

func main() {
	connectDatabase()
	//connectApacheKafka()

	topic := "hello_world"
	worker, err := connectConsumer([]string{"redpanda:9092"})
	if err != nil {
		panic(err)
	}
	// calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	_, err = worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err == nil {
		fmt.Println("Consumer started")
	}
	createServer()

}

func createServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/comment", createComment)
	ctx := context.Background()
	server := &http.Server{
		Addr:    ":80",
		Handler: mux,
		BaseContext: func(listener net.Listener) context.Context {
			ctx = context.WithValue(ctx, keyServerAddr, listener.Addr().String())
			return ctx
		},
	}
	fmt.Println("Server is listening in port localhost:80")
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error listening for server: %s\n", err)
	}
}

func connectDatabase() {
	connStr := "user=postgres dbname=chat-realtime sslmode=verify-full password=root dbname=chat-realtime"
	db, err := sql.Open("postgres", connStr)
	if err != db.Ping() {
		fmt.Println(db.Ping())
	}
}

func connectApacheKafka() {
	brokers := []string{"redpanda:9092"}
	topicName := "hello_world"
	partitions := 3
	replicationFactor := 1

	err := createTopic(brokers, topicName, partitions, replicationFactor)
	if err != nil {
		log.Fatal("Error creating topic: ", err)
	}
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokers, config)
	fmt.Println(brokers)
	if err != nil {
		log.Fatal("Error creating consumer: ", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatal("Error closing consumer: ", err)
		}
	}()

	topics, err := consumer.Topics()
	if err != nil {
		log.Fatal("Error getting topics: ", err)
	}

	fmt.Println("Topics:")

	for _, topic := range topics {
		fmt.Println(topic)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		<-signals
		fmt.Println("Received interrupt signal. Closing consumer. ")
		wg.Done()
	}()

	wg.Wait()
}

func createTopic(brokers []string, topicName string, partitions, replicationFactor int) error {
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return err
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Fatal("Error closing admin: ", err)
		}
	}()

	// Specify the topic configuration
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replicationFactor),
	}

	// Create the topic
	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		return err
	}

	fmt.Printf("Topic '%s' created successfully.\n", topicName)
	return nil
}

func ConnectProducer(brokerUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokerUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"redpanda:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

type Comment struct {
	Message string `json:"message"`
}

func createComment(w http.ResponseWriter, r *http.Request) {
	var cmt Comment
	err := json.NewDecoder(r.Body).Decode(&cmt)
	fmt.Println(cmt)
	cmtInBytes, err := json.Marshal(cmt)
	err = PushCommentToQueue("hello_world", cmtInBytes)
	if err != nil {
		return
	}
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// NewConsumer creates a new consumer using the given broker addresses and configuration
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
