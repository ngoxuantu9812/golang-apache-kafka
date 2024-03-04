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
)

const keyServerAddr = "keyServerAddr"

func main() {
	connectDatabase()
	createServer()

}

func createServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/topic/create", createTopic)
	mux.HandleFunc("/message", createMessage)
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

type Topic struct {
	Name              string `json:"name"`
	Partitions        int8   `json:"partitions"`
	ReplicationFactor int8   `json:"replication_factor"`
}

func createTopic(w http.ResponseWriter, r *http.Request) {
	brokersUrl := []string{"redpanda:9092"}
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokersUrl, config)
	var topic Topic
	err = json.NewDecoder(r.Body).Decode(&topic)
	if err != nil {
		return
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Fatal("Error closing admin: ", err)
		}
	}()

	// Specify the topic configuration
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(topic.Partitions),
		ReplicationFactor: int16(topic.ReplicationFactor),
	}
	// Create the topic
	err = admin.CreateTopic(topic.Name, topicDetail, false)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Topic '%s' created successfully.\n", topic.Name)
	return
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
		Topic:    topic,
		Value:    sarama.StringEncoder(message),
		Metadata: err,
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

type Message struct {
	Message string `json:"message"`
	Chanel  string `json:"chanel"`
}

func createMessage(w http.ResponseWriter, r *http.Request) {
	var message Message
	err := json.NewDecoder(r.Body).Decode(&message)
	cmtInBytes, err := json.Marshal(message.Message)
	err = PushCommentToQueue(message.Chanel, cmtInBytes)
	if err != nil {
		return
	}
}
