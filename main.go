package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2/log"
	_ "github.com/lib/pq"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
)

const keyServerAddr = "keyServerAddr"

func main() {
	connectDatabase()
	createServer()

}

func createServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", getRoot)
	mux.HandleFunc("/hello", getHello)
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
func getRoot(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		io.WriteString(w, "This is my website with only get method\n")
	case http.MethodPost:
		io.WriteString(w, "This is my website with only post method\n")
	case http.MethodPut:
		io.WriteString(w, "This is my website with only put method\n")
	case http.MethodPatch:
		io.WriteString(w, "This is my website with only patch method\n")
	case http.MethodDelete:
		io.WriteString(w, "This is my website with only delete method\n")

	}
}

func getHello(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	fmt.Printf("%s: got /hello request\n", ctx.Value(keyServerAddr))
	myName := r.PostFormValue("myName")
	if myName == "" {
		w.Header().Set("x-missing-fied", "myName")
		w.WriteHeader(http.StatusBadRequest)
	}
	io.WriteString(w, fmt.Sprintf("Hello, %s!\n", myName))
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
	brokersUrl := []string{"kafkahost1:9092", "kafkahost2:9092"}
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
	Text string `form:"text" json:"text"`
}

func createComment(w http.ResponseWriter, r *http.Request) {
	// Instantiate new Message struct

	fmt.Println(w)
	cmt := new(Comment)
	cmt.f = "Hello, I'm Tu"
	err = PushCommentToQueue("comments", cmt)
	//if err != nil {
	//	return
	//}
}
