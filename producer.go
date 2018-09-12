package main

import (
	"github.com/segmentio/kafka-go"
	"context"
	"time"
)

func main() {
	topic := "my-test-topic"
	partition := 0
	// DialLeader opens a connection to the leader of the partition for a given topic
	// "conn" => Conn pointer returned by DialLeader
	// "_" => error returned by DialLeader
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

	conn.SetWriteDeadline(time.Now().Add(10*time.Second))
	conn.WriteMessages(
	    kafka.Message{Value: []byte("hello")},
	    kafka.Message{Value: []byte("world!")},
	)

	conn.Close()
}
