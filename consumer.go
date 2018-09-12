package main

import (
	"fmt"	
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

	conn.SetReadDeadline(time.Now().Add(10*time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
	    _, err := batch.Read(b)
	    if err != nil {
		break
	    }
	    fmt.Println(string(b))
	}

	batch.Close()

	conn.Close()
}
