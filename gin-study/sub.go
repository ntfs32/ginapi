package main

import (
	"github.com/ntfs32/gin-study/services/kafka/producer"
)

func main() {
	config := producer.NewConfig(
		"kafka.gz.baidubce.com:9092",
		"d2e594ce9a6e4b78882bde8ebc64ade2___topic_test",
		true, "configs/kafka/client.pem", "configs/kafka/client.key", "configs/kafka/ca.pem")
	producer.Sub(config)
}
