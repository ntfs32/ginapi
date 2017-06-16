package actions

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/ntfs32/gin-study/services/kafka"
)

func Root(c *gin.Context) {
	broker, topic, enableTLS, clientPemPath, clientKeyPath, caPemPath :=
		[]string{"kafka.gz.baidubce.com:9092"},
		[]string{"127.0.0.1:9093", "127.0.0.1:9094"},
		"d2e594ce9a6e4b78882bde8ebc64ade2__topic_test",
		true,
		"configs/kafka/client.pem",
		"configs/kafka/client.key",
		"configs/kafka/ca.pem"

	client, err := kafka.NewClient(broker, topic, enableTLS, clientPemPath, clientKeyPath, caPemPath)
	if err != nil {
		fmt.Println("create Client failed:" + err.Error())
	}
	partition, offset, err := kafka.Pub(client, topic, "hu")
	fmt.Println("%v", partition, offset, err)
	fmt.Println("%v", client.Config())
	c.String(200, "1")
}

func Name(c *gin.Context) {
	c.JSON(200, gin.H{"name": "shaddock", "age": 25})
}
