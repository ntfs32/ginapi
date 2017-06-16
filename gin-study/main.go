package main

import (
	"github.com/gin-gonic/gin"
	"github.com/ntfs32/gin-study/services/kafka/producer"
)

func main() {
	req := gin.Default()
	req.GET("/pub/:msg", func(c *gin.Context) {
		config := producer.NewConfig(
			"kafka.gz.baidubce.com:9092",
			"d2e594ce9a6e4b78882bde8ebc64ade2___topic_test",
			true, "configs/kafka/client.pem", "configs/kafka/client.key", "configs/kafka/ca.pem")
		result := producer.Pub(config, c.Param("msg"))
		go producer.Sub(config)
		c.JSON(200, gin.H{
			"message": result,
		})
	})
	req.Run(":8080") // listen and serve on 0.0.0.0:8080
}
