package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func Pub(c sarama.Client, topic, msg string) (partition int32, offset int64, err error) {
	fmt.Println("%v", c)
	producer, err := sarama.NewSyncProducerFromClient(c)
	//	defer producer.Close()
	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Println(err.Error())
		}
	}()
	if err != nil {
		panic(err)
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}
	partition, offset, err = producer.SendMessage(message)
	return

}
