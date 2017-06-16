package producer

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
)

type Config struct {

	// "The broker address can be found in https://cloud.baidu.com/doc/Kafka/QuickGuide.html#.E7.BB.BC.E8.BF.B0")
	broker string

	//	"Required, the topic to consume from.  You can create a topic from console."
	topic string

	//	TLS is required to access Baidu Kafka service
	enableTLS bool

	//	File path to client.pem provided in kafka-key.zip from console
	clientPemPath string

	//	File path to client.key provided in kafka-key.zip from console
	clientKeyPath string

	//	File path to ca.pem provided in kafka-key.zip from console.
	caPemPath string
}

func NewConfig(broker, topic string, enableTLS bool, clientPemPath, clientKeyPath, caPemPath string) *Config {
	c := &Config{}
	c.broker = broker
	c.topic = topic
	c.enableTLS = enableTLS
	c.clientPemPath = clientPemPath
	c.clientKeyPath = clientKeyPath
	c.caPemPath = caPemPath
	return c
}

func Pub(c *Config, msg string) string {

	if c.topic == "" {
		panic("Argument topic is required.")
	}

	sarama.Logger = log.New(os.Stderr, "[sarama]", log.LstdFlags)

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.Net.TLS.Enable = c.enableTLS
	if c.enableTLS {
		config.Net.TLS.Config = configTLS(c)
	}

	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer([]string{c.broker}, config)
	if err != nil {
		log.Panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	message := &sarama.ProducerMessage{
		Topic: c.topic,
		Value: sarama.StringEncoder(msg),
	}
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("topic:", c.topic, "partition:", partition, "offset:", offset)

}

func Sub(c *Config) {

	if c.topic == "" {
		panic("Argument topic is required.")
	}

	sarama.Logger = log.New(os.Stderr, "[sarama]", log.LstdFlags)

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0
	config.Net.TLS.Enable = c.enableTLS
	config.ClientID = "hmbp"
	if c.enableTLS {
		config.Net.TLS.Config = configTLS(c)
	}
	consumer, err := sarama.NewConsumer([]string{c.broker}, config)
	if err != nil {
		log.Panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	partitions, err := consumer.Partitions(c.topic)
	if err != nil {
		log.Panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(len(partitions))

	for _, partition := range partitions {
		log.Println("consume partition:", partition)

		shutdown := make(chan os.Signal, 1)
		signal.Notify(shutdown, os.Interrupt)

		go func(partition int32) {
			partitionConsumer, err := consumer.ConsumePartition(c.topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalln(err)
				return
			}

			defer func() {
				wg.Done()
				if err := partitionConsumer.Close(); err != nil {
					log.Fatalln(err)
				}
			}()

		ConsumerLoop:
			for {
				select {
				case err := <-partitionConsumer.Errors():
					if err != nil {
						log.Fatalln("error:", err)
					}
				case msg := <-partitionConsumer.Messages():
					if msg != nil {
						log.Println("topic:", msg.Topic, "partition:", msg.Partition, "offset:", msg.Offset, "message:", string(msg.Value))
					}
				case <-shutdown:
					log.Println("stop consuming partition:", partition)
					break ConsumerLoop
				}
			}
		}(partition)
	}

	wg.Wait()
}

func configTLS(c *Config) (t *tls.Config) {
	checkFile(c.clientPemPath)
	checkFile(c.clientKeyPath)
	checkFile(c.caPemPath)

	clientPem, err := tls.LoadX509KeyPair(c.clientPemPath, c.clientKeyPath)
	if err != nil {
		log.Panic(err)
	}

	caPem, err := ioutil.ReadFile(c.caPemPath)
	if err != nil {
		log.Panic(err)
	}

	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caPem)
	t = &tls.Config{
		Certificates:       []tls.Certificate{clientPem},
		RootCAs:            certPool,
		InsecureSkipVerify: true,
	}

	return t
}

func checkFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	if stat.Size() == 0 {
		panic("Please replace " + path + " with your own. ")
	}

}
