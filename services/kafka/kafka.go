package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/sarama"
	//	sarama "github.com/bsm/sarama-cluster"
)

var topic, clientPemPath, clientKeyPath, caPemPath string

var broker string = os.Getenv("KAFKA_BROKER")

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

func NewClient(broker []string, topic string, enableTLS bool, clientPemPath, clientKeyPath, caPemPath string) (sarama.Client, error) {
	c := &Config{}
	c.enableTLS = enableTLS
	c.clientPemPath = clientPemPath
	c.clientKeyPath = clientKeyPath
	c.caPemPath = caPemPath
	sarama.Logger = log.New(os.Stderr, "[sarama]", log.LstdFlags)
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	config.ClientID = "hmbp1"
	config.Metadata.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Net.TLS.Enable = c.enableTLS
	if c.enableTLS {
		config.Net.TLS.Config = configTLS(c)
	}
	return sarama.NewClient(broker, config)

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
