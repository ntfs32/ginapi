package configs

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

var configFileName = "configs/config.yml"

type config struct {
	kafka struct { // kafka配置
		kroker        string
		enableTLS     string
		clientPemPath string
		clientKeyPath string
		caPemPath     string
	}
}

var Conf map[string]interface{}

func init() {
	Conf = make(map[string]interface{})
	file, err := ioutil.ReadFile(configFileName)
	err = yaml.Unmarshal(file, &Conf)
	if err != nil {
		panic("Unmarshal yaml config failed:" + err.Error())
	}
}

func Get(key string) string {
	fmt.Println(Conf)
	if value, ok := Conf[key]; ok {
		return value.(string)
	}
	return ""
}
