package cmd

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"go.uber.org/zap"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Regurgitate RegurgitateConf `yaml:"regurgitate"`
	Ruminate    RuminateConf    `yaml:"ruminate"`
	Gulp        GulpConf        `yaml:"gulp"`
}

type RegurgitateConf struct {
	Host    string        `yaml:"host"`
	Port    int           `yaml:"port"`
	Proto   string        `yaml:"proto"`
	Index   string        `yaml:"index"`
	Type    string        `yaml:"type"`
	Query   string        `yaml:"query"`
	Sampler SamplerConfig `yaml:"sampler"`
}

type SamplerConfig struct {
	Offset       time.Duration `yaml:"offset"`
	Samples      int           `yaml:"samples"`
	SampleOffset time.Duration `yaml:"sample_offset"`
	Interval     string        `yaml:"interval"`
}

type GulpConf struct {
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Db        string `yaml:"db"`
	Proto     string `yaml:"proto"`
	Series    string `yaml:"series"`
	User      string `yaml:"user"`
	Pass      string `yaml:"pass"`
	Indicator string `yaml:"indicator"`
}

type RuminateConf struct {
	Iterator Iterator `yaml:"iterator"`
}

type Iterator struct {
	Selector  string            `yaml:"selector"`
	Time      string            `yaml:"time"`
	Tags      map[string]string `yaml:"tags"`
	Values    map[string]string `yaml:"values"`
	Iterators []Iterator        `yaml:"iterators"`
}

func Conf(mustExist bool) (Config, error) {
	conf := Config{
		Regurgitate: RegurgitateConf{
			Port:  9200,
			Proto: "http",
			Index: "logstash-*",
			Sampler: SamplerConfig{
				Samples: 1,
			},
		},
		Gulp: GulpConf{
			Proto: "http",
			Port:  8086,
		},
	}

	if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
		if mustExist {
			err = errors.New(fmt.Sprintf("Config file %s does not exist", cfgFile))
			return conf, err
		} else {
			return conf, nil
		}
	}

	file, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		err = errors.New(fmt.Sprintf("Error while reading %s: %s", cfgFile, err.Error()))
		return conf, err
	}

	err = yaml.Unmarshal(file, &conf)
	if err != nil {
		err = errors.New(fmt.Sprintf("Error while parsing %s: %s", cfgFile, err.Error()))
		return conf, err
	}

	SetupLogger()

	return conf, nil
}

func (c Config) String() string {
	b, _ := yaml.Marshal(c)
	return string(b)
}

func SetupLogger() {
	c := zap.NewDevelopmentConfig()
	c.DisableCaller = true
	c.DisableStacktrace = true
	logger, _ := c.Build()
	l = logger.Sugar()
}
