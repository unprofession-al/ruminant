package cmd

import (
	"errors"
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Regurgitate Regurgitate `yaml:"regurgitate"`
	Ruminate    Ruminate    `yaml:"ruminate"`
	Gulp        Gulp        `yaml:"gulp"`
}

type Regurgitate struct {
	Host  string `yaml:"host"`
	Port  int    `yaml:"port"`
	Proto string `yaml:"proto"`
	Index string `yaml:"index"`
	Type  string `yaml:"type"`
	Query string `yaml:"query"`
}

type Gulp struct {
	Host            string `yaml:"host"`
	Port            int    `yaml:"port"`
	Db              string `yaml:"db"`
	Proto           string `yaml:"proto"`
	Series          string `yaml:"series"`
	User            string `yaml:"user"`
	Pass            string `yaml:"Pass"`
	LatestIndicator string `yaml:"latest_indicator"`
}

type Ruminate struct {
	Iterator Iterator `yaml:"iterator"`
}

type Iterator struct {
	Selector  string            `yaml:"selector"`
	Time      string            `yaml:"time"`
	Tags      map[string]string `yaml:"tags"`
	Values    map[string]string `yaml:"values"`
	Iterators []Iterator        `yaml:"iterators"`
}

func Conf() (Config, error) {
	conf := Config{}
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

	return conf, nil
}
