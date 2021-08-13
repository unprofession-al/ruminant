package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"ruminant/sink"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Regurgitate RegurgitateConf `yaml:"regurgitate"`
	Ruminate    RuminateConf    `yaml:"ruminate"`
	Gulp        sink.Config     `yaml:"gulp"`
	Poop        PoopConf        `yaml:"poop"`
}

type PoopConf struct {
	Query      string   `yaml:"query"`
	Fields     []string `yaml:"fields"`
	Format     string   `yaml:"format"`
	Separator  string   `yaml:"separator"`
	ReplaceNil string   `yaml:"replace_nil"`
}

type RegurgitateConf struct {
	BaseURL  string        `yaml:"base_url"`
	User     string        `yaml:"user"`
	Password string        `yaml:"password"`
	Index    string        `yaml:"index"`
	Type     string        `yaml:"type"`
	Query    string        `yaml:"query"`
	Sampler  SamplerConfig `yaml:"sampler"`
}

type SamplerConfig struct {
	Offset       time.Duration `yaml:"offset"`
	Samples      int           `yaml:"samples"`
	SampleOffset time.Duration `yaml:"sample_offset"`
	Interval     string        `yaml:"interval"`
}

type RuminateConf struct {
	Iterator Iterator `yaml:"iterator"`
}

type Iterator struct {
	Selector    string            `yaml:"selector"`
	Time        string            `yaml:"time"`
	Tags        map[string]string `yaml:"tags"`
	FixedTags   map[string]string `yaml:"fixed_tags"`
	Values      map[string]string `yaml:"values"`
	FixedValues map[string]string `yaml:"fixed_values"`
	Iterators   []Iterator        `yaml:"iterators"`
}

func (i Iterator) GetStructure() (tags []string, values []string) {
	for key := range i.Tags {
		tags = append(tags, key)
	}
	for key := range i.FixedTags {
		tags = append(tags, key)
	}
	for key := range i.Values {
		values = append(values, key)
	}
	for key := range i.FixedValues {
		values = append(values, key)
	}
	for _, iter := range i.Iterators {
		t, v := iter.GetStructure()
		tags = append(tags, t...)
		values = append(values, v...)
	}
	return
}

func NewConf(cfgFile string, mustExist bool) (Config, error) {
	conf := Config{
		Regurgitate: RegurgitateConf{
			BaseURL: "http://localhost:9200/",
			Index:   "logstash-*",
			Sampler: SamplerConfig{
				Samples: 1,
			},
		},
		Gulp: sink.Config{},
		Poop: PoopConf{
			Format:     "02/Jan/2006 15:04",
			Separator:  ",",
			ReplaceNil: "[NIL]",
		},
	}

	u, err := url.Parse(cfgFile)
	if err != nil {
		return conf, err
	}

	data := []byte{}

	if u.Scheme == "" {
		if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
			if mustExist {
				err = fmt.Errorf("config file %s does not exist", cfgFile)
				return conf, err
			}
			return conf, nil
		}

		data, err = ioutil.ReadFile(u.Path)
		if err != nil {
			err = fmt.Errorf("error while reading %s: %s", cfgFile, err.Error())
			return conf, err
		}
	} else if u.Scheme == "http" || u.Scheme == "https" {
		resp, err := http.Get(u.String())
		if err != nil {
			err = fmt.Errorf("error while fetching %s: %s", cfgFile, err.Error())
			return conf, err
		}
		defer resp.Body.Close()

		data, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("error while reading body of %s: %s", cfgFile, err.Error())
			return conf, err
		}
	} else if u.Scheme == "s3" {
		sess, _ := session.NewSession(&aws.Config{})
		if err != nil {
			err = fmt.Errorf("error while creating s3 client to read %s: %s", cfgFile, err.Error())
			return conf, err
		}
		s3Client := s3.New(sess)
		input := &s3.GetObjectInput{
			Bucket: aws.String(u.Host),
			Key:    aws.String(u.Path),
		}

		result, err := s3Client.GetObject(input)
		if err != nil {
			err = fmt.Errorf("error while reading object %s: %s", cfgFile, err.Error())
			return conf, err
		}
		defer result.Body.Close()
		data, err = ioutil.ReadAll(result.Body)
		if err != nil {
			err = fmt.Errorf("error while reading body of %s: %s", cfgFile, err.Error())
			return conf, err
		}
	} else {
		err = fmt.Errorf("cannot read %s: unsupported protocol %s", cfgFile, u.Scheme)
		return conf, err
	}

	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		err = fmt.Errorf("error while parsing %s: %s", cfgFile, err.Error())
		return conf, err
	}

	conf.Regurgitate.BaseURL = strings.TrimRight(conf.Regurgitate.BaseURL, "/")

	if len(conf.Poop.Fields) < 1 {
		fields := []string{"time"}
		tags, values := conf.Ruminate.Iterator.GetStructure()
		fields = append(fields, tags...)
		fields = append(fields, values...)
		conf.Poop.Fields = fields
	}

	return conf, nil
}

func (c Config) String() string {
	b, _ := yaml.Marshal(c)
	return string(b)
}
