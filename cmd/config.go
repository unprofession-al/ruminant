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
	Poop        PoopConf        `yaml:"poop"`
}

type PoopConf struct {
	Query      string   `yaml:"query"`
	Fields     []string `yaml:"fields"`
	Start      string   `yaml:"start"`
	End        string   `yaml:"end"`
	Format     string   `yaml:"format"`
	Separator  string   `yaml:"separator"`
	ReplaceNil string   `yaml:"replace_nil"`
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
	Selector    string            `yaml:"selector"`
	Time        string            `yaml:"time"`
	Tags        map[string]string `yaml:"tags"`
	FixedTags   map[string]string `yaml:"fixed_tags"`
	Values      map[string]string `yaml:"values"`
	FixedValues map[string]string `yaml:"fixed_values"`
	Iterators   []Iterator        `yaml:"iterators"`
}

func (i Iterator) GetStructure() (tags []string, values []string) {
	for key, _ := range i.Tags {
		tags = append(tags, key)
	}
	for key, _ := range i.FixedTags {
		tags = append(tags, key)
	}
	for key, _ := range i.Values {
		values = append(values, key)
	}
	for key, _ := range i.FixedValues {
		values = append(values, key)
	}
	for _, iter := range i.Iterators {
		t, v := iter.GetStructure()
		tags = append(tags, t...)
		values = append(values, v...)
	}
	return
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
		Poop: PoopConf{
			Query:      "SELECT {{ range $index, $element := .Fields }}{{if $index}},{{end}}\"{{$element}}\"{{end}} FROM \"{{.Series}}\" WHERE time > {{.Start}} AND time < {{.End}}",
			Start:      "now() - 1d",
			End:        "now()",
			Format:     "02/Jan/2006 15:04",
			Separator:  ",",
			ReplaceNil: "[NIL]",
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

	if len(conf.Poop.Fields) < 1 {
		fields := []string{"time"}
		tags, values := conf.Ruminate.Iterator.GetStructure()
		fields = append(fields, tags...)
		fields = append(fields, values...)
		conf.Poop.Fields = fields
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
