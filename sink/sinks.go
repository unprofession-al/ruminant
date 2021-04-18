package sink

import (
	"errors"
	"sync"
)

var (
	sMu sync.Mutex
	s   = make(map[string]func(map[string]string) (Sink, error))
)

// Config keeps the configuration of the sink implementation. This struct is
// passed to the sink implementatinon itself via the registered setup function.
// The field 'Kind' is used to determine which provider is requested.
type Config struct {
	Kind       string            `yaml:"kind"`
	Connection map[string]string `yaml:"connection"`
}

func (c Config) Validate() ([]string, error) {
	errs := []string{}
	if c.Kind == "" {
		errs = append(errs, "Field 'kind' cannot be empty.")
	}
	if len(errs) > 0 {
		err := errors.New("Config of 'sink' has errors")
		return errs, err
	}
	return errs, nil
}

// Sink interface needs to be implemented in order to provide a Sink
// backend such as InfluxDB.
type Sink interface {
	Write(points []Point) error
	//Query(cmd string) (res []client.Result, err error)
}

// Register must be called in the init function of each sink implementation.
// The Register function will panic if two sink impelmentations with the
// same name try to register themselves.
func Register(name string, setupFunc func(map[string]string) (Sink, error)) {
	sMu.Lock()
	defer sMu.Unlock()
	if _, dup := s[name]; dup {
		panic("sink: Register called twice for store " + name)
	}
	s[name] = setupFunc
}

// New well return a configured instance of a sink implementation. The
// implementation requested is determined by the 'Kind' field of the
// configuration struct.
func New(conf Config) (Sink, error) {
	setupFunc, ok := s[conf.Kind]
	if !ok {
		return nil, errors.New("sink: The Sink '" + conf.Kind + "' does not exist")
	}
	return setupFunc(conf.Connection)
}
