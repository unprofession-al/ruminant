package influx

import (
	"fmt"
	"ruminant/sink"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

func init() {
	sink.Register("influx", setup)
}

type Influx struct {
	DB     string
	Client client.Client
	Series string
}

func setup(config map[string]string) (sink.Sink, error) {
	i := Influx{}

	var found bool
	var addr, db, user, pass, series string

	if addr, found = config["addr"]; !found || addr == "" {
		return i, fmt.Errorf("sink requires field 'addr' to be set")
	}
	if db, found = config["db"]; !found || db == "" {
		return i, fmt.Errorf("sink requires field 'db' to be set")
	}
	if user, found = config["user"]; !found || user == "" {
		return i, fmt.Errorf("sink requires field 'user' to be set")
	}
	if pass, found = config["pass"]; !found || pass == "" {
		return i, fmt.Errorf("sink requires field 'pass' to be set")
	}
	if series, found = config["series"]; !found || series == "" {
		return i, fmt.Errorf("sink requires field 'series' to be set")
	}

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     addr,
		Username: user,
		Password: pass,
	})
	if err != nil {
		return i, err
	}

	i = Influx{
		DB:     db,
		Client: c,
		Series: series,
	}
	return i, nil
}

func (i Influx) Query(cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: i.DB,
		Chunked:  true,
	}
	if response, err := i.Client.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

func (i Influx) Write(points []sink.Point) error {
	if len(points) < 1 {
		return fmt.Errorf("no points to be written")
	}
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  i.DB,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	var newest time.Time
	for _, p := range points {

		pt, err := client.NewPoint(i.Series, p.Tags, p.Values, p.Timestamp)
		if err != nil {
			return err
		}
		if p.Timestamp.After(newest) {
			newest = p.Timestamp
		}
		bp.AddPoint(pt)
	}

	if err := i.Client.Write(bp); err != nil {
		return err
	}
	return nil
}
