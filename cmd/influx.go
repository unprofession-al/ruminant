package cmd

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type Point struct {
	Timestamp   time.Time
	Tags        map[string]string
	Values      map[string]interface{}
	Measurement string
}

func (p Point) Copy() Point {
	tags := make(map[string]string)
	for k, v := range p.Tags {
		tags[k] = v
	}

	values := make(map[string]interface{})
	for k, v := range p.Values {
		values[k] = v
	}

	c := Point{
		Timestamp:   p.Timestamp,
		Tags:        tags,
		Values:      values,
		Measurement: p.Measurement,
	}
	return c
}

type Influx struct {
	DB     string
	Client client.Client
}

func NewInflux(host, proto, db, user, pass string, port int) (Influx, error) {
	i := Influx{}
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     fmt.Sprintf("%s://%s:%d", proto, host, port),
		Username: user,
		Password: pass,
	})
	if err != nil {
		return i, err
	}

	i = Influx{
		DB:     db,
		Client: c,
	}
	return i, nil
}

func (i Influx) Write(points []Point) error {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  i.DB,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	for _, p := range points {
		pt, err := client.NewPoint(
			p.Measurement,
			p.Tags,
			p.Values,
			p.Timestamp,
		)
		if err != nil {
			return err
		}
		bp.AddPoint(pt)
	}
	if err := i.Client.Write(bp); err != nil {
		return err
	}
	return nil
}
