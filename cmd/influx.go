package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type Point struct {
	Timestamp   time.Time
	Tags        map[string]string
	Values      map[string]interface{}
	Measurement string
}

func (p Point) String() string {
	out := new(bytes.Buffer)
	timestamp := p.Timestamp.Format("2006-01-02 15:04:05")

	const padding = 1

	var valuesStr []string
	for key, val := range p.Values {
		valuesStr = append(valuesStr, fmt.Sprintf("%s: %v", key, val))
	}

	var tagsStr []string
	for key, val := range p.Tags {
		tagsStr = append(tagsStr, fmt.Sprintf("%s: %s", key, val))
	}

	var iterations int

	if len(tagsStr) > len(valuesStr) {
		iterations = len(tagsStr)
	} else {
		iterations = len(valuesStr)
	}

	w := tabwriter.NewWriter(out, 0, 0, padding, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintf(w, "%s \tTags \tValues \t %s\n", p.Measurement, timestamp)
	for i := 0; i < iterations; i++ {
		tag := ""
		if len(tagsStr) > i {
			tag = tagsStr[i]
		}
		value := ""
		if len(valuesStr) > i {
			value = valuesStr[i]
		}
		fmt.Fprintf(w, "\t%s \t%s \t\n", tag, value)
	}
	w.Flush()

	return string(out.String())
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

const LatestIndicator = "RUMINANT_LAST_RUN"

func (i Influx) GetLatestInSeries(series string) (t time.Time, err error) {
	var res []client.Result
	q := client.Query{
		Command:  fmt.Sprintf("SELECT last(%s) FROM %s", LatestIndicator, series),
		Database: i.DB,
	}
	response, err := i.Client.Query(q)
	if err != nil {
		return
	} else {
		if response.Error() != nil {
			err = response.Error()
			return
		}
		res = response.Results
	}
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("Latest Timestamp could not be found")
			return
		}
	}()
	t, err = time.Parse(time.RFC3339, res[0].Series[0].Values[0][0].(string))
	if err != nil {
		return
	}
	return
}

func (i Influx) Query(cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: i.DB,
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

func (i Influx) Write(points []Point) error {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  i.DB,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	var newest time.Time
	series := ""
	for _, p := range points {
		pt, err := client.NewPoint(p.Measurement, p.Tags, p.Values, p.Timestamp)
		if err != nil {
			return err
		}
		if p.Timestamp.After(newest) {
			newest = p.Timestamp
		}
		if series == "" {
			series = p.Measurement
		}
		bp.AddPoint(pt)
	}

	tags := map[string]string{"ruminant": "system"}
	fields := map[string]interface{}{LatestIndicator: "write"}
	p, err := client.NewPoint(series, tags, fields, newest)
	if err != nil {
		return err
	}
	bp.AddPoint(p)

	if err := i.Client.Write(bp); err != nil {
		return err
	}
	return nil
}
