package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type Point struct {
	Timestamp time.Time
	Tags      map[string]string
	Values    map[string]interface{}
}

func Avg(points []Point, samples int) []Point {
	var measurements []Point
	for _, point := range points {
		found := false
		for _, measurement := range measurements {
			if measurement.Timestamp == point.Timestamp && reflect.DeepEqual(measurement.Tags, point.Tags) {
				for key, value := range point.Values {
					add, _ := value.(float64)
					pre, _ := measurement.Values[key].(float64)
					measurement.Values[key] = pre + add
				}
				found = true
				break
			}
		}
		if !found {
			measurements = append(measurements, point.Copy())
		}
	}

	for index, measurement := range measurements {
		for key, value := range measurement.Values {
			pre, _ := value.(float64)
			measurements[index].Values[key] = pre / float64(samples)
		}
	}

	return measurements
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

	w := tabwriter.NewWriter(out, 0, 0, padding, ' ', tabwriter.Debug)
	fmt.Fprintf(w, "@%s\t Tags\t Values\n", timestamp)
	for i := 0; i < iterations; i++ {
		tag := ""
		if len(tagsStr) > i {
			tag = tagsStr[i]
		}
		value := ""
		if len(valuesStr) > i {
			value = valuesStr[i]
		}
		fmt.Fprintf(w, "\t %s\t %s\n", tag, value)
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
		Timestamp: p.Timestamp,
		Tags:      tags,
		Values:    values,
	}
	return c
}

type Influx struct {
	DB        string
	Client    client.Client
	Series    string
	Indicator string
}

func NewInflux(host, proto, db, user, pass, series, indicator string, port int) (Influx, error) {
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
		DB:        db,
		Client:    c,
		Series:    series,
		Indicator: indicator,
	}
	return i, nil
}

const LatestIndicator = "RUMINANT_LAST_RUN"

func (i Influx) GetLatestMarker() (t time.Time, err error) {
	var res []client.Result
	q := client.Query{
		Command:  fmt.Sprintf("SELECT last(%s) FROM %s WHERE ruminant = '%s'", LatestIndicator, i.Series, i.Indicator),
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

func (i Influx) DeleteLatestMarker() error {
	q := client.Query{
		Command:  fmt.Sprintf("DELETE FROM %s WHERE ruminant = '%s'", i.Series, i.Indicator),
		Database: i.DB,
	}
	_, err := i.Client.Query(q)
	return err
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

func (i Influx) Write(points []Point) error {
	if len(points) < 1 {
		return errors.New("No points to be written")
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

	bp.AddPoint(i.LatestMarker(newest, "write"))

	if err := i.Client.Write(bp); err != nil {
		return err
	}
	return nil
}

func (i Influx) LatestMarker(t time.Time, note string) *client.Point {
	tags := map[string]string{"ruminant": i.Indicator}
	fields := map[string]interface{}{
		LatestIndicator: fmt.Sprintf("%s: %s", i.Indicator, note),
	}
	p, _ := client.NewPoint(i.Series, tags, fields, t)
	return p
}
