package main

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

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
	}
	if response.Error() != nil {
		err = response.Error()
		return
	}
	res = response.Results
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("latest Timestamp could not be found")
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
