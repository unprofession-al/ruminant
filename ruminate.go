package main

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
	"time"

	"go.uber.org/zap"
)

func GetFromDate(c Config, markerOverwrite string) (time.Time, error) {
	var err error
	latest := time.Now()
	if markerOverwrite == "none" {
		t, err := NewTimestream(c.Gulp.Db, c.Gulp.Series, c.Gulp.Indicator, "eu-west-1")

		if err != nil {
			return latest, fmt.Errorf("Could net create InfluxDB client: %s", err.Error())
		}

		latest, err = t.GetLatestMarker()
		if err != nil {
			return latest, fmt.Errorf("Could not get latest timestamp in series. How you already prepared the database with 'init'?: %s", err.Error())
		}
	} else {
		latest, err = time.Parse(time.RFC3339, markerOverwrite)
	}
	return latest, err
}

func Ruminate(c Config, burp bool, from time.Time, l *zap.SugaredLogger) []Point {
	l.Infof("Reading data starting from %s", from.Format("2006-01-02 15:04:05"))

	es := NewElasticSearch(c.Regurgitate.Proto, c.Regurgitate.Host, c.Regurgitate.User, c.Regurgitate.Password, c.Regurgitate.Port)

	sampledQueries := make(map[time.Time][]string)
	interv := c.Regurgitate.Sampler.Interval
	if interv != "" {
		l.Infow("Sampler found, building queries")
		s, err := NewSampler(c.Regurgitate.Sampler)
		if err != nil {
			l.Fatalw("Error occurred", "error", err.Error())
		}
		sampledQueries = s.BuildQueries(c.Regurgitate.Query, from)
		l.Infof("A total of %d queries are built", len(sampledQueries)*c.Regurgitate.Sampler.Samples)
	} else {
		l.Infow("No sampler config found, building simple query")
		t := template.Must(template.New("t1").Parse(c.Regurgitate.Query))
		var query bytes.Buffer
		t.Execute(&query, ToEsTimestamp(from))
		sampledQueries[from] = []string{query.String()}
	}

	var points []Point
	processed := 0
	for ts, queries := range sampledQueries {
		if burp && len(points) > 0 {
			break
		}
		var samples []Point
		l.Infof("Sampling @ %s", ts.Format("2006-01-02 15:04:05"))
		for i, query := range queries {
			fmt.Printf("\n\n---\n\n%s\n\n---\n\n", query)
			l.Infof("-- Query ElasticSearch for sample %d", i)
			result, err := es.Query(c.Regurgitate.Index, c.Regurgitate.Type, query)
			if err != nil {
				l.Fatalw("-- Query failed", "error", err.Error())
			}
			j, err := result.AggsAsJson()
			if err != nil {
				log.Fatal(err)
			}
			p := Point{
				Timestamp: ts,
				Tags:      make(map[string]string),
				Values:    make(map[string]interface{}),
			}
			l.Infow("-- Processing results")
			var sample []Point
			var jsonFragment string
			if burp {
				l.Infow("Printing latest processed json fragment")
				sample, jsonFragment, err = Burp(j, c.Ruminate.Iterator, p)
				if jsonFragment != "" {
					fmt.Printf("\n%s\n\n", jsonFragment)
				}
			} else {
				sample, err = Chew(j, c.Ruminate.Iterator, p)
			}
			if err != nil {
				l.Fatalw("Could not process data", "error", err.Error())
			}
			samples = append(samples, sample...)
			processed += 1
		}

		if c.Regurgitate.Sampler.Samples > 1 {
			l.Infow("-- Calculating average of samples")
			samples = Avg(samples, c.Regurgitate.Sampler.Samples)
		}
		points = append(points, samples...)
		l.Infof("%d of %d queries run and processed", processed, len(sampledQueries)*c.Regurgitate.Sampler.Samples)
	}

	return points
}
