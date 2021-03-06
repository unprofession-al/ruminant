package main

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
	"time"

	"go.uber.org/zap"
)

func Ruminate(c Config, burp bool, l *zap.SugaredLogger) []Point {
	l.Infow("Going to create InfluxDB client")
	i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
	if err != nil {
		l.Fatal("Could net create InfluxDB client", "error", err.Error())
	}

	l.Infow("Getting latest timestamp from InfluxDB")
	latest, err := i.GetLatestMarker()
	if err != nil {
		l.Fatalw("Could not get latest timestamp in series. How you already prepared the database with 'init'?", "error", err.Error())
	}
	l.Infof("Latest entry at %s", latest.Format("2006-01-02 15:04:05"))

	es := NewElasticSearch(c.Regurgitate.Proto, c.Regurgitate.Host, c.Regurgitate.Port)

	sampledQueries := make(map[time.Time][]string)
	interv := c.Regurgitate.Sampler.Interval
	if interv != "" {
		l.Infow("Sampler found, building queries")
		s, err := NewSampler(c.Regurgitate.Sampler)
		if err != nil {
			l.Fatalw("Error occurred", "error", err.Error())
		}
		sampledQueries = s.BuildQueries(c.Regurgitate.Query, latest)
		l.Infof("A total of %d queries are built", len(sampledQueries)*c.Regurgitate.Sampler.Samples)
	} else {
		l.Infow("No sampler config found, building simple query")
		t := template.Must(template.New("t1").Parse(c.Regurgitate.Query))
		var query bytes.Buffer
		t.Execute(&query, ToEsTimestamp(latest))
		sampledQueries[latest] = []string{query.String()}
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
