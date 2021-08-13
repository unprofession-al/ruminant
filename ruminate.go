package main

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
	"ruminant/sink"
	"time"

	"go.uber.org/zap"
)

func Ruminate(c Config, burp bool, from, to time.Time, l *zap.SugaredLogger) []sink.Point {
	l.Infof("Reading data starting from %s", from.Format("2006-01-02 15:04:05"))
	l.Infof("Reading data starting to %s", to.Format("2006-01-02 15:04:05"))

	var templateData = struct {
		From int64
		To   int64
	}{
		ToEsTimestamp(from),
		ToEsTimestamp(to),
	}

	es := NewElasticSearch(c.Regurgitate.BaseURL, c.Regurgitate.User, c.Regurgitate.Password)

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
		t.Execute(&query, templateData)
		sampledQueries[from] = []string{query.String()}
	}

	var points []sink.Point
	processed := 0
	for ts, queries := range sampledQueries {
		if burp && len(points) > 0 {
			break
		}
		var samples []sink.Point
		l.Infof("Sampling @ %s", ts.Format("2006-01-02 15:04:05"))
		for i, query := range queries {
			//fmt.Printf("\n\n---\n\n%s\n\n---\n\n", query)
			l.Infof("-- Query ElasticSearch for sample %d", i)
			result, err := es.Query(c.Regurgitate.Index, c.Regurgitate.Type, query)
			if err != nil {
				l.Fatalw("-- Query failed", "error", err.Error())
			}
			j, err := result.AggsAsJSON()
			if err != nil {
				log.Fatal(err)
			}
			p := sink.Point{
				Timestamp: ts,
				Tags:      make(map[string]string),
				Values:    make(map[string]interface{}),
			}
			l.Infow("-- Processing results")
			var sample []sink.Point
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
			processed++
		}

		if c.Regurgitate.Sampler.Samples > 1 {
			l.Infow("-- Calculating average of samples")
			samples = sink.PointAvg(samples, c.Regurgitate.Sampler.Samples)
		}
		points = append(points, samples...)
		l.Infof("%d of %d queries run and processed", processed, len(sampledQueries)*c.Regurgitate.Sampler.Samples)
	}

	return points
}
