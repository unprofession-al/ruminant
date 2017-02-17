package cmd

import (
	"bytes"
	"html/template"
	"log"
)

func QueryAndProcess(c Config) ([]Point, Influx) {
	l.Infow("Going to create InfluxDB client")
	i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
	if err != nil {
		l.Fatal("Could net create InfluxDB client", "error", err.Error())
	}

	l.Infow("Getting latest timestamp from InfluxDB")
	latest, err := i.GetLatestMarker()
	if err != nil {
		l.Fatal("Could not get latest timestamp in series", "error", err.Error())
	}
	l.Infof("Latest entry at %s", latest.Format("2006-01-02 15:04:05"))

	t := template.Must(template.New("t1").Parse(c.Regurgitate.Query))

	var query bytes.Buffer
	t.Execute(&query, ToEsTimestamp(latest))

	l.Infow("Querying ElasticSearch")
	es := NewElasticSearch(c.Regurgitate.Proto, c.Regurgitate.Host, c.Regurgitate.Port)
	result, err := es.Query(c.Regurgitate.Index, c.Regurgitate.Type, query.String())
	if err != nil {
		l.Fatal("Could not query ElasticSearch", "error", err.Error())
	}
	j, err := result.AggsAsJson()
	if err != nil {
		log.Fatal(err)
	}
	p := Point{
		Tags:   make(map[string]string),
		Values: make(map[string]interface{}),
	}
	l.Infow("Processing results")
	out, err := Process(j, c.Ruminate.Iterator, p, 0)
	if err != nil {
		l.Fatal("Could not process data", "error", err.Error())
	}

	return out, i
}
