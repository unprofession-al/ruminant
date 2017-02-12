package cmd

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
)

func Setup(c Config) ([]Point, Influx) {
	log.Print("Create InfluxDB client")
	i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Port)
	if err != nil {
		log.Print("Could not create influx client")
		log.Fatal(err)
	}

	log.Print("Getting latest timestamp from InfluxDB")
	latest, err := i.GetLatestInSeries(c.Gulp.Series, c.Gulp.LatestIndicator)
	if err != nil {
		log.Print("Could not get latest timestamp in series")
		log.Fatal(err)
	}
	log.Print(fmt.Sprintf("Latest entry at %v", latest))

	t := template.Must(template.New("t1").Parse(c.Regurgitate.Query))

	var query bytes.Buffer
	t.Execute(&query, ToEsTimestamp(latest))

	log.Print("Querying ElasticSearch")
	es := NewElasticSearch(c.Regurgitate.Proto, c.Regurgitate.Host, c.Regurgitate.Port)
	result, err := es.Query(c.Regurgitate.Index, c.Regurgitate.Type, query.String())
	if err != nil {
		log.Print("Could not query ElasticSearch")
		log.Fatal(err)
	}
	j, err := result.AggsAsJson()
	if err != nil {
		log.Fatal(err)
	}
	p := Point{
		Tags:        make(map[string]string),
		Values:      make(map[string]interface{}),
		Measurement: c.Gulp.Series,
	}
	log.Print("Processing results")
	out, err := Process(j, c.Ruminate.Iterator, p, 0)
	if err != nil {
		log.Print("Could not process data")
		log.Fatal(err)
	}

	return out, i
}
