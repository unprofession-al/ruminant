// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

// blaCmd represents the bla command
var blaCmd = &cobra.Command{
	Use:   "bla",
	Short: "bla does stuff for reasons",
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf()
		if err != nil {
			log.Fatal(err)
		}

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

		es := NewElasticSearch(c.Regurgitate.Proto, c.Regurgitate.Host, c.Regurgitate.Port)

		interv := c.Regurgitate.Sampler.Interval
		if interv != "" {
			s, err := NewSampler(c.Regurgitate.Sampler)
			if err != nil {
				l.Fatal("Error occured", "error", err.Error())
			}
			out := s.BuildQueries(c.Regurgitate.Query, latest)
			for ts, queries := range out {
				var samples [][]Point
				l.Infof("Sampling @ %s", ts.Format("2006-01-02 15:04:05"))
				for i, query := range queries {
					l.Infof("Query ElaticSearch for Sample %d", i)
					result, err := es.Query(c.Regurgitate.Index, c.Regurgitate.Type, query)
					if err != nil {
						l.Fatal("Query failed", "error", err.Error())
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
					l.Infow("Processing results")
					sample, err := Process(j, c.Ruminate.Iterator, p, 0)
					if err != nil {
						l.Fatalw("Could not process data", "error", err.Error())
					}
					samples = append(samples, sample)
				}
			}
		} else {
			l.Infow("No Sampler found")
		}

	},
}

func init() {
	RootCmd.AddCommand(blaCmd)
}
