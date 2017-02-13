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
	"fmt"
	"log"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/spf13/cobra"
)

// vomitCmd represents the vomit command
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "creates the Database if required and sets a start date",
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf()
		if err != nil {
			log.Fatal(err)
		}

		log.Print("Create InfluxDB client")
		i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Port)
		if err != nil {
			log.Print("Could not create influx client")
			log.Fatal(err)
		}

		log.Print(fmt.Sprintf("Create InfluxDB %s", c.Gulp.Db))
		_, err = i.Query(fmt.Sprintf("CREATE DATABASE %s", c.Gulp.Db))
		if err != nil {
			log.Print("Could not create database")
			log.Fatal(err)
		}

		log.Print("Creating initial timestamp")

		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  i.DB,
			Precision: "s",
		})
		if err != nil {
			log.Fatal(err)
		}

		tags := map[string]string{"ruminant": "system"}
		fields := map[string]interface{}{LatestIndicator: "init"}
		timestamp := time.Now().Add(-(time.Hour * 24))
		pt, err := client.NewPoint(c.Gulp.Series, tags, fields, timestamp)
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)

		if err := i.Client.Write(bp); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(initCmd)
}
