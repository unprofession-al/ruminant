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

// gulpCmd represents the gulp command
var gulpCmd = &cobra.Command{
	Use:   "gulp",
	Short: "Feed data to Infux DB",
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf()
		if err != nil {
			log.Fatal(err)
		}

		points := Ruminate(c, false)

		l.Infow("Going to create InfluxDB client")
		i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
		if err != nil {
			l.Fatal("Could net create InfluxDB client", "error", err.Error())
		}

		l.Infof("Saving %d data points to InfluxDB", len(points))
		err = i.Write(points)
		if err != nil {
			l.Fatalw("Could not write data to InfluxDB", "error", err.Error())
		}
		l.Infow("Data points saved")

	},
}

func init() {
	RootCmd.AddCommand(gulpCmd)
}
