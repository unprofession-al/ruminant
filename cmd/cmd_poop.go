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
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
)

// poopCmd represents the poop command
var poopCmd = &cobra.Command{
	Use:   "poop",
	Short: "Dump data from Infux DB to stdout",
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf(true)
		if err != nil {
			log.Fatal(err)
		}

		i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
		if err != nil {
			l.Fatalw("Could not create InfluxDB client", "error", err.Error())
		}

		t := template.Must(template.New("query").Parse(c.Poop.Query))

		qd := struct {
			Fields []string
			Series string
			Start  string
			End    string
		}{
			Fields: c.Poop.Fields,
			Series: c.Gulp.Series,
			Start:  c.Poop.Start,
			End:    c.Poop.End,
		}

		var query bytes.Buffer
		t.Execute(&query, qd)
		res, err := i.Query(query.String())
		if err != nil {
			l.Fatalw("Could not query InfluxDB", "error", err.Error())
		}

		wr := csv.NewWriter(os.Stdout)
		wr.Write(c.Poop.Fields)
		wr.Flush()
		for _, point := range res[0].Series[0].Values {
			st := strings.Fields(strings.Trim(fmt.Sprint(point), "[]"))
			wr := csv.NewWriter(os.Stdout)
			wr.Write(st)
			wr.Flush()
		}
	},
}

func init() {
	RootCmd.AddCommand(poopCmd)
}
