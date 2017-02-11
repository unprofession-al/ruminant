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
	Short: "A brief description of your command",
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf()
		if err != nil {
			log.Fatal(err)
		}
		es := NewElasticSearch(c.Regurgitate.Proto, c.Regurgitate.Host, c.Regurgitate.Port)
		result, err := es.Query(c.Regurgitate.Index, c.Regurgitate.Type, c.Regurgitate.Query)
		if err != nil {
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
		out, err := Process(j, c.Ruminate.Iterator, p, 0)
		if err != nil {
			log.Fatal(err)
		}

		i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Port)
		if err != nil {
			log.Fatal(err)
		}

		err = i.Write(out)
		if err != nil {
			log.Fatal(err)
		}

	},
}

func init() {
	RootCmd.AddCommand(vomitCmd)
}
