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
	Short: "Feed data to InfluxDB",
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf()
		if err != nil {
			log.Fatal(err)
		}

		out, i := QueryAndProcess(c)

		l.Infof("Saving %d data points to Influx DB", len(out))
		err = i.Write(out)
		if err != nil {
			l.Fatal("Could not write data to influx", "error", err.Error())
		}

		l.Infow("Data points saved")
	},
}

func init() {
	RootCmd.AddCommand(gulpCmd)
}
