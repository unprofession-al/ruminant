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

	"github.com/spf13/cobra"
)

// vomitCmd represents the vomit command
var vomitCmd = &cobra.Command{
	Use:   "vomit",
	Short: "Throw up to standart output",
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
		store := make(map[string]string)
		out, err := Process(j, c.Ruminate.Iterator, store)
		if err != nil {
			log.Fatal(err)
		}

		for _, point := range out {
			fmt.Println(point)
		}
	},
}

func init() {
	RootCmd.AddCommand(vomitCmd)
}
