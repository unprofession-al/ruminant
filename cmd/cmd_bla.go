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

// blaCmd represents the bla command
var blaCmd = &cobra.Command{
	Use:   "bla",
	Short: "bla does stuff for reasons",
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf()
		if err != nil {
			log.Fatal(err)
		}
		interv := c.Regurgitate.Sampler.Interval
		if interv != "" {
			s, err := NewSampler(c.Regurgitate.Sampler)
			if err != nil {
				l.Fatal("Error occured", "error", err.Error())
			}
			fmt.Println(s)
		} else {
			l.Infow("No Sampler found")
		}

	},
}

func init() {
	RootCmd.AddCommand(blaCmd)
}
