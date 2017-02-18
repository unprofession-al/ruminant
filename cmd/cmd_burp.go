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

// burpCmd represents the burp command
var burpCmd = &cobra.Command{
	Use:   "burp",
	Short: "Test the query and iterator",
	Long: `'burp' works simiar no the 'vomit' command. But in contrast to
'vomit', 'burp' does only print one data point instead of all. It also
prints the json fragment that has been processed last by your ruminate
iterators. This is useful for debugging existing ruminate configurations
or creating new ones.`,
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf()
		if err != nil {
			log.Fatal(err)
		}

		points := Ruminate(c, true)

		l.Infof("Printing sample data point\n")
		for _, p := range points {
			fmt.Println(p)
		}

	},
}

func init() {
	RootCmd.AddCommand(burpCmd)
}
