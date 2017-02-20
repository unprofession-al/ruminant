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

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Prints the config used to the stdout",
	Long: `Prints the configuration used to standard output. If no configuration
is passed in via '-c' the defaults are printed. This is useful either to bootstrap
a new configuration or to debug an existig config file.`,
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf(false)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(c)
	},
}

func init() {
	RootCmd.AddCommand(configCmd)
}
