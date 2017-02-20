package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

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
