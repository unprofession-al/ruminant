package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var vomitCmd = &cobra.Command{
	Use:   "vomit",
	Short: "Throw up to standart output",
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf(true)
		if err != nil {
			log.Fatal(err)
		}

		points := Ruminate(c, false)

		l.Infof("Printing data points\n")
		for _, p := range points {
			fmt.Println(p)
		}

	},
}

func init() {
	RootCmd.AddCommand(vomitCmd)
}
