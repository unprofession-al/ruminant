package cmd

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var burpCmd = &cobra.Command{
	Use:   "burp",
	Short: "Test the query and iterator",
	Long: `'burp' works simiar no the 'vomit' command. But in contrast to
'vomit', 'burp' does only print one data point instead of all. It also
prints the json fragment that has been processed last by your ruminate
iterators. This is useful for debugging existing ruminate configurations
or creating new ones.`,
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf(true)
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
