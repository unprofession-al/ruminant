package cmd

import (
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/spf13/cobra"
)

var (
	cfgFile string
	l       *zap.SugaredLogger
)

var RootCmd = &cobra.Command{
	Use:   "ruminant",
	Short: "Feed data from ElasticSearch to InfluxDB",
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&cfgFile, "cfg", "c", "$HOME/ruminant.yaml", "config file (default is $HOME/ruminant.yaml)")
}
