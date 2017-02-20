package cmd

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/spf13/cobra"
)

var initOffset int

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Creates the Database if required and sets a start date",
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf(true)
		if err != nil {
			log.Fatal(err)
		}

		l.Infow("Going to create InfluxDB client")
		i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
		if err != nil {
			l.Fatal("Could net create InfluxDB client", "error", err.Error())
		}

		l.Infof("Create InfluxDB %s", c.Gulp.Db)
		_, err = i.Query(fmt.Sprintf("CREATE DATABASE %s", c.Gulp.Db))
		if err != nil {
			l.Fatal("Could not create database", "error", err.Error())
		}

		l.Infof("Creating initial timestamp with an offset of %d hours", initOffset)
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{Database: i.DB, Precision: "s"})
		if err != nil {
			l.Fatal("Could create initial timestamp", "error", err.Error())
		}

		timestamp := time.Now().Add(-(time.Hour * time.Duration(initOffset)))
		bp.AddPoint(i.LatestMarker(timestamp, "init"))

		if err := i.Client.Write(bp); err != nil {
			l.Fatal("Could save initial timestamp", "error", err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(initCmd)
	initCmd.PersistentFlags().IntVarP(&initOffset, "offset", "o", 24, "Offset of the initial timestamp in hours")
}
