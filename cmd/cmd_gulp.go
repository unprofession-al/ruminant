package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

var gulpCmd = &cobra.Command{
	Use:   "gulp",
	Short: "Feed data to InfuxDB",
	Long: `Query the ElasticSearch Database, Process the results add feed the
time series data points generated to the InfluxDB configured. This
At the end of this process, this also writes a new marker timestamp
to the InfluxDB.`,
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf(true)
		if err != nil {
			log.Fatal(err)
		}

		points := Ruminate(c, false)

		l.Infow("Going to create InfluxDB client")
		i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
		if err != nil {
			l.Fatalw("Could not create InfluxDB client", "error", err.Error())
		}

		l.Infof("Saving %d data points to InfluxDB", len(points))
		err = i.Write(points)
		if err != nil {
			l.Fatalw("Could not write data to InfluxDB", "error", err.Error())
		}
		l.Infow("Data points saved")

	},
}

func init() {
	RootCmd.AddCommand(gulpCmd)
}
