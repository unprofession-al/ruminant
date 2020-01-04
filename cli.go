package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"text/template"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type App struct {
	cfgFile string

	cfg struct {
		initOffset int
		initDelete bool
	}

	log *zap.SugaredLogger

	// entry point
	Execute func() error
}

func NewApp() *App {
	a := &App{}

	// prepare logger
	c := zap.NewDevelopmentConfig()
	c.DisableCaller = true
	c.DisableStacktrace = true
	logger, _ := c.Build()
	a.log = logger.Sugar()

	// init
	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Prepares the InfluxDB to be used with Ruminant",
		Long: `Creates the InfluxDB as configured at sets an initial marker
timestamp with a given offset in relation to the current time.`,
		Run: a.initCmd,
	}
	initCmd.PersistentFlags().IntVarP(&a.cfg.initOffset, "offset", "o", 24, "Offset of the initial timestamp in hours")
	initCmd.PersistentFlags().BoolVarP(&a.cfg.initDelete, "delete", "d", false, "Delete existing timestamps")

	// config
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Prints the config used to the stdout",
		Long: `Prints the configuration used to standard output. If no configuration
is passed in via '-c' the defaults are printed. This is useful either to bootstrap
a new configuration or to debug an existig config file.`,
		Run: a.configCmd,
	}

	// burp
	burpCmd := &cobra.Command{
		Use:   "burp",
		Short: "Test the query and iterator",
		Long: `'burp' works simiar no the 'vomit' command. But in contrast to
 'vomit', 'burp' does only print one data point instead of all. It also
 prints the json fragment that has been processed last by your ruminate
 iterators. This is useful for debugging existing ruminate configurations
 or creating new ones.`,
		Run: a.burpCmd,
	}

	// vomit
	vomitCmd := &cobra.Command{
		Use:   "vomit",
		Short: "Throw up to stdout",
		Long: `Prints all time series data points to standard output. This can be
helpful for debugging reasons.`,
		Run: a.vomitCmd,
	}

	// poop
	poopCmd := &cobra.Command{
		Use:   "poop",
		Short: "Dump data from InfluxDB to stdout",
		Long: `Dumps the content of the InfluxDB to the standard output as
CSV file. The time range can be configured.`,
		Run: a.poopCmd,
	}

	// gulp
	gulpCmd := &cobra.Command{
		Use:   "gulp",
		Short: "Feed data to InfuxDB",
		Long: `Query the ElasticSearch Database, Process the results add feed the
time series data points generated to the InfluxDB configured. This
At the end of this process, this also writes a new marker timestamp
to the InfluxDB.`,
		Run: a.gulpCmd,
	}

	// version
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version info",
		Run:   a.versionCmd,
	}

	// root
	rootCmd := &cobra.Command{
		Use:   "ruminant",
		Short: "Feed data from ElasticSearch to InfluxDB",
	}
	rootCmd.PersistentFlags().StringVarP(&a.cfgFile, "cfg", "c", "$HOME/ruminant.yaml", "configuration file path")
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(configCmd)
	rootCmd.AddCommand(vomitCmd)
	rootCmd.AddCommand(burpCmd)
	rootCmd.AddCommand(gulpCmd)
	rootCmd.AddCommand(poopCmd)
	rootCmd.AddCommand(versionCmd)
	a.Execute = rootCmd.Execute

	return a
}

func (a App) vomitCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, true)
	if err != nil {
		log.Fatal(err)
	}

	points := Ruminate(c, false, a.log)

	a.log.Infof("Printing data points\n")
	for _, p := range points {
		fmt.Println(p)
	}
}

func (a App) poopCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, true)
	if err != nil {
		log.Fatal(err)
	}

	i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
	if err != nil {
		a.log.Fatalw("Could not create InfluxDB client", "error", err.Error())
	}

	t := template.Must(template.New("query").Parse(c.Poop.Query))

	qd := struct {
		Fields []string
		Series string
		Start  string
		End    string
	}{
		Fields: c.Poop.Fields,
		Series: c.Gulp.Series,
		Start:  c.Poop.Start,
		End:    c.Poop.End,
	}

	var query bytes.Buffer
	t.Execute(&query, qd)
	res, err := i.Query(query.String())
	if err != nil {
		a.log.Infof("Query was: %s", query)
		a.log.Fatalw("Could not query InfluxDB", "error", err.Error())
	}

	wr := csv.NewWriter(os.Stdout)
	wr.Write(c.Poop.Fields)
	wr.Flush()
	for _, chunk := range res {
		if len(chunk.Series) < 1 {
			continue
		}
		for _, point := range chunk.Series[0].Values {
			for i, elem := range point {
				if i != 0 {
					fmt.Printf(c.Poop.Separator)
					switch elem := elem.(type) {
					default:
						if elem == nil {
							fmt.Printf(c.Poop.ReplaceNil)
						} else {
							fmt.Printf("%v", elem)
						}
					case json.Number:
						number, err := elem.Float64()
						if err != nil {
							fmt.Println(err)
							break
						}
						fmt.Printf("%.0f", number)
					}
				} else {
					ts, err := time.Parse("2006-01-02T15:04:05Z", elem.(string))
					if err != nil {
						fmt.Println(err)
						break
					}
					fmt.Printf(ts.Format(c.Poop.Format))
				}
			}
			fmt.Printf("\n")
		}
	}
}

func (a App) initCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, true)
	if err != nil {
		log.Fatal(err)
	}

	a.log.Infow("Going to create InfluxDB client")
	i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
	if err != nil {
		a.log.Fatal("Could net create InfluxDB client", "error", err.Error())
	}

	//a.log.Infof("Create InfluxDB %s", c.Gulp.Db)
	//_, err = i.Query(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.Gulp.Db))
	//if err != nil {
	//	a.log.Fatal("Could not create database", "error", err.Error())
	//}
	if a.cfg.initDelete {
		a.log.Infow("Deleting existing timestamps")
		i.DeleteLatestMarker()
		if err != nil {
			a.log.Fatal("Could not create database", "error", err.Error())
		}
	}

	a.log.Infof("Creating initial timestamp with an offset of %d hours", a.cfg.initOffset)
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{Database: i.DB, Precision: "s"})
	if err != nil {
		a.log.Fatal("Could create initial timestamp", "error", err.Error())
	}

	timestamp := time.Now().Add(-(time.Hour * time.Duration(a.cfg.initOffset)))
	bp.AddPoint(i.LatestMarker(timestamp, "init"))

	if err := i.Client.Write(bp); err != nil {
		a.log.Fatal("Could save initial timestamp", "error", err.Error())
	}
}

func (a *App) burpCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, true)
	if err != nil {
		log.Fatal(err)
	}

	points := Ruminate(c, true, a.log)

	a.log.Infof("Printing sample data point\n")
	for _, p := range points {
		fmt.Println(p)
	}
}

func (a App) configCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, false)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(c)
}

func (a App) gulpCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, true)
	if err != nil {
		log.Fatal(err)
	}

	points := Ruminate(c, false, a.log)

	a.log.Infow("Going to create InfluxDB client")
	i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
	if err != nil {
		a.log.Fatalw("Could not create InfluxDB client", "error", err.Error())
	}

	if len(points) < 1 {
		a.log.Infow("No data points to save")
		os.Exit(0)
	}
	a.log.Infof("Saving %d data points to InfluxDB", len(points))
	err = i.Write(points)
	if err != nil {
		a.log.Fatalw("Could not write data to InfluxDB", "error", err.Error())
	}
	a.log.Infow("Data points saved")
}

func (a App) versionCmd(cmd *cobra.Command, args []string) {
	fmt.Println(versionInfo())
}
