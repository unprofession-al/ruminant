package main

import (
	"fmt"
	"log"
	"os"
	"ruminant/sink"
	_ "ruminant/sink/influx"
	_ "ruminant/sink/timestream"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type App struct {
	cfgFile string

	cfg struct {
		from string
		to   string
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

	// prepare time
	now := time.Now().Format(time.RFC3339)

	// root
	rootCmd := &cobra.Command{
		Use:   "ruminant",
		Short: "Feed data from ElasticSearch to sink",
	}
	rootCmd.PersistentFlags().StringVarP(&a.cfg.from, "from", "f", "none", "Beginning of the time frame to query, as RFC3339")
	rootCmd.PersistentFlags().StringVarP(&a.cfg.to, "to", "t", now, "End of the time frame to query, as RFC3339")
	rootCmd.PersistentFlags().StringVarP(&a.cfgFile, "cfg", "c", "$HOME/ruminant.yaml", "configuration file path")
	a.Execute = rootCmd.Execute

	// config
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Prints the config used to the stdout",
		Long: `Prints the configuration used to standard output. If no configuration
is passed in via '-c' the defaults are printed. This is useful either to bootstrap
a new configuration or to debug an existig config file.`,
		Run: a.configCmd,
	}
	rootCmd.AddCommand(configCmd)

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
	rootCmd.AddCommand(burpCmd)

	// vomit
	vomitCmd := &cobra.Command{
		Use:   "vomit",
		Short: "Throw up to stdout",
		Long: `Prints all time series data points to standard output. This can be
helpful for debugging reasons.`,
		Run: a.vomitCmd,
	}
	rootCmd.AddCommand(vomitCmd)

	// poop
	poopCmd := &cobra.Command{
		Use:   "poop",
		Short: "Dump data from sink to stdout",
		Long: `Dumps the content of the sink to the standard output as
CSV file. The time range can be configured.`,
		Run: a.poopCmd,
	}
	rootCmd.AddCommand(poopCmd)

	// gulp
	gulpCmd := &cobra.Command{
		Use:   "gulp",
		Short: "Feed data to InfuxDB",
		Long: `Query the ElasticSearch Database, Process the results add feed the
time series data points generated to the sink configured. This
At the end of this process, this also writes a new marker timestamp
to the sink.`,
		Run: a.gulpCmd,
	}
	rootCmd.AddCommand(gulpCmd)

	// version
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version info",
		Run:   a.versionCmd,
	}
	rootCmd.AddCommand(versionCmd)

	return a
}

func (a *App) vomitCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, true)
	if err != nil {
		log.Fatal(err)
	}

	from, err := parseTimeString(a.cfg.from)
	if err != nil {
		a.log.Fatal("Could not parse 'from' date", " error ", err.Error())
	}
	to, err := parseTimeString(a.cfg.to)
	if err != nil {
		a.log.Fatal("Could not parse 'to' date ", " error ", err.Error())
	}
	a.log.Infof("Staring at %s", from.Format(time.RFC3339))
	a.log.Infof("Ending at %s", to.Format(time.RFC3339))

	points := Ruminate(c, false, from, to, a.log)

	a.log.Infof("Printing data points\n")
	for _, p := range points {
		fmt.Println(p)
	}
}

func (a *App) poopCmd(cmd *cobra.Command, args []string) {
	/*
		c, err := NewConf(a.cfgFile, true)
		if err != nil {
			log.Fatal(err)
		}

		i, err := sink.New(c.Gulp)
		if err != nil {
			a.log.Fatalw("Could not create sink", "error", err.Error())
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
			a.log.Fatalw("Could not query sink", "error", err.Error())
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
						fmt.Printf("%s", ts.Format(c.Poop.Format))
					}
				}
				fmt.Printf("\n")
			}
		}
	*/
}

func (a *App) burpCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, true)
	if err != nil {
		log.Fatal(err)
	}

	from, err := parseTimeString(a.cfg.from)
	if err != nil {
		a.log.Fatal("Could not parse 'from' date", " error ", err.Error())
	}
	to, err := parseTimeString(a.cfg.to)
	if err != nil {
		a.log.Fatal("Could not parse 'to' date ", " error ", err.Error())
	}
	a.log.Infof("Staring at %s", from.Format(time.RFC3339))
	a.log.Infof("Ending at %s", to.Format(time.RFC3339))

	points := Ruminate(c, true, from, to, a.log)

	a.log.Infof("Printing sample data point\n")
	for _, p := range points {
		fmt.Println(p)
	}
}

func (a *App) configCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, false)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(c)
}

func (a *App) gulpCmd(cmd *cobra.Command, args []string) {
	c, err := NewConf(a.cfgFile, true)
	if err != nil {
		log.Fatal(err)
	}

	from, err := parseTimeString(a.cfg.from)
	if err != nil {
		a.log.Fatal("Could not parse 'from' date", " error ", err.Error())
	}
	to, err := parseTimeString(a.cfg.to)
	if err != nil {
		a.log.Fatal("Could not parse 'to' date ", " error ", err.Error())
	}
	a.log.Infof("Staring at %s", from.Format(time.RFC3339))
	a.log.Infof("Ending at %s", to.Format(time.RFC3339))

	points := Ruminate(c, false, from, to, a.log)

	a.log.Infow("Going to create sink")
	t, err := sink.New(c.Gulp)
	if err != nil {
		a.log.Fatalw("Could not create sink", "error", err.Error())
	}

	if len(points) < 1 {
		a.log.Infow("No data points to save")
		os.Exit(0)
	}
	a.log.Infof("Saving %d data points to sink", len(points))
	err = t.Write(points)
	if err != nil {
		a.log.Fatalw("Could not write data to sink", "error", err.Error())
	}
	a.log.Infow("Data points saved")
}

func (a *App) versionCmd(cmd *cobra.Command, args []string) {
	fmt.Println(versionInfo())
}
