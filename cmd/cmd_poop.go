package cmd

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"text/template"
	"time"

	"github.com/spf13/cobra"
)

var poopCmd = &cobra.Command{
	Use:   "poop",
	Short: "Dump data from InfluxDB to stdout",
	Long: `Dumps the content of the InfluxDB to the standard output as
CSV file. The time range can be configured.`,
	Run: func(cmd *cobra.Command, args []string) {
		c, err := Conf(true)
		if err != nil {
			log.Fatal(err)
		}

		i, err := NewInflux(c.Gulp.Host, c.Gulp.Proto, c.Gulp.Db, c.Gulp.User, c.Gulp.Pass, c.Gulp.Series, c.Gulp.Indicator, c.Gulp.Port)
		if err != nil {
			l.Fatalw("Could not create InfluxDB client", "error", err.Error())
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
			l.Infof("Query was: %s", query)
			l.Fatalw("Could not query InfluxDB", "error", err.Error())
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
	},
}

func init() {
	RootCmd.AddCommand(poopCmd)
}
