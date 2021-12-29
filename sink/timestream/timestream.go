package timestream

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"ruminant/sink"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"golang.org/x/net/http2"
)

func init() {
	sink.Register("timestream", setup)
}

type Timestream struct {
	DB          string
	WriteClient *timestreamwrite.Client
	QueryClient *timestreamquery.Client
	Series      string
}

func setup(c map[string]string) (sink.Sink, error) {
	t := Timestream{}

	var db, series, region string
	var found bool

	if db, found = c["db"]; !found || db == "" {
		return t, fmt.Errorf("sink requires field 'db' to be set")
	}
	if series, found = c["series"]; !found || series == "" {
		return t, fmt.Errorf("sink requires field 'series' to be set")
	}
	if region, found = c["region"]; !found || region == "" {
		return t, fmt.Errorf("sink requires field 'region' to be set")
	}

	tr := &http.Transport{
		ResponseHeaderTimeout: 20 * time.Second,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			DualStack: true,
			Timeout:   30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	http2.ConfigureTransport(tr)

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	//sess, err := session.NewSession(&aws.Config{Region: aws.String(region), MaxRetries: aws.Int(10), HTTPClient: &http.Client{Transport: tr}})
	if err != nil {
		return t, err
	}
	t.WriteClient = timestreamwrite.NewFromConfig(cfg)
	t.QueryClient = timestreamquery.NewFromConfig(cfg)

	t.DB = db
	t.Series = series

	return t, nil
}

func (t Timestream) Write(points []sink.Point) error {
	if len(points) < 1 {
		return fmt.Errorf("no points to be written")
	}

	version := time.Now().Round(time.Millisecond).UnixNano()
	records := []types.Record{}
	for _, p := range points {
		// get dimensions from tags
		dimensions := []types.Dimension{}
		for k, v := range p.Tags {
			d := types.Dimension{
				Name:  aws.String(k),
				Value: aws.String(v),
			}
			dimensions = append(dimensions, d)
		}

		// get records
		for k, v := range p.Values {
			r := types.Record{
				Version:          version,
				Dimensions:       dimensions,
				MeasureName:      aws.String(k),
				MeasureValue:     aws.String(fmt.Sprintf("%v", v)),
				MeasureValueType: "DOUBLE",
				Time:             aws.String(strconv.FormatInt(p.Copy().Timestamp.Unix(), 10)),
				TimeUnit:         "SECONDS",
			}
			records = append(records, r)
		}
	}

	chunkSize := 100
	for i := 0; i < len(records); i += chunkSize {
		end := i + chunkSize

		if end > len(records) {
			end = len(records)
		}

		chunk := &timestreamwrite.WriteRecordsInput{
			DatabaseName: aws.String(t.DB),
			TableName:    aws.String(t.Series),
			Records:      records[i:end],
		}

		if _, err := t.WriteClient.WriteRecords(context.TODO(), chunk); err != nil {
			return err
		}
	}

	return nil
}

func (t Timestream) Close() {
}
