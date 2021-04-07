package main

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"golang.org/x/net/http2"
)

type Timestream struct {
	DB          string
	WriteClient *timestreamwrite.TimestreamWrite
	QueryClient *timestreamquery.TimestreamQuery
	Series      string
	Indicator   string
}

func NewTimestream(db, series, indicator, region string) (Timestream, error) {
	t := Timestream{}

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

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region), MaxRetries: aws.Int(10), HTTPClient: &http.Client{Transport: tr}})
	if err != nil {
		return t, err
	}
	t.WriteClient = timestreamwrite.New(sess)
	t.QueryClient = timestreamquery.New(sess)

	t.DB = db
	t.Series = series
	t.Indicator = indicator

	return t, nil
}

func (t Timestream) GetLatestMarker() (tm time.Time, err error) {
	// SELECT max(time) as latest_time FROM "test_ltmed".test WHERE measure_name = 'RUMINANT_LAST_RUN' AND ruminant = 'lsa_segmented'
	q := &timestreamquery.QueryInput{
		QueryString: aws.String(fmt.Sprintf("SELECT max(time) as latest_time FROM \"%s\".%s WHERE measure_name = '%s' AND ruminant = '%s'", t.DB, t.Series, LatestIndicator, t.Indicator)),
	}
	response, err := t.QueryClient.Query(q)
	if err != nil {
		return
	}
	timestamp := *response.Rows[0].Data[0].ScalarValue
	tm, err = time.Parse("2006-01-02 15:04:05.000000000", timestamp)
	if err != nil {
		return
	}
	return
}

func (t Timestream) Write(points []Point) error {
	if len(points) < 1 {
		return fmt.Errorf("no points to be written")
	}

	version := time.Now().Round(time.Millisecond).UnixNano()
	records := []*timestreamwrite.Record{}
	var newest time.Time
	for _, p := range points {
		// get dimensions from tags
		dimensions := []*timestreamwrite.Dimension{}
		for k, v := range p.Tags {
			d := &timestreamwrite.Dimension{
				Name:  aws.String(k),
				Value: aws.String(v),
			}
			dimensions = append(dimensions, d)
		}

		// get records
		for k, v := range p.Values {
			r := &timestreamwrite.Record{
				Version:          &version,
				Dimensions:       dimensions,
				MeasureName:      aws.String(k),
				MeasureValue:     aws.String(fmt.Sprintf("%v", v)),
				MeasureValueType: aws.String("DOUBLE"),
				Time:             aws.String(strconv.FormatInt(p.Copy().Timestamp.Unix(), 10)),
				TimeUnit:         aws.String("SECONDS"),
			}
			records = append(records, r)
		}

		if p.Timestamp.After(newest) {
			newest = p.Timestamp
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

		if _, err := t.WriteClient.WriteRecords(chunk); err != nil {
			return err
		}
	}

	if err := t.LatestMarker(newest, "write"); err != nil {
		return fmt.Errorf("Error while writing marker: %s", err.Error())
	}
	return nil
}

func (t Timestream) LatestMarker(tm time.Time, note string) error {
	timeInSeconds := tm.Unix()
	writeRecordsInput := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(t.DB),
		TableName:    aws.String(t.Series),
		Records: []*timestreamwrite.Record{
			&timestreamwrite.Record{
				Dimensions: []*timestreamwrite.Dimension{
					&timestreamwrite.Dimension{
						Name:  aws.String("ruminant"),
						Value: aws.String(t.Indicator),
					},
				},
				MeasureName:      aws.String(LatestIndicator),
				MeasureValue:     aws.String(fmt.Sprintf("%s: %s", t.Indicator, note)),
				MeasureValueType: aws.String("VARCHAR"),
				Time:             aws.String(strconv.FormatInt(timeInSeconds, 10)),
				TimeUnit:         aws.String("SECONDS"),
			},
		},
	}

	_, err := t.WriteClient.WriteRecords(writeRecordsInput)
	return err
}
