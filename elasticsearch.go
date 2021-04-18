package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

type EsResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
		Failures   []struct {
			Shard  int    `json:"shard"`
			Index  string `json:"index"`
			Node   string `json:"node"`
			Reason struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"reason"`
		} `json:"failures"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore interface{}   `json:"max_score"`
		Hits     []interface{} `json:"hits"`
	} `json:"hits"`
	Aggregations struct {
		OverTime struct {
			Buckets []struct {
				KeyAsString time.Time `json:"key_as_string"`
				Key         int64     `json:"key"`
				DocCount    int       `json:"doc_count"`
				ByDomain    struct {
					DocCountErrorUpperBound int           `json:"doc_count_error_upper_bound"`
					SumOtherDocCount        int           `json:"sum_other_doc_count"`
					Buckets                 []interface{} `json:"buckets"`
				} `json:"by_domain"`
			} `json:"buckets"`
		} `json:"over_time"`
	} `json:"aggregations"`
}

func NewEsResponse(in io.Reader) (EsResponse, error) {
	var response EsResponse
	body, err := ioutil.ReadAll(in)
	if err != nil {
		return response, err
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return response, fmt.Errorf("elasticsearch error '%s' occurred on line", err.Error())
	}
	return response, nil
}

func (esr EsResponse) AggsAsJSON() ([]byte, error) {
	return json.Marshal(esr.Aggregations)
}

type ElasticSearch struct {
	Proto string
	Host  string
	User  string
	Pass  string
	Port  int
}

func NewElasticSearch(proto, host, user, pass string, port int) ElasticSearch {
	return ElasticSearch{
		Proto: proto,
		Host:  host,
		User:  user,
		Pass:  pass,
		Port:  port,
	}
}

func (es ElasticSearch) Query(index, kind, jsonQuery string) (EsResponse, error) {
	var esr EsResponse
	url := fmt.Sprintf("%s://%s:%d/%s/%s/_search?pretty", es.Proto, es.Host, es.Port, index, kind)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsonQuery)))
	if err != nil {
		return esr, err
	}

	if es.User != "" && es.Pass != "" {
		req.SetBasicAuth(es.User, es.Pass)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return esr, err
	}
	defer resp.Body.Close()

	esr, err = NewEsResponse(resp.Body)
	if err != nil {
		return esr, err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return esr, fmt.Errorf("error while executing query, status code %d, output %s", resp.StatusCode, err)
	}

	if esr.Shards.Failed > 0 {
		return esr, fmt.Errorf("%d of %d shards failed while executing query", esr.Shards.Failed, esr.Shards.Total)
	}

	return esr, nil
}

func ToEsTimestamp(t time.Time) int64 {
	i := t.Unix() * 1000
	return i
}
