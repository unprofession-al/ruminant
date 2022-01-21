package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	Aggregations interface{} `json:"aggregations"`
}

func NewEsResponse(body []byte) (EsResponse, error) {
	var response EsResponse
	err := json.Unmarshal(body, &response)
	if err != nil {
		return response, fmt.Errorf("elasticsearch error '%s' occurred on line", err.Error())
	}
	//fmt.Println(response)
	return response, nil
}

func (esr EsResponse) AggsAsJSON() ([]byte, error) {
	return json.Marshal(esr.Aggregations)
}

type ElasticSearch struct {
	BaseURL   string
	User      string
	Pass      string
	QueryArgs map[string]string
}

func NewElasticSearch(baseURL, user, pass string, queryArgs map[string]string) ElasticSearch {
	return ElasticSearch{
		BaseURL:   baseURL,
		User:      user,
		Pass:      pass,
		QueryArgs: queryArgs,
	}
}

func (es ElasticSearch) Query(index, kind, jsonQuery string) (EsResponse, error) {
	var esr EsResponse
	queryArgs := "?pretty"
	for k, v := range es.QueryArgs {
		if v == "" {
			queryArgs = fmt.Sprintf("%s&%s", queryArgs, k)
		} else {
			queryArgs = fmt.Sprintf("%s&%s=%s", queryArgs, k, v)
		}
	}
	url := fmt.Sprintf("%s/%s/%s/_search%s", es.BaseURL, index, kind, queryArgs)
	//fmt.Println(url)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsonQuery)))
	if err != nil {
		return esr, err
	}
	//fmt.Println(jsonQuery)

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

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return esr, err
	}

	//fmt.Println(string(body))
	esr, err = NewEsResponse(body)
	if err != nil {
		return esr, err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return esr, fmt.Errorf("error while executing query, status code %d, output %s", resp.StatusCode, string(body))
	}

	if esr.Shards.Failed > 0 {
		errors := ""
		for _, failed := range esr.Shards.Failures {
			errors += " >>> " + failed.Reason.Reason
		}
		return esr, fmt.Errorf("%d of %d shards failed while executing query, errors are: %s", esr.Shards.Failed, esr.Shards.Total, errors)
	}

	return esr, nil
}

func ToEsTimestamp(t time.Time) int64 {
	i := t.Unix() * 1000
	return i
}
