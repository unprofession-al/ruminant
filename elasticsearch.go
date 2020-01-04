package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

type EsResponse struct {
	Took     float64 `json:"took"`
	TimedOut bool    `json:"timed_out"`
	Shards   struct {
		Total   float64 `json:"total"`
		Success float64 `json:"success"`
		Failed  float64 `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total    float64     `json:"total"`
		MaxScore float64     `json:"max_score"`
		Hits     interface{} `json:"hits"`
	} `json:"hits"`
	Aggregations interface{} `json:"aggregations"`
	Error        string      `json:"error"`
}

type EsError struct {
	Error struct {
		RootCause []struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
			Line   int    `json:"line"`
			Col    int    `json:"col"`
		} `json:"root_cause"`
		Type   string `json:"type"`
		Reason string `json:"reason"`
		Line   int    `json:"line"`
		Col    int    `json:"col"`
	} `json:"error"`
	Status int `json:"status"`
}

func NewEsResponse(in io.Reader) (EsResponse, error) {
	var response EsResponse
	body, err := ioutil.ReadAll(in)
	if err != nil {
		return response, err
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		var eserror EsError
		nastyerr := json.Unmarshal(body, &eserror)
		if nastyerr != nil {
			return response, errors.New(fmt.Sprintf("Could not unmarshal response: %s. Error was %s", string(body), err.Error()))
		}
		return response, errors.New(fmt.Sprintf("ElasticSeach %s occured on line %d: %s", eserror.Error.Type, eserror.Error.Line, eserror.Error.Reason))
	}
	return response, nil
}

func (esr EsResponse) AggsAsJson() ([]byte, error) {
	return json.Marshal(esr.Aggregations)
}

type ElasticSearch struct {
	Proto string
	Host  string
	Port  int
}

func NewElasticSearch(proto, host string, port int) ElasticSearch {
	return ElasticSearch{
		Proto: proto,
		Host:  host,
		Port:  port,
	}
}

func (es ElasticSearch) Query(index, kind, jsonQuery string) (EsResponse, error) {
	var esr EsResponse
	url := fmt.Sprintf("%s://%s:%d/%s/%s/_search?pretty", es.Proto, es.Host, es.Port, index, kind)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsonQuery)))
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
		return esr, errors.New(fmt.Sprintf("Error while executing query, status code %d, output %s", resp.StatusCode, esr.Error))
	}

	if esr.Shards.Failed > 0 {
		return esr, errors.New(fmt.Sprintf("%f of %f shards failed while executing query", esr.Shards.Failed, esr.Shards.Total))
	}

	return esr, nil
}

func ToEsTimestamp(t time.Time) int64 {
	i := t.Unix() * 1000
	return i
}
