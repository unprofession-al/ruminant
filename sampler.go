package main

import (
	"bytes"
	"html/template"
	"time"

	"gopkg.in/robfig/cron.v2"
)

type Sampler struct {
	interval      cron.Schedule
	offset        time.Duration
	sampleOffsets []time.Duration
}

func NewSampler(c SamplerConfig) (Sampler, error) {
	s := Sampler{
		offset: c.Offset,
	}

	interv, err := cron.Parse(c.Interval)
	if err != nil {
		return s, err
	}
	s.interval = interv

	var sampleOffsets []time.Duration
	sCount := 0
	diffSampleOffset := c.SampleOffset
	if c.Samples%2 != 0 {
		offset := time.Duration(0)
		sampleOffsets = append(sampleOffsets, offset)
		sCount = (c.Samples - 1) / 2
	} else {
		sCount = (c.Samples) / 2
		diffSampleOffset = c.SampleOffset / 2
	}
	iterSampleOffset := time.Duration(0)
	for i := 0; i < sCount; i++ {
		iterSampleOffset += diffSampleOffset
		if i == 0 {
			diffSampleOffset = c.SampleOffset
		}
		sampleOffsets = append(sampleOffsets, iterSampleOffset)
		sampleOffsets = append(sampleOffsets, -iterSampleOffset)
	}
	s.sampleOffsets = sampleOffsets
	return s, nil
}

func (s Sampler) Iterate(from time.Time) []time.Time {
	var out []time.Time

	maxTime := time.Now().Add(-s.offset)
	currentTime := s.interval.Next(from)

	for keepgoing := currentTime.Before(maxTime); keepgoing; keepgoing = (currentTime.Before(maxTime)) {
		out = append(out, currentTime)
		currentTime = s.interval.Next(currentTime)
	}
	return out
}

func (s Sampler) BuildQueries(templ string, start time.Time) map[time.Time][]string {
	t := template.Must(template.New("query").Parse(templ))
	out := make(map[time.Time][]string)
	for _, at := range s.Iterate(start) {
		var queries []string
		for _, offset := range s.sampleOffsets {
			var query bytes.Buffer
			t.Execute(&query, ToEsTimestamp(at.Add(offset)))
			queries = append(queries, query.String())
		}
		out[at] = queries
	}
	return out
}
