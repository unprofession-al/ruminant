package cmd

import (
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
