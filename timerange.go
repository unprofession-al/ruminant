package main

import (
	"time"

	"github.com/tj/go-naturaldate"
)

func parseTimeString(in string) (time.Time, error) {
	out, err := time.Parse(time.RFC3339, in)
	if err == nil {
		return out, err
	}

	return naturaldate.Parse(in, time.Now())
}
