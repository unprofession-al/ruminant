package main

import (
	"bytes"
	"fmt"
	"reflect"
	"text/tabwriter"
	"time"
)

type Point struct {
	Timestamp time.Time
	Tags      map[string]string
	Values    map[string]interface{}
}

func Avg(points []Point, samples int) []Point {
	var measurements []Point
	for _, point := range points {
		found := false
		for _, measurement := range measurements {
			if measurement.Timestamp == point.Timestamp && reflect.DeepEqual(measurement.Tags, point.Tags) {
				for key, value := range point.Values {
					add, _ := value.(float64)
					pre, _ := measurement.Values[key].(float64)
					measurement.Values[key] = pre + add
				}
				found = true
				break
			}
		}
		if !found {
			measurements = append(measurements, point.Copy())
		}
	}

	for index, measurement := range measurements {
		for key, value := range measurement.Values {
			pre, _ := value.(float64)
			measurements[index].Values[key] = pre / float64(samples)
		}
	}

	return measurements
}

func (p Point) String() string {
	out := new(bytes.Buffer)
	timestamp := p.Timestamp.Format("2006-01-02 15:04:05")

	const padding = 1

	var valuesStr []string
	for key, val := range p.Values {
		valuesStr = append(valuesStr, fmt.Sprintf("%s: %v", key, val))
	}

	var tagsStr []string
	for key, val := range p.Tags {
		tagsStr = append(tagsStr, fmt.Sprintf("%s: %s", key, val))
	}

	var iterations int

	if len(tagsStr) > len(valuesStr) {
		iterations = len(tagsStr)
	} else {
		iterations = len(valuesStr)
	}

	w := tabwriter.NewWriter(out, 0, 0, padding, ' ', tabwriter.Debug)
	fmt.Fprintf(w, "@%s\t Tags\t Values\n", timestamp)
	for i := 0; i < iterations; i++ {
		tag := ""
		if len(tagsStr) > i {
			tag = tagsStr[i]
		}
		value := ""
		if len(valuesStr) > i {
			value = valuesStr[i]
		}
		fmt.Fprintf(w, "\t %s\t %s\n", tag, value)
	}
	w.Flush()

	return string(out.String())
}

func (p Point) Copy() Point {
	tags := make(map[string]string)
	for k, v := range p.Tags {
		tags[k] = v
	}

	values := make(map[string]interface{})
	for k, v := range p.Values {
		values[k] = v
	}

	c := Point{
		Timestamp: p.Timestamp,
		Tags:      tags,
		Values:    values,
	}
	return c
}
