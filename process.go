package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"ruminant/sink"
	"strings"
	"time"

	"github.com/itchyny/gojq"
)

func Burp(j []byte, i Iterator, inherited sink.Point) ([]sink.Point, string, error) {
	var points []sink.Point
	if i.Selector == "" {
		return points, string(j), fmt.Errorf("no selector definded")
	}
	points, _, jsonFragment, err := process(j, i, inherited, true)
	return points, jsonFragment, err
}

func Chew(j []byte, i Iterator, inherited sink.Point, noTrim bool) ([]sink.Point, error) {
	var points []sink.Point
	if i.Selector == "" {
		return points, fmt.Errorf("no selector definded")
	}
	points, _, _, err := process(j, i, inherited, false)
	if !noTrim {
		points, _, _ = sink.TrimPoints(points)
	}
	return points, err
}

func process(j []byte, i Iterator, inherited sink.Point, test bool) ([]sink.Point, bool, string, error) {
	var results []sink.Point
	// fmt.Printf("\n\n---\n\n%s\n\n---\n\n", j)

	selected, err := queryArrayBytes(j, i.Selector)
	if err != nil {
		return results, false, "", err
	}

	var elements []interface{}

	err = json.Unmarshal(selected, &elements)
	if err != nil {
		return results, false, "", err
	}

	for _, element := range elements {
		point := inherited.Copy()
		elem, err := json.MarshalIndent(element, "", "  ")

		if err != nil {
			return results, false, "", err
		}

		if i.Time != "" {
			out, err := queryObject(elem, i.Time)
			if err != nil {
				return results, false, "", err
			}
			if f, ok := out.(float64); ok {
				point.Timestamp = time.Unix(int64(f)/1000, 0)
			} else {
				return results, false, " ", fmt.Errorf("time could not be read")
			}
		}

		for key, selector := range i.Values {
			out, err := queryObject(elem, selector)
			if err != nil {
				return results, false, "", err
			}
			point.Values[key] = out
		}

		for key, value := range i.FixedValues {
			point.Values[key] = value
		}

		for key, selector := range i.Tags {
			out, err := queryObjectBytes(elem, selector)
			if err != nil {
				return results, false, "", err
			}
			trimmed := strings.Trim(string(out), "\"\\")
			point.Tags[key] = trimmed
		}

		for key, value := range i.FixedTags {
			point.Tags[key] = value
		}

		if len(i.Iterators) > 0 {
			for _, iterator := range i.Iterators {
				processed, stop, jsonFragment, err := process(elem, iterator, point, test)
				if err != nil {
					return results, false, "", err
				}
				results = append(results, processed...)
				if stop {
					return results, stop, jsonFragment, nil
				}
			}
		} else {
			results = append(results, point)
			if test {
				return results, true, string(elem), nil
			}
		}
	}

	return results, false, "", nil
}

func queryArrayBytes(j []byte, q string) ([]byte, error) {
	//fmt.Println("queryArrayBytes")
	result, err := queryArray(j, q)
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(result)
}

func queryArray(j []byte, q string) ([]interface{}, error) {
	j = bytes.ReplaceAll(j, []byte("buckets\":null"), []byte("buckets\":[]"))
	//fmt.Println("queryArray")
	//fmt.Println(string(j))
	//fmt.Println(q)

	var input map[string]interface{}
	err := json.Unmarshal(j, &input)
	if err != nil {
		return nil, err
	}

	query, err := gojq.Parse(q)
	if err != nil {
		log.Fatalln(err)
	}

	var out []interface{}
	iter := query.Run(input) // or query.RunWithContext
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}

func queryObjectBytes(j []byte, q string) ([]byte, error) {
	//fmt.Println("queryObjectBytes")
	result, err := queryObject(j, q)
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(result)
}
func queryObject(j []byte, q string) (interface{}, error) {
	j = bytes.ReplaceAll(j, []byte("buckets\":null"), []byte("buckets\":[]"))
	//fmt.Println("queryObject")
	//fmt.Println(string(j))
	//fmt.Println(q)

	var input map[string]interface{}
	err := json.Unmarshal(j, &input)
	if err != nil {
		return nil, err
	}

	query, err := gojq.Parse(q)
	if err != nil {
		log.Fatalln(err)
	}

	var out interface{}
	iter := query.Run(input) // or query.RunWithContext
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return out, err
		}
		out = v
	}
	return out, nil
}
