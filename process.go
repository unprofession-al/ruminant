package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	jee "github.com/nytlabs/gojee"
)

func Burp(j []byte, i Iterator, inherited Point) ([]Point, string, error) {
	var points []Point
	if i.Selector == "" {
		return points, string(j), fmt.Errorf("no selector definded")
	}
	points, _, jsonFragment, err := process(j, i, inherited, true)
	return points, jsonFragment, err
}

func Chew(j []byte, i Iterator, inherited Point) ([]Point, error) {
	var points []Point
	if i.Selector == "" {
		return points, fmt.Errorf("no selector definded")
	}
	points, _, _, err := process(j, i, inherited, false)
	return points, err
}

func process(j []byte, i Iterator, inherited Point, test bool) ([]Point, bool, string, error) {
	var results []Point

	selected, err := queryBytes(j, i.Selector)
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
			out, err := query(elem, i.Time)
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
			out, err := query(elem, selector)
			if err != nil {
				return results, false, "", err
			}
			point.Values[key] = out
		}

		for key, value := range i.FixedValues {
			point.Values[key] = value
		}

		for key, selector := range i.Tags {
			out, err := queryBytes(elem, selector)
			if err != nil {
				return results, false, "", err
			}
			trimmed := strings.Trim(string(out), "\"\"")
			trimmed = strings.Trim(trimmed, "\\\"")
			trimmed = strings.Trim(trimmed, "\"")
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

func queryBytes(j []byte, q string) ([]byte, error) {
	result, err := query(j, q)
	if err != nil {
		return []byte{}, err
	}
	return json.Marshal(result)
}

func query(j []byte, q string) (interface{}, error) {
	var umsg jee.BMsg
	l, err := jee.Lexer(q)
	if err != nil {
		return nil, err
	}

	tree, err := jee.Parser(l)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(j, &umsg)
	if err != nil {
		return nil, err
	}

	result, err := jee.Eval(tree, umsg)
	if err != nil {
		return nil, err
	}
	return result, nil
}
