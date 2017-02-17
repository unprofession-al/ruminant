package cmd

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/nytlabs/gojee"
)

func Process(j []byte, i Iterator, inherited Point) ([]Point, error) {
	var results []Point

	selected, err := queryBytes(j, i.Selector)
	if err != nil {
		return results, err
	}

	var elements []interface{}

	err = json.Unmarshal(selected, &elements)
	if err != nil {
		return results, err
	}

	for _, element := range elements {
		point := inherited.Copy()
		elem, err := json.Marshal(element)
		if err != nil {
			return results, err
		}

		if i.Time != "" {
			out, err := query(elem, i.Time)
			if err != nil {
				return results, err
			}
			if f, ok := out.(float64); ok {
				point.Timestamp = time.Unix(int64(f)/1000, 0)
			} else {
				return results, errors.New("Time could not be read")
			}
		}

		for key, selector := range i.Values {
			out, err := query(elem, selector)
			if err != nil {
				return results, err
			}
			point.Values[key] = out
		}

		for key, selector := range i.Tags {
			out, err := queryBytes(elem, selector)
			if err != nil {
				return results, err
			}
			trimmed := strings.Trim(string(out), "\"")
			point.Tags[key] = trimmed
		}

		if len(i.Iterators) > 0 {
			for _, iterator := range i.Iterators {
				processed, err := Process(elem, iterator, point)
				if err != nil {
					return results, err
				}
				results = append(results, processed...)
			}
		} else {
			results = append(results, point)
		}
	}

	return results, nil
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
