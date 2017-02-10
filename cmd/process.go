package cmd

import (
	"encoding/json"

	"github.com/nytlabs/gojee"
)

func Process(j []byte, i Iterator, store map[string]string) ([]map[string]string, error) {
	var results []map[string]string
	results = append(results, store)

	selected, err := query(j, i.Selector)
	if err != nil {
		return results, err
	}

	var elements []interface{}

	err = json.Unmarshal(selected, &elements)
	if err != nil {
		return results, err
	}

	for _, element := range elements {
		elem, err := json.Marshal(element)
		if err != nil {
			return results, err
		}

		for key, selector := range i.Probes {
			out, err := query(elem, selector)
			if err != nil {
				return results, err
			}
			store[key] = string(out)
		}

		for _, iterator := range i.Iterator {
			processed, err := Process(elem, iterator, store)
			if err != nil {
				return results, err
			}
			results = append(results, processed...)
		}
	}
	return results, nil
}

func query(j []byte, query string) ([]byte, error) {
	var umsg jee.BMsg
	l, err := jee.Lexer(query)
	if err != nil {
		return []byte{}, err
	}

	tree, err := jee.Parser(l)
	if err != nil {
		return []byte{}, err
	}

	err = json.Unmarshal(j, &umsg)
	if err != nil {
		return []byte{}, err
	}

	result, err := jee.Eval(tree, umsg)
	if err != nil {
		return []byte{}, err
	}

	return json.Marshal(result)
}
