package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mgutz/ansi"
	"github.com/nytlabs/gojee"
)

func Process(j []byte, i Iterator, inherited map[string]string, depht int) ([]map[string]string, error) {
	var results []map[string]string
	indent := strings.Repeat("   ", depht)
	indent = indent + "|"

	if debug {
		var prettyJSON bytes.Buffer
		err := json.Indent(&prettyJSON, j, "", "   ")
		if err != nil {
			return results, err
		}

		scanner := bufio.NewScanner(strings.NewReader(prettyJSON.String()))
		for scanner.Scan() {
			fmt.Println(indent, ansi.Color(scanner.Text(), "red"))
		}
	}

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
		store := make(map[string]string)
		for k, v := range inherited {
			store[k] = v
		}
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

		if debug {
			fmt.Println(indent, ansi.Color(fmt.Sprintf("%d", store), "green"))
		}

		if len(i.Iterators) > 0 {
			for _, iterator := range i.Iterators {
				processed, err := Process(elem, iterator, store, depht+1)
				if err != nil {
					return results, err
				}
				results = append(results, processed...)
			}
		} else {
			results = append(results, store)
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
