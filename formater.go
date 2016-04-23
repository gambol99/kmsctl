/*
Copyright 2015 All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"encoding/json"
	"fmt"
	"io"

	"gopkg.in/yaml.v2"
)

type formater struct {
	// the format we should be using
	format string
	// the writer
	writer io.Writer
}

func newFormater(format string, writer io.Writer) (*formater, error) {
	switch format {
	case "yml":
		fallthrough
	case "yaml":
	case "json":
	case "text":
	default:
		return nil, fmt.Errorf("unsupport output format")
	}

	return &formater{
		format: format,
		writer: writer,
	}, nil
}

func (r *formater) fields(v map[string]interface{}) *formater {
	switch r.format {
	case "yml":
		fallthrough
	case "yaml":
		encode, err := yaml.Marshal(v)
		if err != nil {
			return r
		}
		fmt.Fprintf(r.writer, string(encode))
	case "json":
		encode, err := json.Marshal(v)
		if err != nil {
			return r
		}
		fmt.Fprintf(r.writer, string(encode))
	default:
	}

	return r
}

// add a message to the last log entry
func (r *formater) log(message string, args ...interface{}) *formater {
	if r.format == "text" {
		fmt.Fprintf(r.writer, message, args...)
	}

	return r
}
