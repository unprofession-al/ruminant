// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"os"
)

var (
	// These variables are passed during `go build` via ldflags, for example:
	//   go build -ldflags "-X main.commit=$(git rev-list -1 HEAD)"
	// goreleaser (https://goreleaser.com/) does this by default.
	version string
	commit  string
	date    string
)

func main() {
	err := NewApp().Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
		os.Exit(-1)
	}
}

// verisonInfo returns a string containing information usually passed via
// ldflags during build time.
func versionInfo() string {
	if version == "" {
		version = "dirty"
	}
	if commit == "" {
		commit = "dirty"
	}
	if date == "" {
		date = "unknown"
	}
	return fmt.Sprintf("Version:    %s\nCommit:     %s\nBuild Date: %s\n", version, commit, date)
}
