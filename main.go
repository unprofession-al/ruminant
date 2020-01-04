package main

import (
	"fmt"
	"os"
)

func main() {
	err := NewApp().Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
		os.Exit(-1)
	}
}
