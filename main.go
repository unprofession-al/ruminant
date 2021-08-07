package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	_, isLambda := os.LookupEnv("_LAMBDA_SERVER_PORT")
	if isLambda {
		lambda.Start(launchAsLambda)
	} else {
		err := launch()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
			os.Exit(-1)
		}
	}
}

type LambdaEvent struct {
	Command string            `json:"command"`
	Args    map[string]string `json:"args"`
}

func launchAsLambda(ctx context.Context, e LambdaEvent) (string, error) {
	// read args from event
	args := append([]string{os.Args[0]}, e.Command)
	for k, v := range e.Args {
		args = append(args, k)
		args = append(args, v)
	}
	os.Args = args
	fmt.Println(strings.Join(os.Args, " "))

	// run
	err := launch()
	return "done", err
}

func launch() error {
	return NewApp().Execute()
}
