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
	Args string `json:"args"`
}

func launchAsLambda(ctx context.Context, e LambdaEvent) (string, error) {
	// read args from event
	args := append([]string{os.Args[0]}, strings.Split(e.Args, " ")...)
	os.Args = args

	// run
	err := launch()
	return "done", err
}

func launch() error {
	return NewApp().Execute()
}
