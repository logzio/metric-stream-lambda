package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/logzio/metric-stream-lambda/handler"
)

func main() {
	lambda.Start(handler.HandleRequest)
}
