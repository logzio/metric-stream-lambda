package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	jsonFile, err := os.Open("testEvent.json")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()
	ctx := context.Background()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	request := events.APIGatewayProxyRequest{}
	json.Unmarshal(byteValue, &request)
	result, _ := handleRequest(ctx, request)
	log.Println(result)
}
