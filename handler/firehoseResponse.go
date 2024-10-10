package handler

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"go.uber.org/zap"
	"time"
)

type firehoseResponse struct {
	RequestId    string `json:"requestId"`
	Timestamp    int64  `json:"timestamp"`
	ErrorMessage string `json:"errorMessage"`
}

type responseClient struct {
	requestId string
	logger    *zap.Logger
}

func newResponseClient(requestId string, logger *zap.Logger) *responseClient {
	return &responseClient{
		requestId: requestId,
		logger:    logger,
	}
}

func (rc *responseClient) generateValidFirehoseResponse(statusCode int, errorMessage string, err error) events.APIGatewayProxyResponse {
	if errorMessage != "" {
		rc.logger.Warn(errorMessage, zap.Error(err))
		data := firehoseResponse{
			RequestId:    rc.requestId,
			Timestamp:    time.Now().Unix(),
			ErrorMessage: fmt.Sprintf("%s %s", errorMessage, err.Error()),
		}
		jsonData, _ := json.Marshal(data)
		return events.APIGatewayProxyResponse{
			Body:       string(jsonData),
			StatusCode: statusCode,
			Headers: map[string]string{
				"content-type": "application/json",
			},
			IsBase64Encoded:   false,
			MultiValueHeaders: map[string][]string{},
		}
	} else {
		rc.logger.Info("Request processed successfully")
		data := firehoseResponse{
			RequestId:    rc.requestId,
			Timestamp:    time.Now().Unix(),
			ErrorMessage: "",
		}
		jsonData, _ := json.Marshal(data)
		return events.APIGatewayProxyResponse{
			Body:       string(jsonData),
			StatusCode: statusCode,
			Headers: map[string]string{
				"content-type": "application/json",
			},
			IsBase64Encoded:   false,
			MultiValueHeaders: map[string][]string{},
		}
	}
}
