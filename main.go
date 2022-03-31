package main

import (
	"context"
	_ "context"
	base64 "encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	pdata "go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/prometheusremotewriteexporter"
	_ "go.opentelemetry.io/otel/metric"
	pb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/golang/protobuf/proto"
	"log"
)

// Takes a base64 encoded string and returns decoded string
func base64Decode(str string) (string, bool) {
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", true
	}
	return string(data), false
}
func handleRequest(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	log.Println("Getting secretes from header")
	metricCount := 0
	var ca map[string]interface{}
	json.Unmarshal([]byte(request.Headers["x-amz-firehose-common-attributes"]), &ca)
	if ca == nil {
		err := json.Unmarshal([]byte(request.Headers["X-Amz-Firehose-Common-Attributes"]), &ca)
		if err != nil {
			log.Printf("Error while getting secretes from header: %s", err)
			return events.APIGatewayProxyResponse{Body: "Error while sgetting secretes from header", StatusCode: 500}, err
		}
	}
	secretsMap := ca["commonAttributes"].(map[string]interface{})
	//converting the secrets to string
	LOGZIO_TOKEN := secretsMap["LOGZIO_TOKEN"].(string)
	LISTENER_URL := secretsMap["LISTENER_URL"].(string)
	// Initializing prometheus remote write exporter
	cfg := &prometheusremotewriteexporter.Config{
		TimeoutSettings:    exporterhelper.TimeoutSettings{},
		RetrySettings:      exporterhelper.RetrySettings{},
		Namespace:          "",
		ExternalLabels:     map[string]string{},
		HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: ""},
	}
	buildInfo := component.BuildInfo{
		Description: "OpenTelemetry",
		Version:     "1.0",
	}
	cfg.HTTPClientSettings.Endpoint = LISTENER_URL
	cfg.HTTPClientSettings.Headers = map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", LOGZIO_TOKEN),
	}
	cfg.Namespace = ""
	cfg.ExternalLabels = map[string]string{"p8s_logzio_name": "cloudwatch-stream-metrics"}
	cfg.RemoteWriteQueue.NumConsumers = 1
	log.Println("Starting metrics exporter")
	metricsExporter, err := prometheusremotewriteexporter.NewPRWExporter(cfg, buildInfo)
	if err != nil {
		log.Println(fmt.Sprintf("Error while starting metrics exporter: %s", err))
		return events.APIGatewayProxyResponse{Body: "Error while starting metrics exporter", StatusCode: 500}, err
	}
	err = metricsExporter.Start(ctx, componenttest.NewNopHost())
	if err != nil {
		return events.APIGatewayProxyResponse{Body: "Error while starting metrics exporter", StatusCode: 500}, err
	}
	log.Println("Starting to parse request body")
	var body map[string]interface{}
	err = json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		log.Printf("Error while unmarshalling request body: %s", err)
		return events.APIGatewayProxyResponse{Body: "Error while unmarshalling request body", StatusCode: 500}, err
	}
	records := body["records"].([]interface{})
	for _, record := range records {
		//Converting the data to string
		data := record.(map[string]interface{})["data"].(string)
		//Decoding data and convert to otlp proto message
		rawDecodedText, _ := base64Decode(data)
		myBuffer := proto.NewBuffer([]byte(rawDecodedText))
		ExportMetricsServiceRequest := &pb.ExportMetricsServiceRequest{}
		err := myBuffer.DecodeMessage(ExportMetricsServiceRequest)
		if err != nil {
			return events.APIGatewayProxyResponse{}, err
		}
		// Converting otlp proto message to proto bytes
		protoBytes, err := proto.Marshal(ExportMetricsServiceRequest)
		if err != nil {
			log.Printf("Error while converting otlp proto message to proto bytes: %s", err)
			return events.APIGatewayProxyResponse{Body: "Error while converting otlp proto message to proto bytes", StatusCode: 500}, err
		}
		// Converting otlp proto bytes to pdata.metrics
		metrics, err := pdata.MetricsFromOtlpProtoBytes(protoBytes)
		metricCount += metrics.MetricCount()
		// Lopping threw all data points and enhance labels with logz.io naming conventions and resource attributes
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			resource := metrics.ResourceMetrics().At(i)
			resourceAttributes := resource.Resource().Attributes()
			for j := 0; j < resource.InstrumentationLibraryMetrics().Len(); j++ {
				metricsList := resource.InstrumentationLibraryMetrics().At(j)
				for n := 0; n < metricsList.Metrics().Len(); n++ {
					metric := metricsList.Metrics().At(n)
					// Converts to new name (example: amazonaws.com/AWS/AppRunner/ActiveInstances -> aws_apprunner_activeinstances)
					newName := strings.ReplaceAll(strings.ToLower(strings.ReplaceAll(metric.Name(), "/", "_")), "amazonaws.com_", "")
					metric.SetName(newName)
					// Assuming Summary metric type according to AWS docs:
					// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/acw-ug.pdf#CloudWatch-metric-streams-formats-opentelemetry
					// Page 262
					var dps = metric.Summary().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						// For each data point, lowercase all attributes and add resource attributes
						datapoint := dps.At(d)
						datapoint.LabelsMap().Range(func(k string, v string) bool {
							datapoint.LabelsMap().Insert(strings.ToLower(k), strings.ToLower(v))
							datapoint.LabelsMap().Delete(k)
							return true
						})
						accountId, _ := resourceAttributes.Get("cloud.account.id")
						region, _ := resourceAttributes.Get("cloud.region")
						datapoint.LabelsMap().Insert("account", accountId.StringVal())
						datapoint.LabelsMap().Insert("region", region.StringVal())
					}
				}
			}
		}
		log.Println(fmt.Sprintf("Found %d new metircs", metrics.MetricCount()))
		if err != nil {
			log.Printf("Error: %s", err)
			return events.APIGatewayProxyResponse{Body: "Error", StatusCode: 500}, err
		}
		log.Println("Sending metrics")
		err = metricsExporter.PushMetrics(ctx, metrics)
		if err != nil {
			log.Printf("Error while pushing metrics: %s", err)
			return events.APIGatewayProxyResponse{Body: "Error while pushing metrics", StatusCode: 500}, err
		}
	}
	log.Printf("Found total of %d metics", metricCount)
	log.Println("Shutting down metrics exporter")
	err = metricsExporter.Shutdown(ctx)
	if err != nil {
		log.Printf("Error while shutting down exporter: %s", err)
		return events.APIGatewayProxyResponse{Body: "Error while shutting down exporter", StatusCode: 500}, err
	}
	return events.APIGatewayProxyResponse{Body: "Done", StatusCode: 200}, nil
}

func main() {
	lambda.Start(handleRequest)
}
