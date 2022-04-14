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

const (
	sumStr   = "_sum"
	countStr = "_count"
	minStr   = "_min"
	maxStr   = "_max"
)

// Takes a base64 encoded string and returns decoded string
func base64Decode(str string) (string, bool) {
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", true
	}
	return string(data), false
}

// Takes origin metric and suffix based on desired aggregation. Creates new DoubleSum metric with origin name plus suffix, origin unit and origin description
func createMetricFromAttributes(metric pdata.Metric, suffix string) pdata.Metric {
	destMetric := pdata.NewMetric()
	destMetric.SetName(metric.Name() + suffix)
	destMetric.SetDataType(pdata.MetricDataTypeDoubleSum)
	destMetric.SetUnit(metric.Unit())
	destMetric.SetDescription(metric.Description())
	destMetric.DoubleSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
	return destMetric
}

// Takes origin datapoint, destination datapoint and resource attributes. Coping labels from origin and adding resource attributes as labels
func addLabelsAndResourceAttributes(dp pdata.SummaryDataPoint, destDp pdata.DoubleDataPoint, resourceAttributes pdata.AttributeMap) {
	dp.LabelsMap().Range(func(k string, v string) bool {
		destDp.LabelsMap().Insert(strings.ToLower(k), strings.ToLower(v))
		return true
	})
	accountId, _ := resourceAttributes.Get("cloud.account.id")
	region, _ := resourceAttributes.Get("cloud.region")
	destDp.LabelsMap().Insert("account", accountId.StringVal())
	destDp.LabelsMap().Insert("region", region.StringVal())
}

// Takes Summary metric and generates new metrics (sum, count, min, max) than add them to metricsToSend
func summaryValuesToMetrics(metricsToSendSlice pdata.InstrumentationLibraryMetricsSlice, metric pdata.Metric, resourceAttributes pdata.AttributeMap) {
	// Converts to new name (example: amazonaws.com/AWS/AppRunner/ActiveInstances -> aws_apprunner_activeinstances)
	newName := strings.ReplaceAll(strings.ToLower(strings.ReplaceAll(metric.Name(), "/", "_")), "amazonaws.com_", "")
	metric.SetName(newName)
	// Assuming Summary metric type according to AWS docs:
	// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/acw-ug.pdf#CloudWatch-metric-streams-formats-opentelemetry
	// Page 262
	var dps = metric.Summary().DataPoints()
	// Generate new metrics (sum, count, min, max), and for each summary data point, lowercase all attributes and add resource attributes
	sumMetric := createMetricFromAttributes(metric, sumStr)
	countMetric := createMetricFromAttributes(metric, countStr)
	maxMetric := createMetricFromAttributes(metric, maxStr)
	minMetric := createMetricFromAttributes(metric, minStr)
	for d := 0; d < dps.Len(); d++ {
		datapoint := dps.At(d)
		// Sum datapoint
		sumDp := sumMetric.DoubleSum().DataPoints().AppendEmpty()
		sumDp.SetValue(datapoint.Sum())
		sumDp.SetTimestamp(datapoint.Timestamp())
		addLabelsAndResourceAttributes(datapoint, sumDp, resourceAttributes)
		// Count datapoint
		countDp := countMetric.DoubleSum().DataPoints().AppendEmpty()
		countDp.SetValue(float64(datapoint.Count()))
		countDp.SetTimestamp(datapoint.Timestamp())
		addLabelsAndResourceAttributes(datapoint, countDp, resourceAttributes)
		// Max datapoint
		minDp := minMetric.DoubleSum().DataPoints().AppendEmpty()
		minDp.SetValue(datapoint.QuantileValues().At(0).Value())
		minDp.SetTimestamp(datapoint.Timestamp())
		addLabelsAndResourceAttributes(datapoint, minDp, resourceAttributes)
		// Min datapoint
		maxDp := maxMetric.DoubleSum().DataPoints().AppendEmpty()
		maxDp.SetValue(datapoint.QuantileValues().At(1).Value())
		maxDp.SetTimestamp(datapoint.Timestamp())
		addLabelsAndResourceAttributes(datapoint, maxDp, resourceAttributes)
	}
	// Add new aggregated metrics to destination
	sumMetric.CopyTo(metricsToSendSlice.AppendEmpty().Metrics().AppendEmpty())
	countMetric.CopyTo(metricsToSendSlice.AppendEmpty().Metrics().AppendEmpty())
	maxMetric.CopyTo(metricsToSendSlice.AppendEmpty().Metrics().AppendEmpty())
	minMetric.CopyTo(metricsToSendSlice.AppendEmpty().Metrics().AppendEmpty())
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
		Version:     "0.7",
	}
	cfg.HTTPClientSettings.Endpoint = LISTENER_URL
	cfg.HTTPClientSettings.Headers = map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", LOGZIO_TOKEN),
	}
	cfg.Namespace = ""
	cfg.ExternalLabels = map[string]string{"p8s_logzio_name": "otlp-cloudwatch-stream-metrics"}
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
		// represents the metrics to send after converting from summary to sum, count, min, max (those are the metrics we are actually going to send)
		metricsToSend := pdata.NewMetrics()
		aggregatedInstrumentationLibraryMetrics := metricsToSend.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics()
		// Lopping threw all metrics and data points, generate new metrics (sum, count, min, max) and enhance labels with logz.io naming conventions and resource attributes
		for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
			resource := metrics.ResourceMetrics().At(i)
			resourceAttributes := resource.Resource().Attributes()
			for j := 0; j < resource.InstrumentationLibraryMetrics().Len(); j++ {
				instrumentationLibraryMetrics := resource.InstrumentationLibraryMetrics().At(j)
				for k := 0; k < instrumentationLibraryMetrics.Metrics().Len(); k++ {
					metric := instrumentationLibraryMetrics.Metrics().At(k)
					summaryValuesToMetrics(aggregatedInstrumentationLibraryMetrics, metric, resourceAttributes)
				}
			}
		}
		numberOfMetrics, numberOfDataPoints := metricsToSend.MetricAndDataPointCount()
		log.Println(fmt.Sprintf("Found %d new metrics with %d datapoints", numberOfMetrics, numberOfDataPoints))
		if err != nil {
			log.Printf("Error: %s", err)
			return events.APIGatewayProxyResponse{Body: "Error", StatusCode: 500}, err
		}
		metricCount += numberOfMetrics
		log.Println("Sending metrics")
		err = metricsExporter.PushMetrics(ctx, metricsToSend)
		if err != nil {
			log.Printf("Error while pushing metrics: %s", err)
			return events.APIGatewayProxyResponse{Body: "Error while pushing metrics", StatusCode: 500}, err
		}
	}
	log.Printf("Found total of %d metrics", metricCount)
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
