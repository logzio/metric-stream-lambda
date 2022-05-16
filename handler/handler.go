package handler

import (
	"context"
	_ "context"
	base64 "encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/golang/protobuf/proto"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	pdata "go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/prometheusremotewriteexporter"
	_ "go.opentelemetry.io/otel/metric"
	pb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"log"
	"os"
	"strings"
	"time"
)

const (
	sumStr   = "_sum"
	countStr = "_count"
	minStr   = "_min"
	maxStr   = "_max"
)

type firehoseResponse struct {
	RequestId    string `json:"requestId"`
	Timestamp    int64  `json:"timestamp"`
	ErrorMessage string `json:"errorMessage"`
}

func generateValidFirehoseResponse(statusCode int, requestId string, errorMessage string, err error) events.APIGatewayProxyResponse {
	if errorMessage != "" {
		data := firehoseResponse{
			RequestId:    requestId,
			Timestamp:    time.Now().Unix(),
			ErrorMessage: fmt.Sprintf("%s %s", errorMessage, err),
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
		data := firehoseResponse{
			RequestId: requestId,
			Timestamp: time.Now().Unix(),
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

// Takes a base64 encoded string and returns decoded string
func base64Decode(str string) (string, bool) {
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", true
	}
	return string(data), false
}

// Generates logzio listener url based on aws region
func getListenerUrl() string {
	var url string
	// reserved lambda environment variable AWS_REGION https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
	switch awsRegion := os.Getenv("AWS_REGION"); awsRegion {
	case "us-east-1":
		url = "https://listener.logz.io:8053"
	case "ca-central-1":
		url = "https://listener-ca.logz.io:8053"
	case "eu-central-1":
		url = "https://listener-eu.logz.io:8053"
	case "eu-west-2":
		url = "https://listener-uk.logz.io:8053"
	default:
		log.Printf("Region '%s' is not supported yet, setting url to default value", awsRegion)
		url = "https://listener.logz.io:8053"
	}
	log.Printf("Setting logzio listener url to: %s", url)
	return url
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
	quantileMetric := createMetricFromAttributes(metric, "")
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
		// Min datapoint
		minDp := minMetric.DoubleSum().DataPoints().AppendEmpty()
		minDp.SetValue(datapoint.QuantileValues().At(0).Value())
		minDp.SetTimestamp(datapoint.Timestamp())
		addLabelsAndResourceAttributes(datapoint, minDp, resourceAttributes)
		// Max datapoint
		maxDp := maxMetric.DoubleSum().DataPoints().AppendEmpty()
		maxDp.SetValue(datapoint.QuantileValues().At(datapoint.QuantileValues().Len() - 1).Value())
		maxDp.SetTimestamp(datapoint.Timestamp())
		addLabelsAndResourceAttributes(datapoint, maxDp, resourceAttributes)
		// If the count value is greater than 1 and we have more than 2 Quantiles we need to add datapoints for each quantileValues
		if datapoint.Count() > 1 && datapoint.QuantileValues().Len() > 2 && datapoint.Sum() > 0 {
			for i := 1; i < datapoint.QuantileValues().Len()-1; i++ {
				quantileDp := quantileMetric.DoubleSum().DataPoints().AppendEmpty()
				quantileDp.SetValue(datapoint.QuantileValues().At(i).Value())
				quantileDp.SetTimestamp(datapoint.Timestamp())
				quantileDp.LabelsMap().Insert("quantile", fmt.Sprintf("%v", datapoint.QuantileValues().At(i).Quantile()))
				addLabelsAndResourceAttributes(datapoint, quantileDp, resourceAttributes)
			}
		}
	}
	// Add new aggregated metrics to destination
	sumMetric.CopyTo(metricsToSendSlice.AppendEmpty().Metrics().AppendEmpty())
	countMetric.CopyTo(metricsToSendSlice.AppendEmpty().Metrics().AppendEmpty())
	maxMetric.CopyTo(metricsToSendSlice.AppendEmpty().Metrics().AppendEmpty())
	minMetric.CopyTo(metricsToSendSlice.AppendEmpty().Metrics().AppendEmpty())
	if quantileMetric.DoubleSum().DataPoints().Len() > 0 {
		quantileMetric.CopyTo(metricsToSendSlice.AppendEmpty().Metrics().AppendEmpty())
	}
}

func HandleRequest(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	metricCount := 0
	dataPointCount := 0
	ListenerUrl := ""
	// extract parameters
	var ca map[string]interface{}
	err := json.Unmarshal([]byte(request.Headers["x-amz-firehose-common-attributes"]), &ca)
	if ca == nil {
		json.Unmarshal([]byte(request.Headers["X-Amz-Firehose-Common-Attributes"]), &ca)
	}
	if ca != nil {
		parameterMap := ca["commonAttributes"].(map[string]interface{})
		if parameterMap != nil {
			ListenerUrl = parameterMap["CUSTOM_LISTENER"].(string)
		}
	}
	if ListenerUrl == "" {
		ListenerUrl = getListenerUrl()
	}

	log.Println("Getting access key from headers")
	LogzioToken := request.Headers["X-Amz-Firehose-Access-Key"]
	if LogzioToken == "" {
		LogzioToken = request.Headers["x-amz-firehose-access-key"]
	}
	// get requestId to match firehose response requirements
	requestId := request.Headers["X-Amz-Firehose-Request-Id"]
	if requestId == "" {
		requestId = request.Headers["x-amz-firehose-request-id"]
	}

	if LogzioToken == "" {
		log.Printf("Cant find access key in 'X-Amz-Firehose-Access-Key' or 'x-amz-firehose-access-key' headers")
		return generateValidFirehoseResponse(400, requestId, "Cant find access key in 'X-Amz-Firehose-Access-Key' or 'x-amz-firehose-access-key' headers", nil), nil
	}

	// Initializing prometheus remote write exporter
	cfg := &prometheusremotewriteexporter.Config{
		TimeoutSettings: exporterhelper.TimeoutSettings{},
		RetrySettings: exporterhelper.RetrySettings{
			Enabled:         true,
			InitialInterval: 500 * time.Millisecond,
			MaxInterval:     1 * time.Second,
		},
		Namespace:      "",
		ExternalLabels: map[string]string{"p8s_logzio_name": "otlp-cloudwatch-stream-metrics"},
		HTTPClientSettings: confighttp.HTTPClientSettings{
			Endpoint: ListenerUrl,
			Headers:  map[string]string{"Authorization": fmt.Sprintf("Bearer %s", LogzioToken)},
		},
	}
	cfg.RemoteWriteQueue.NumConsumers = 3
	buildInfo := component.BuildInfo{
		Description: "OpenTelemetry",
		Version:     "0.7",
	}
	log.Println("Starting metrics exporter")
	metricsExporter, err := prometheusremotewriteexporter.NewPRWExporter(cfg, buildInfo)
	if err != nil {
		log.Printf("Error while creating metrics exporter: %s", err)
		return generateValidFirehoseResponse(500, requestId, "Error while creating metrics exporter:", err), err
	}
	err = metricsExporter.Start(ctx, componenttest.NewNopHost())
	if err != nil {
		log.Printf("Error while starting metrics exporter: %s", err)
		return generateValidFirehoseResponse(500, requestId, "Error while starting metrics exporter:", err), err
	}
	log.Println("Starting to parse request body")
	var body map[string]interface{}
	err = json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		log.Printf("Error while unmarshalling request body: %s", err)
		return generateValidFirehoseResponse(500, requestId, "Error while unmarshalling request body:", err), err
	}
	/*
		api request body example structure:
		{
		  "requestId": string,
		  "timestamp": int,
		  "records": [
		    {
		      "data": base 64 encoded string
		    },
		     {
		      "data": base 64 encoded string
		    },
		  ]
		}
	*/
	records := body["records"].([]interface{})
	for bulkNum, record := range records {
		//Converting the data to string
		data := record.(map[string]interface{})["data"].(string)
		//Decoding data and convert to otlp proto message
		rawDecodedText, _ := base64Decode(data)
		protoBuffer := proto.NewBuffer([]byte(rawDecodedText))
		ExportMetricsServiceRequest := &pb.ExportMetricsServiceRequest{}
		err = protoBuffer.DecodeMessage(ExportMetricsServiceRequest)
		if err != nil {
			log.Printf("Error decoding data: %s", err)
			return generateValidFirehoseResponse(500, requestId, "Error decoding data:", err), err
		}
		// Converting otlp proto message to proto bytes
		protoBytes, err := proto.Marshal(ExportMetricsServiceRequest)
		if err != nil {
			log.Printf("Error while converting otlp proto message to proto bytes: %s", err)
			return generateValidFirehoseResponse(500, requestId, "Error while converting otlp proto message to proto bytes:", err), err
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
		log.Printf("Sending metrics, Bulk number: %d", bulkNum)
		err = metricsExporter.PushMetrics(ctx, metricsToSend)
		if err != nil {
			log.Printf("Error while sending metrics: %s", err)
		} else {
			numberOfMetrics, numberOfDataPoints := metricsToSend.MetricAndDataPointCount()
			metricCount += numberOfMetrics
			dataPointCount += numberOfDataPoints
		}
	}
	log.Printf("Found total of %d metrics with %d datapoints", metricCount, dataPointCount)
	log.Println("Shutting down metrics exporter")
	err = metricsExporter.Shutdown(ctx)
	if err != nil {
		log.Printf("Error while shutting down exporter: %s", err)
		return generateValidFirehoseResponse(500, requestId, "Error while shutting down exporter:", err), err
	}

	return generateValidFirehoseResponse(200, requestId, "", nil), nil
}
