package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/golang/protobuf/proto"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/prometheusremotewriteexporter"
	pb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"go.uber.org/zap"
	logold "log"
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

var detectedNamespaces []string

type ErrorCollector []error

func (c *ErrorCollector) Collect(e error) { *c = append(*c, e) }

func (c *ErrorCollector) Length() int {
	return len(*c)
}
func (c *ErrorCollector) Error() error {
	err := "Collected errors:\n"
	for i, e := range *c {
		err += fmt.Sprintf("\tError %d: %s\n", i, e.Error())
	}
	return errors.New(err)
}

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
		data := firehoseResponse{
			RequestId:    requestId,
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
func initLogger(ctx context.Context, request events.APIGatewayProxyRequest) zap.SugaredLogger {
	awsRequestId := ""
	account := ""
	lambdaContext, ok := lambdacontext.FromContext(ctx)
	if ok {
		awsRequestId = lambdaContext.AwsRequestID
	}
	awsAccount := strings.Split(request.Headers["X-Amz-Firehose-Source-Arn"], ":")
	if len(awsAccount) > 4 {
		account = awsAccount[4]
	}
	firehoseRequestId := request.Headers["X-Amz-Firehose-Request-Id"]
	config := zap.NewProductionConfig()
	config.EncoderConfig.StacktraceKey = "" // to hide stacktrace info
	config.InitialFields = map[string]interface{}{
		"aws_account":          account,
		"lambda_invocation_id": awsRequestId,
		"firehose_request_id":  firehoseRequestId,
	}
	logger, configErr := config.Build()
	if configErr != nil {
		logold.Fatal(configErr)
	}
	return *logger.Sugar()
}

// Takes a base64 encoded string and returns decoded string
func base64Decode(str string) (string, bool) {
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", true
	}
	return string(data), false
}
func removeDuplicateValues(intSlice []string) []string {
	keys := make(map[string]bool)
	var list []string
	for _, entry := range intSlice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

// isDemoData checks if the payload is kinesis demo data
func isDemoData(rawData string) bool {
	if strings.Contains(rawData, "TICKER_SYMBOL") && strings.Contains(rawData, "SECTOR") && strings.Contains(rawData, "CHANGE") {
		return true
	}
	return false
}

// Generates logzio listener url based on aws region
func getListenerUrl(log zap.SugaredLogger) string {
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
	case "ap-southeast-2":
		url = "https://listener-au.logz.io:8053"
	default:
		log.Infof("Region '%s' is not supported yet, setting url to default value", awsRegion)
		url = "https://listener.logz.io:8053"
	}
	log.Infof("Setting logzio listener url to: %s", url)
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
		namespace, _ := datapoint.LabelsMap().Get("Namespace")
		detectedNamespaces = append(detectedNamespaces, namespace)
		setTimestampNow := namespace == "AWS/S3"
		// Sum datapoint
		sumDp := sumMetric.DoubleSum().DataPoints().AppendEmpty()
		sumDp.SetValue(datapoint.Sum())
		if setTimestampNow {
			sumDp.SetTimestamp(pdata.TimestampFromTime(time.Now()))
		} else {
			sumDp.SetTimestamp(datapoint.Timestamp())
		}
		addLabelsAndResourceAttributes(datapoint, sumDp, resourceAttributes)
		// Count datapoint
		countDp := countMetric.DoubleSum().DataPoints().AppendEmpty()
		countDp.SetValue(float64(datapoint.Count()))
		if setTimestampNow {
			countDp.SetTimestamp(pdata.TimestampFromTime(time.Now()))
		} else {
			countDp.SetTimestamp(datapoint.Timestamp())
		}
		addLabelsAndResourceAttributes(datapoint, countDp, resourceAttributes)
		// Min datapoint
		minDp := minMetric.DoubleSum().DataPoints().AppendEmpty()
		minDp.SetValue(datapoint.QuantileValues().At(0).Value())
		if setTimestampNow {
			minDp.SetTimestamp(pdata.TimestampFromTime(time.Now()))
		} else {
			minDp.SetTimestamp(datapoint.Timestamp())
		}
		addLabelsAndResourceAttributes(datapoint, minDp, resourceAttributes)
		// Max datapoint
		maxDp := maxMetric.DoubleSum().DataPoints().AppendEmpty()
		maxDp.SetValue(datapoint.QuantileValues().At(datapoint.QuantileValues().Len() - 1).Value())
		if setTimestampNow {
			maxDp.SetTimestamp(pdata.TimestampFromTime(time.Now()))
		} else {
			maxDp.SetTimestamp(datapoint.Timestamp())
		}
		addLabelsAndResourceAttributes(datapoint, maxDp, resourceAttributes)
		// If the count value is greater than 1, and we have more than 2 Quantiles we need to add datapoints for each quantileValues
		if datapoint.Count() > 1 && datapoint.QuantileValues().Len() > 2 && datapoint.Sum() > 0 {
			for i := 1; i < datapoint.QuantileValues().Len()-1; i++ {
				quantileDp := quantileMetric.DoubleSum().DataPoints().AppendEmpty()
				quantileDp.SetValue(datapoint.QuantileValues().At(i).Value())
				if setTimestampNow {
					quantileDp.SetTimestamp(pdata.TimestampFromTime(time.Now()))
				} else {
					quantileDp.SetTimestamp(datapoint.Timestamp())
				}
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
	log := initLogger(ctx, request)
	metricCount := 0
	dataPointCount := 0
	shippingErrors := new(ErrorCollector)
	log.Infof("Getting access key from headers")
	// get requestId to match firehose response requirements
	requestId := request.Headers["X-Amz-Firehose-Request-Id"]
	if requestId == "" {
		requestId = request.Headers["x-amz-firehose-request-id"]
	}
	LogzioToken := request.Headers["X-Amz-Firehose-Access-Key"]
	if LogzioToken == "" {
		LogzioToken = request.Headers["x-amz-firehose-access-key"]
	}
	if LogzioToken == "" {
		accessKeyErr := errors.New("cant find access key in 'X-Amz-Firehose-Access-Key' or 'x-amz-firehose-access-key' headers")
		log.Error(accessKeyErr)
		return generateValidFirehoseResponse(400, requestId, "Error while getting access keys:", accessKeyErr), nil
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
			Endpoint: getListenerUrl(log),
			Headers:  map[string]string{"Authorization": fmt.Sprintf("Bearer %s", LogzioToken)},
		},
	}
	cfg.RemoteWriteQueue.NumConsumers = 3
	buildInfo := component.BuildInfo{
		Description: "OpenTelemetry",
		Version:     "0.7",
	}
	log.Info("Starting metrics exporter")
	metricsExporter, err := prometheusremotewriteexporter.NewPRWExporter(cfg, buildInfo)
	if err != nil {
		log.Infof("Error while creating metrics exporter: %s", err)
		return generateValidFirehoseResponse(500, requestId, "Error while creating metrics exporter:", err), nil
	}
	err = metricsExporter.Start(ctx, componenttest.NewNopHost())
	if err != nil {
		log.Infof("Error while starting metrics exporter: %s", err)
		return generateValidFirehoseResponse(500, requestId, "Error while starting metrics exporter:", err), nil
	}
	log.Info("Starting to parse request body")
	var body map[string]interface{}
	err = json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		log.Infof("Error while unmarshalling request body: %s", err)
		return generateValidFirehoseResponse(500, requestId, "Error while unmarshalling request body:", err), nil
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
		if isDemoData(rawDecodedText) {
			continue
		}
		protoBuffer := proto.NewBuffer([]byte(rawDecodedText))
		ExportMetricsServiceRequest := &pb.ExportMetricsServiceRequest{}
		err = protoBuffer.DecodeMessage(ExportMetricsServiceRequest)
		if err != nil {
			log.Warnf("Error decoding otlp proto message, make sure you are using opentelemetry 0.7 metrics format. Error message: %s", err)
			return generateValidFirehoseResponse(400, requestId, "Error decoding otlp proto message, make sure you are using opentelemetry 0.7 metrics format. Error message:", err), nil
		}
		// Converting otlp proto message to proto bytes
		protoBytes, err := proto.Marshal(ExportMetricsServiceRequest)
		if err != nil {
			log.Warnf("Error while converting otlp proto message to proto bytes: %s", err)
			return generateValidFirehoseResponse(500, requestId, "Error while converting otlp proto message to proto bytes:", err), nil
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
		log.Infof("Sending metrics, Bulk number: %d", bulkNum)
		err = metricsExporter.PushMetrics(ctx, metricsToSend)
		if err != nil {
			log.Warnf("Error while sending metrics: %s", err)
			shippingErrors.Collect(err)
		} else {
			numberOfMetrics, numberOfDataPoints := metricsToSend.MetricAndDataPointCount()
			metricCount += numberOfMetrics
			dataPointCount += numberOfDataPoints
		}
	}
	log.Infof("Found total of %d metrics with %d datapoints from the following namespaces %s", metricCount, dataPointCount, removeDuplicateValues(detectedNamespaces))
	log.Info("Shutting down metrics exporter")
	err = metricsExporter.Shutdown(ctx)
	if err != nil {
		log.Warnf("Error while shutting down exporter: %s", err)
		return generateValidFirehoseResponse(500, requestId, "Error while shutting down exporter:", err), nil
	}
	if shippingErrors.Length() > 0 {
		return generateValidFirehoseResponse(500, requestId, "Error while sending metrics:", shippingErrors.Error()), nil
	}
	return generateValidFirehoseResponse(200, requestId, "", nil), nil
}
