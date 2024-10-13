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
	"github.com/logzio/metric-stream-lambda/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/resourcetotelemetry"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/trace"
	pb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"strings"
	"time"
)

const (
	minStr            = "_min"
	maxStr            = "_max"
	cloudAccountIdAtt = "cloud.account.id"
	cloudRegionAtt    = "cloud.region"
)

func initLogger(ctx context.Context, request events.APIGatewayProxyRequest, token string) *zap.Logger {
	awsRequestId := ""
	account := ""
	logzioIdentifier := ""
	lambdaContext, ok := lambdacontext.FromContext(ctx)
	if ok {
		awsRequestId = lambdaContext.AwsRequestID
	}
	awsAccount := strings.Split(request.Headers["X-Amz-Firehose-Source-Arn"], ":")
	if len(awsAccount) > 4 {
		account = awsAccount[4]
	}
	if len(token) >= 5 {
		logzioIdentifier = token[len(token)-5:]
	}
	firehoseRequestId := request.Headers["X-Amz-Firehose-Request-Id"]
	config := zap.NewProductionConfig()
	config.EncoderConfig.StacktraceKey = "" // to hide stacktrace info
	config.OutputPaths = []string{"stdout"} // write to stdout
	config.InitialFields = map[string]interface{}{
		"aws_account":               account,
		"lambda_invocation_id":      awsRequestId,
		"firehose_request_id":       firehoseRequestId,
		"logzio_account_identifier": logzioIdentifier,
	}
	logger, configErr := config.Build()
	if configErr != nil {
		fmt.Printf("Error while initializing the logger: %v", configErr)
		panic(configErr)
	}
	return logger
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
func getListenerUrl(log zap.Logger) string {
	// reserved lambda environment variable AWS_REGION https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
	switch awsRegion := os.Getenv("AWS_REGION"); awsRegion {
	case "us-east-1":
		return "https://listener.logz.io:8053"
	case "ca-central-1":
		return "https://listener-ca.logz.io:8053"
	case "eu-central-1":
		return "https://listener-eu.logz.io:8053"
	case "eu-west-2":
		return "https://listener-uk.logz.io:8053"
	case "ap-southeast-2":
		return "https://listener-au.logz.io:8053"
	default:
		log.Info("Region is not supported yet, setting url to default value", zap.Field{Key: "region", Type: zapcore.StringType, String: awsRegion})
		return "https://listener.logz.io:8053"
	}
}

func extractHeaders(request events.APIGatewayProxyRequest) (string, string, string) {
	requestId := request.Headers["X-Amz-Firehose-Request-Id"]
	if requestId == "" {
		requestId = request.Headers["x-amz-firehose-request-id"]
	}
	logzioToken := request.Headers["X-Amz-Firehose-Access-Key"]
	if logzioToken == "" {
		logzioToken = request.Headers["x-amz-firehose-access-key"]
	}
	commonAttributes := request.Headers["X-Amz-Firehose-Common-Attributes"]
	if commonAttributes == "" {
		commonAttributes = request.Headers["x-amz-firehose-common-attributes"]
	}
	var commonAttributesMap map[string]interface{}
	err := json.Unmarshal([]byte(commonAttributes), &commonAttributesMap)
	if err != nil {
		return "", "", ""
	}
	envID := commonAttributesMap["commonAttributes"].(map[string]interface{})["p8s_logzio_name"].(string)
	fmt.Println("Common attributes: ", commonAttributesMap)

	return requestId, logzioToken, envID
}

func createPrometheusRemoteWriteExporter(log *zap.Logger, LogzioToken string, envId string) (exporter.Metrics, error) {
	cfg := &prometheusremotewriteexporter.Config{
		Namespace:      "",
		ExternalLabels: map[string]string{"p8s_logzio_name": envId},
		ClientConfig: confighttp.ClientConfig{
			Endpoint: getListenerUrl(*log),
			Timeout:  5 * time.Second,
			Headers:  map[string]configopaque.String{"Authorization": configopaque.String(fmt.Sprintf("Bearer %s", LogzioToken))},
		},
		ResourceToTelemetrySettings: resourcetotelemetry.Settings{
			Enabled: true,
		},
		TargetInfo: &prometheusremotewriteexporter.TargetInfo{
			Enabled: false,
		},
		CreatedMetric: &prometheusremotewriteexporter.CreatedMetric{
			Enabled: false,
		},
		RemoteWriteQueue: prometheusremotewriteexporter.RemoteWriteQueue{
			Enabled:      true,
			NumConsumers: 3,
			QueueSize:    1000,
		},
		MaxBatchSizeBytes: 3000000,
	}
	settings := exporter.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger:         log,
			MetricsLevel:   configtelemetry.LevelNone,
			TracerProvider: trace.NewTracerProvider(),
			LeveledMeterProvider: func(level configtelemetry.Level) metric.MeterProvider {
				return noop.NewMeterProvider()
			},
		},
		BuildInfo: component.BuildInfo{
			Description: "logzioMetricStream",
			Version:     "1.0",
		},
	}
	metricsExporter, err := prometheusremotewriteexporter.NewFactory().CreateMetricsExporter(context.Background(), settings, cfg)
	if err != nil {
		return nil, err
	}
	return metricsExporter, nil
}

func convertResourceAttributes(resourceAttributes pcommon.Map) {
	resourceAttributes.Range(func(k string, v pcommon.Value) bool {
		resourceAttributes.PutStr(strings.ToLower(k), strings.ToLower(v.AsString()))
		return true
	})
	accountId, ok := resourceAttributes.Get(cloudAccountIdAtt)
	if ok {
		resourceAttributes.PutStr("account", accountId.AsString())
		resourceAttributes.Remove(cloudAccountIdAtt)
	}
	region, ok := resourceAttributes.Get(cloudRegionAtt)
	if ok {
		resourceAttributes.PutStr("region", region.AsString())
		resourceAttributes.Remove(cloudRegionAtt)
	}
	resourceAttributes.Remove("aws.exporter.arn")
}

func convertAttributes(attributes pcommon.Map) {
	attributes.Range(func(k string, v pcommon.Value) bool {
		if k == "Dimensions" {
			dimensions := v.AsRaw().(map[string]interface{})
			for dimensionKey, dimensionValue := range dimensions {
				attributes.PutStr(strings.ToLower(dimensionKey), strings.ToLower(dimensionValue.(string)))
			}
			attributes.Remove(k)
		} else {
			attributes.PutStr(strings.ToLower(k), strings.ToLower(v.AsString()))
			attributes.Remove(k)
		}
		return true
	})
}

func createMinMaxMetrics(metricName string, dp pmetric.SummaryDataPoint) (pmetric.Metric, pmetric.Metric) {
	minMetric := pmetric.NewMetric()
	minMetric.SetName(metricName + minStr)
	maxMetric := pmetric.NewMetric()
	maxMetric.SetName(metricName + maxStr)
	minDp := minMetric.SetEmptyGauge().DataPoints().AppendEmpty()
	maxDp := maxMetric.SetEmptyGauge().DataPoints().AppendEmpty()

	minDp.SetTimestamp(dp.StartTimestamp())
	maxDp.SetTimestamp(dp.StartTimestamp())
	dp.Attributes().Range(func(k string, v pcommon.Value) bool {
		minDp.Attributes().PutStr(k, v.AsString())
		maxDp.Attributes().PutStr(k, v.AsString())
		return true
	})
	minDp.SetDoubleValue(dp.QuantileValues().At(0).Value())
	maxDp.SetDoubleValue(dp.QuantileValues().At(dp.QuantileValues().Len() - 1).Value())

	return minMetric, maxMetric
}

func processRecord(protoBuffer *proto.Buffer, log *zap.Logger) (pmetric.Metrics, error) {
	protoExportMetricsServiceRequest := &pb.ExportMetricsServiceRequest{}
	err := protoBuffer.DecodeMessage(protoExportMetricsServiceRequest)
	if err != nil {
		return pmetric.Metrics{}, err
	}
	protoBytes, marshalErr := proto.Marshal(protoExportMetricsServiceRequest)
	if marshalErr != nil {
		return pmetric.Metrics{}, marshalErr
	}
	exportRequest := pmetricotlp.NewExportRequest()
	err = exportRequest.UnmarshalProto(protoBytes)
	if err != nil {
		return pmetric.Metrics{}, err
	}
	exportRequestMetrics := exportRequest.Metrics()
	minMaxMetrics := pmetric.NewMetrics()
	minMaxMetrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	// Parse metrics according to logzio naming conventions
	for i := 0; i < exportRequestMetrics.ResourceMetrics().Len(); i++ {
		resourceMetrics := exportRequestMetrics.ResourceMetrics().At(i)
		convertResourceAttributes(resourceMetrics.Resource().Attributes())
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			scopeMetrics := resourceMetrics.ScopeMetrics().At(j)
			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				sm := scopeMetrics.Metrics().At(k)
				// Convert metric name to Logz.io naming convention
				newName := strings.ReplaceAll(strings.ToLower(strings.ReplaceAll(sm.Name(), "/", "_")), "amazonaws.com_", "")
				sm.SetName(newName)
				if sm.Summary().DataPoints().Len() > 1 {
					log.Info("Metric has more than one data point", zap.Field{Key: "metric_name", Type: zapcore.StringType, String: sm.Name()})
				}
				for l := 0; l < sm.Summary().DataPoints().Len(); l++ {
					dp := sm.Summary().DataPoints().At(l)
					convertAttributes(dp.Attributes())
					minMetric, maxMetric := createMinMaxMetrics(sm.Name(), dp)
					minMetric.CopyTo(minMaxMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty())
					maxMetric.CopyTo(minMaxMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty())
					// Remove quantiles 0 and 1 that represent min and max values
					dp.QuantileValues().RemoveIf(func(qv pmetric.SummaryDataPointValueAtQuantile) bool {
						return qv.Quantile() == 0 || qv.Quantile() == 1
					})
				}
			}
		}
	}
	minMaxMetrics.ResourceMetrics().MoveAndAppendTo(exportRequestMetrics.ResourceMetrics())
	return exportRequestMetrics, nil
}

func HandleRequest(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	metricCount := 0
	dataPointCount := 0
	shippingErrors := new(internal.ErrorCollector)
	requestId, LogzioToken, envid := extractHeaders(request)
	log := initLogger(ctx, request, LogzioToken)
	firehoseResponseClient := internal.NewResponseClient(requestId, log)
	defer log.Sync()
	if LogzioToken == "" {
		accessKeyErr := errors.New("cant find access key in 'X-Amz-Firehose-Access-Key' or 'x-amz-firehose-access-key' headers")
		log.Error(accessKeyErr.Error())
		return firehoseResponseClient.GenerateValidFirehoseResponse(400, "Error while getting access keys:", accessKeyErr), nil
	}
	metricsExporter, err := createPrometheusRemoteWriteExporter(log, LogzioToken, envid)
	if err != nil {
		return firehoseResponseClient.GenerateValidFirehoseResponse(500, "Error while creating metrics exporter:", err), nil
	}
	err = metricsExporter.Start(ctx, componenttest.NewNopHost())
	if err != nil {
		return firehoseResponseClient.GenerateValidFirehoseResponse(500, "Error while starting metrics exporter:", err), nil
	}
	log.Info("Starting to parse request body")
	var body map[string]interface{}
	err = json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return firehoseResponseClient.GenerateValidFirehoseResponse(500, "Error while unmarshalling request body:", err), nil
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
	for recordIdx, record := range records {
		data := record.(map[string]interface{})["data"].(string)
		rawDecodedText, _ := base64Decode(data)
		if isDemoData(rawDecodedText) {
			continue
		}
		protoBuffer := proto.NewBuffer([]byte(rawDecodedText))
		metricsToSend, err := processRecord(protoBuffer, log)
		if err != nil {
			return firehoseResponseClient.GenerateValidFirehoseResponse(400, "Error processing record:", err), nil
		}

		log.Info("Sending metrics", zap.Field{Key: "bulk_number", Type: zapcore.Int64Type, Integer: int64(recordIdx)})
		shippingErr := metricsExporter.ConsumeMetrics(ctx, metricsToSend)
		if shippingErr != nil {
			if strings.Contains(shippingErr.Error(), "status 401") {
				return firehoseResponseClient.GenerateValidFirehoseResponse(401, "Error while sending metrics:", shippingErr), nil
			}
			shippingErrors.Collect(err)
		} else {
			metricCount += metricsToSend.MetricCount()
			dataPointCount += metricsToSend.DataPointCount()
		}
	}
	log.Info("Found metrics with data points", zap.Field{Key: "metric_count", Type: zapcore.Int64Type, Integer: int64(metricCount)}, zap.Field{Key: "datapoint_count", Type: zapcore.Int64Type, Integer: int64(dataPointCount)})
	log.Info("Shutting down metrics exporter")
	err = metricsExporter.Shutdown(ctx)
	if err != nil {
		return firehoseResponseClient.GenerateValidFirehoseResponse(500, "Error while shutting down exporter:", err), nil
	}
	if shippingErrors.Length() > 0 {
		return firehoseResponseClient.GenerateValidFirehoseResponse(500, "Error while sending metrics:", shippingErrors.Error()), nil
	}
	return firehoseResponseClient.GenerateValidFirehoseResponse(200, "", nil), nil
}
