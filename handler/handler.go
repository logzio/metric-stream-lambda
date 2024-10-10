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
	minStr = "_min"
	maxStr = "_max"
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

func updateMetricTimestamps(metrics pmetric.Metrics, log *zap.Logger) {
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		resourceMetrics := metrics.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			scopeMetrics := resourceMetrics.ScopeMetrics().At(j)
			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				m := scopeMetrics.Metrics().At(k)
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						m.Gauge().DataPoints().At(l).SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						m.Sum().DataPoints().At(l).SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						m.Histogram().DataPoints().At(l).SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						m.Summary().DataPoints().At(l).SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
					}
				default:
					log.Info("Unknown metric type", zap.Field{Key: "metric_type", Type: zapcore.StringType, String: m.Type().String()})
				}
			}
		}
	}
}

func extractHeaders(request events.APIGatewayProxyRequest) (string, string) {
	requestId := request.Headers["X-Amz-Firehose-Request-Id"]
	if requestId == "" {
		requestId = request.Headers["x-amz-firehose-request-id"]
	}
	LogzioToken := request.Headers["X-Amz-Firehose-Access-Key"]
	if LogzioToken == "" {
		LogzioToken = request.Headers["x-amz-firehose-access-key"]
	}
	return requestId, LogzioToken
}

func createPrometheusRemoteWriteExporter(log *zap.Logger, LogzioToken string) (exporter.Metrics, error) {
	cfg := &prometheusremotewriteexporter.Config{
		Namespace:      "",
		ExternalLabels: map[string]string{"p8s_logzio_name": "otlp-1"},
		ClientConfig: confighttp.ClientConfig{
			Endpoint: getListenerUrl(*log),
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
	log.Info("Starting metrics exporter")
	metricsExporter, err := prometheusremotewriteexporter.NewFactory().CreateMetricsExporter(context.Background(), settings, cfg)
	if err != nil {
		log.Info("Error while creating metrics exporter", zap.Error(err))
		return nil, err
	}
	err = metricsExporter.Start(context.Background(), componenttest.NewNopHost())
	if err != nil {
		log.Info("Error while starting metrics exporter", zap.Error(err))
		return nil, err
	}
	return metricsExporter, nil
}

func HandleRequest(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	metricCount := 0
	dataPointCount := 0
	shippingErrors := new(errorCollector)
	requestId, LogzioToken := extractHeaders(request)
	log := initLogger(ctx, request, LogzioToken)
	firehoseResponseClient := newResponseClient(requestId, log)
	defer log.Sync()
	if LogzioToken == "" {
		accessKeyErr := errors.New("cant find access key in 'X-Amz-Firehose-Access-Key' or 'x-amz-firehose-access-key' headers")
		log.Error(accessKeyErr.Error())
		return firehoseResponseClient.generateValidFirehoseResponse(400, "Error while getting access keys:", accessKeyErr), nil
	}
	metricsExporter, err := createPrometheusRemoteWriteExporter(log, LogzioToken)
	if err != nil {
		return firehoseResponseClient.generateValidFirehoseResponse(500, "Error while creating metrics exporter:", err), nil
	}
	err = metricsExporter.Start(ctx, componenttest.NewNopHost())
	if err != nil {
		return firehoseResponseClient.generateValidFirehoseResponse(500, "Error while starting metrics exporter:", err), nil
	}
	log.Info("Starting to parse request body")
	var body map[string]interface{}
	err = json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return firehoseResponseClient.generateValidFirehoseResponse(500, "Error while unmarshalling request body:", err), nil
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
		protoExportMetricsServiceRequest := &pb.ExportMetricsServiceRequest{}
		err = protoBuffer.DecodeMessage(protoExportMetricsServiceRequest)
		if err != nil {
			return firehoseResponseClient.generateValidFirehoseResponse(400, "Error decoding otlp proto message, make sure you are using opentelemetry 1.0 metrics format. Error message:", err), nil
		}
		protoBytes, marshalErr := proto.Marshal(protoExportMetricsServiceRequest)
		if marshalErr != nil {
			return firehoseResponseClient.generateValidFirehoseResponse(500, "Error while converting otlp proto message to proto bytes:", marshalErr), nil
		}
		exportRequest := pmetricotlp.NewExportRequest()
		err = exportRequest.UnmarshalProto(protoBytes)
		if err != nil {
			return events.APIGatewayProxyResponse{}, err
		}
		exportRequestMetrics := exportRequest.Metrics()
		minMaxMetrics := pmetric.NewMetrics()
		minMaxMetrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
		// TODO remove this after testing
		updateMetricTimestamps(exportRequestMetrics, log)
		// Parse metrics according to logzio naming conventions
		for i := 0; i < exportRequestMetrics.ResourceMetrics().Len(); i++ {
			resourceMetrics := exportRequestMetrics.ResourceMetrics().At(i)
			// Handle resource attributes conversion
			resourceAttributes := resourceMetrics.Resource().Attributes()
			resourceAttributes.Range(func(k string, v pcommon.Value) bool {
				resourceAttributes.PutStr(strings.ToLower(k), strings.ToLower(v.AsString()))
				return true
			})
			accountId, ok := resourceAttributes.Get("cloud.account.id")
			if ok {
				resourceAttributes.PutStr("account", accountId.AsString())
				resourceAttributes.Remove("cloud.account.id")
			}
			region, ok := resourceAttributes.Get("cloud.region")
			if ok {
				resourceAttributes.PutStr("region", region.AsString())
				resourceAttributes.Remove("cloud.region")
			}
			resourceAttributes.Remove("aws.exporter.arn")

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
						dp.Attributes().Range(func(k string, v pcommon.Value) bool {
							if k == "Dimensions" {
								dimensions := v.AsRaw().(map[string]interface{})
								for dimensionKey, dimensionValue := range dimensions {
									dp.Attributes().PutStr(strings.ToLower(dimensionKey), strings.ToLower(dimensionValue.(string)))
								}
								dp.Attributes().Remove(k)
							} else {
								dp.Attributes().PutStr(strings.ToLower(k), strings.ToLower(v.AsString()))
								dp.Attributes().Remove(k)
							}
							return true
						})
						// Create new min max metrics and remove quantiles
						minMetric := pmetric.NewMetric()
						minMetric.SetName(sm.Name() + minStr)
						maxMetric := pmetric.NewMetric()
						maxMetric.SetName(sm.Name() + maxStr)
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
						minMetric.CopyTo(minMaxMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty())
						maxMetric.CopyTo(minMaxMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty())

						// Remove quantiles 0 and 1 that represent min and max
						dp.QuantileValues().RemoveIf(func(qv pmetric.SummaryDataPointValueAtQuantile) bool {
							return qv.Quantile() == 0 || qv.Quantile() == 1
						})
					}
				}
			}
		}
		metricsToSend := pmetric.NewMetrics()
		exportRequestMetrics.ResourceMetrics().MoveAndAppendTo(metricsToSend.ResourceMetrics())
		minMaxMetrics.ResourceMetrics().MoveAndAppendTo(metricsToSend.ResourceMetrics())

		log.Info("Sending metrics", zap.Field{Key: "bulk_number", Type: zapcore.Int64Type, Integer: int64(bulkNum)})
		err = metricsExporter.ConsumeMetrics(ctx, metricsToSend)
		if err != nil {
			if strings.Contains(err.Error(), "status 401") {
				return firehoseResponseClient.generateValidFirehoseResponse(401, "Error while sending metrics:", err), nil
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
		return firehoseResponseClient.generateValidFirehoseResponse(500, "Error while shutting down exporter:", err), nil
	}
	if shippingErrors.Length() > 0 {
		return firehoseResponseClient.generateValidFirehoseResponse(500, "Error while sending metrics:", shippingErrors.Error()), nil
	}
	return firehoseResponseClient.generateValidFirehoseResponse(200, "", nil), nil
}
