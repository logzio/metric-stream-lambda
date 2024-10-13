package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

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
)

const (
	minStr            = "_min"
	maxStr            = "_max"
	cloudAccountIdAtt = "cloud.account.id"
	cloudRegionAtt    = "cloud.region"
)

func initLogger(ctx context.Context, request events.APIGatewayProxyRequest, token string) *zap.Logger {
	awsRequestId, account, logzioIdentifier := "", "", ""
	if lambdaContext, ok := lambdacontext.FromContext(ctx); ok {
		awsRequestId = lambdaContext.AwsRequestID
	}
	if arnStr := strings.Split(request.Headers["X-Amz-Firehose-Source-Arn"], ":"); len(arnStr) > 4 {
		account = arnStr[4]
	}
	if len(token) >= 5 {
		logzioIdentifier = token[len(token)-5:]
	}
	firehoseRequestId := request.Headers["X-Amz-Firehose-Request-Id"]
	config := zap.NewProductionConfig()
	config.EncoderConfig.StacktraceKey = ""
	config.OutputPaths = []string{"stdout"}
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

func isDemoData(rawData string) bool {
	return strings.Contains(rawData, "TICKER_SYMBOL") && strings.Contains(rawData, "SECTOR") && strings.Contains(rawData, "CHANGE")
}

func getListenerUrl(log *zap.Logger) string {
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
		log.Info("Region is not supported yet, setting url to default value", zap.String("region", awsRegion))
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
	if err := json.Unmarshal([]byte(commonAttributes), &commonAttributesMap); err != nil {
		return requestId, logzioToken, ""
	}
	envID := commonAttributesMap["commonAttributes"].(map[string]interface{})["p8s_logzio_name"].(string)
	fmt.Println("Common attributes: ", commonAttributesMap)
	return requestId, logzioToken, envID
}

func createPrometheusRemoteWriteExporter(log *zap.Logger, LogzioToken, envId string) (exporter.Metrics, error) {
	if envId == "" {
		envId = "logzio-otlp-metrics-stream"
	}
	cfg := &prometheusremotewriteexporter.Config{
		ExternalLabels: map[string]string{"p8s_logzio_name": envId},
		ClientConfig: confighttp.ClientConfig{
			Endpoint: getListenerUrl(log),
			Timeout:  5 * time.Second,
			Headers:  map[string]configopaque.String{"Authorization": configopaque.String(fmt.Sprintf("Bearer %s", LogzioToken))},
		},
		TargetInfo: &prometheusremotewriteexporter.TargetInfo{
			Enabled: false,
		},
		CreatedMetric: &prometheusremotewriteexporter.CreatedMetric{
			Enabled: false,
		},
		ResourceToTelemetrySettings: resourcetotelemetry.Settings{Enabled: true},
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
	return prometheusremotewriteexporter.NewFactory().CreateMetricsExporter(context.Background(), settings, cfg)
}

func convertResourceAttributes(resourceAttributes pcommon.Map) {
	newAttributes := pcommon.NewMap()
	resourceAttributes.Range(func(k string, v pcommon.Value) bool {
		lowerKey := strings.ToLower(k)
		newAttributes.PutStr(lowerKey, v.AsString())
		if lowerKey == cloudAccountIdAtt {
			newAttributes.PutStr("account", v.AsString())
		} else if lowerKey == cloudRegionAtt {
			newAttributes.PutStr("region", v.AsString())
		}
		return true
	})
	newAttributes.Remove("cloud.account.id")
	newAttributes.Remove("cloud.region")
	newAttributes.Remove("aws.exporter.arn")
	resourceAttributes.Clear()
	newAttributes.CopyTo(resourceAttributes)
}

func convertAttributes(attributes pcommon.Map) {
	attributes.Range(func(k string, v pcommon.Value) bool {
		if k == "Dimensions" {
			for dimensionKey, dimensionValue := range v.AsRaw().(map[string]interface{}) {
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

func createMinMaxMetrics(metricName string, dp pmetric.SummaryDataPoint, resourceAttributes pcommon.Map) (pmetric.Metric, pmetric.Metric) {
	minMetric := pmetric.NewMetric()
	minMetric.SetName(metricName + minStr)
	maxMetric := pmetric.NewMetric()
	maxMetric.SetName(metricName + maxStr)
	minDp := minMetric.SetEmptyGauge().DataPoints().AppendEmpty()
	maxDp := maxMetric.SetEmptyGauge().DataPoints().AppendEmpty()
	minDp.SetTimestamp(dp.StartTimestamp())
	maxDp.SetTimestamp(dp.StartTimestamp())
	// Copy resource attributes to min and max metrics
	resourceAttributes.Range(func(k string, v pcommon.Value) bool {
		minDp.Attributes().PutStr(k, v.AsString())
		return true
	})
	// Copy datapoint attributes to min and max metrics
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
	if err := protoBuffer.DecodeMessage(protoExportMetricsServiceRequest); err != nil {
		return pmetric.Metrics{}, err
	}
	protoBytes, marshalErr := proto.Marshal(protoExportMetricsServiceRequest)
	if marshalErr != nil {
		return pmetric.Metrics{}, marshalErr
	}
	exportRequest := pmetricotlp.NewExportRequest()
	if err := exportRequest.UnmarshalProto(protoBytes); err != nil {
		return pmetric.Metrics{}, err
	}
	exportRequestMetrics := exportRequest.Metrics()
	minMaxMetrics := pmetric.NewMetrics()
	minMaxMetrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	for i := 0; i < exportRequestMetrics.ResourceMetrics().Len(); i++ {
		resourceMetrics := exportRequestMetrics.ResourceMetrics().At(i)
		convertResourceAttributes(resourceMetrics.Resource().Attributes())
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			scopeMetrics := resourceMetrics.ScopeMetrics().At(j)
			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				sm := scopeMetrics.Metrics().At(k)
				newName := strings.ReplaceAll(strings.ToLower(strings.ReplaceAll(sm.Name(), "/", "_")), "amazonaws.com_", "")
				sm.SetName(newName)
				if sm.Summary().DataPoints().Len() > 1 {
					log.Info("Metric has more than one data point", zap.String("metric_name", sm.Name()))
				}
				for l := 0; l < sm.Summary().DataPoints().Len(); l++ {
					dp := sm.Summary().DataPoints().At(l)
					convertAttributes(dp.Attributes())
					minMetric, maxMetric := createMinMaxMetrics(sm.Name(), dp, resourceMetrics.Resource().Attributes())
					minMetric.CopyTo(minMaxMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty())
					maxMetric.CopyTo(minMaxMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().AppendEmpty())
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
	metricCount, dataPointCount := 0, 0
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
	if err := metricsExporter.Start(ctx, componenttest.NewNopHost()); err != nil {
		return firehoseResponseClient.GenerateValidFirehoseResponse(500, "Error while starting metrics exporter:", err), nil
	}
	log.Info("Starting to parse request body")
	var body map[string]interface{}
	if err := json.Unmarshal([]byte(request.Body), &body); err != nil {
		return firehoseResponseClient.GenerateValidFirehoseResponse(500, "Error while unmarshalling request body:", err), nil
	}
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
		log.Info("Sending metrics", zap.Int64("bulk_number", int64(recordIdx)))
		if shippingErr := metricsExporter.ConsumeMetrics(ctx, metricsToSend); shippingErr != nil {
			if strings.Contains(shippingErr.Error(), "status 401") {
				return firehoseResponseClient.GenerateValidFirehoseResponse(401, "Error while sending metrics:", shippingErr), nil
			}
			shippingErrors.Collect(err)
		} else {
			metricCount += metricsToSend.MetricCount()
			dataPointCount += metricsToSend.DataPointCount()
		}
	}
	log.Info("Found metrics with data points", zap.Int64("metric_count", int64(metricCount)), zap.Int64("datapoint_count", int64(dataPointCount)))
	log.Info("Shutting down metrics exporter")
	if err := metricsExporter.Shutdown(ctx); err != nil {
		return firehoseResponseClient.GenerateValidFirehoseResponse(500, "Error while shutting down exporter:", err), nil
	}
	if shippingErrors.Length() > 0 {
		return firehoseResponseClient.GenerateValidFirehoseResponse(500, "Error while sending metrics:", shippingErrors.Error()), nil
	}
	return firehoseResponseClient.GenerateValidFirehoseResponse(200, "", nil), nil
}
