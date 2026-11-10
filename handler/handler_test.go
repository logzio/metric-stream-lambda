package handler

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/gogo/protobuf/proto"
	golangproto "github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestBase64Decode(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		hasError bool
	}{
		{
			name:     "valid base64 string",
			input:    "aGVsbG8gd29ybGQ=",
			expected: "hello world",
			hasError: false,
		},
		{
			name:     "invalid base64 string",
			input:    "invalid_base64",
			expected: "",
			hasError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := base64Decode(test.input)
			if test.hasError {
				assert.Equal(t, true, err)
			} else {
				assert.Equal(t, false, err)
				assert.Equal(t, test.expected, result)
			}
		})
	}
}

func TestIsDemoData(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "contains demo data",
			input:    "TICKER_SYMBOL, SECTOR, CHANGE",
			expected: true,
		},
		{
			name:     "does not contain demo data",
			input:    "random data",
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := isDemoData(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestGetListenerUrl(t *testing.T) {
	tests := []struct {
		name     string
		region   string
		expected string
	}{
		{
			name:     "us-east-1 region",
			region:   "us-east-1",
			expected: "https://listener.logz.io:8053",
		},
		{
			name:     "ca-central-1 region",
			region:   "ca-central-1",
			expected: "https://listener-ca.logz.io:8053",
		},
		{
			name:     "unsupported region",
			region:   "unsupported-region",
			expected: "https://listener.logz.io:8053",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			os.Setenv("AWS_REGION", test.region)
			log := zap.NewNop()
			result := getListenerUrl(log)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestExtractHeaders(t *testing.T) {
	tests := []struct {
		name          string
		headers       map[string]string
		expected      handlerConfig
		expectedError bool
	}{
		{
			name: "All headers present",
			headers: map[string]string{
				"X-Amz-Firehose-Request-Id":        "request-id",
				"X-Amz-Firehose-Access-Key":        "access-key",
				"X-Amz-Firehose-Common-Attributes": `{"commonAttributes":{"P8S_LOGZIO_NAME":"env-id","CUSTOM_LISTENER":"custom-url"}}`,
			},
			expected: handlerConfig{
				RequestId:   "request-id",
				LogzioToken: "access-key",
				EnvId:       "env-id",
				url:         "custom-url",
			},
		},
		{
			name: "Missing optional headers",
			headers: map[string]string{
				"X-Amz-Firehose-Request-Id": "request-id",
				"X-Amz-Firehose-Access-Key": "access-key",
			},
			expected: handlerConfig{
				RequestId:   "request-id",
				LogzioToken: "access-key",
				EnvId:       "",
				url:         "",
			},
		},
		{
			name: "Missing required headers",
			headers: map[string]string{
				"X-Amz-Firehose-Common-Attributes": `{"commonAttributes":{"P8S_LOGZIO_NAME":"env-id","CUSTOM_LISTENER":"custom-url"}}`,
			},
			expected: handlerConfig{
				RequestId:   "",
				LogzioToken: "",
				EnvId:       "env-id",
				url:         "custom-url",
			},
		},
		{
			name: "Invalid JSON in common attributes",
			headers: map[string]string{
				"X-Amz-Firehose-Request-Id":        "request-id",
				"X-Amz-Firehose-Access-Key":        "access-key",
				"X-Amz-Firehose-Common-Attributes": `{"commonAttributes":{"P8S_LOGZIO_NAME":"env-id","CUSTOM_LISTENER":"custom-url"`,
			},
			expected: handlerConfig{
				RequestId:   "request-id",
				LogzioToken: "access-key",
				EnvId:       "",
				url:         "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := events.APIGatewayProxyRequest{
				Headers: tt.headers,
			}
			config := extractHeaders(request)
			assert.Equal(t, tt.expected, config)
		})
	}
}
func TestConvertResourceAttributes(t *testing.T) {
	tests := []struct {
		name     string
		input    pcommon.Map
		expected pcommon.Map
	}{
		{
			name: "convert attributes",
			input: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("cloud.account.id", "12345")
				m.PutStr("cloud.region", "us-east-1")
				m.PutStr("aws.exporter.arn", "arn:aws:...")
				m.PutStr("OTHER_KEY", "value")
				m.PutStr("TestAttribute", "value")
				return m
			}(),
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("account", "12345")
				m.PutStr("region", "us-east-1")
				m.PutStr("other_key", "value")
				m.PutStr("testattribute", "value")

				return m
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			convertResourceAttributes(test.input)
			assert.Equal(t, test.expected.AsRaw(), test.input.AsRaw())
		})
	}
}

func TestConvertAttributes(t *testing.T) {
	tests := []struct {
		name     string
		input    pcommon.Map
		expected pcommon.Map
	}{
		{
			name: "convert dimensions",
			input: func() pcommon.Map {
				m := pcommon.NewMap()
				dimensions := map[string]interface{}{
					"Dimension1": "Value1",
					"Dimension2": "Value2",
				}
				m.PutEmptyMap("Dimensions").FromRaw(dimensions)
				return m
			}(),
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("dimension1", "value1")
				m.PutStr("dimension2", "value2")
				return m
			}(),
		},
		{
			name: "convert attributes",
			input: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("Attribute1", "Value1")
				m.PutStr("Attribute2", "Value2")
				return m
			}(),
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("attribute1", "value1")
				m.PutStr("attribute2", "value2")
				return m
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			convertAttributes(test.input)
			assert.Equal(t, test.expected.AsRaw(), test.input.AsRaw())
		})
	}
}

func TestCreateMinMaxMetrics(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		quantiles  []float64
		attributes map[string]string
	}{
		{
			name:       "single quantile",
			metricName: "test_metric",
			quantiles:  []float64{1.0, 5.0},
			attributes: map[string]string{"key": "value"},
		},
		{
			name:       "multiple quantiles",
			metricName: "test_metric_multiple",
			quantiles:  []float64{0.5, 2.5, 4.5},
			attributes: map[string]string{"key1": "value1", "key2": "value2"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dp := pmetric.NewSummaryDataPoint()
			dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			for _, q := range test.quantiles {
				dp.QuantileValues().AppendEmpty().SetValue(q)
			}
			for k, v := range test.attributes {
				dp.Attributes().PutStr(k, v)
			}

			minMetric, maxMetric := createMinMaxMetrics(test.metricName, dp, pcommon.NewMap())

			assert.Equal(t, test.metricName+minStr, minMetric.Name())
			assert.Equal(t, test.metricName+maxStr, maxMetric.Name())
			if len(test.quantiles) > 0 {
				assert.Equal(t, test.quantiles[0], minMetric.Gauge().DataPoints().At(0).DoubleValue())
				assert.Equal(t, test.quantiles[len(test.quantiles)-1], maxMetric.Gauge().DataPoints().At(0).DoubleValue())
			}
			assert.Equal(t, dp.StartTimestamp(), minMetric.Gauge().DataPoints().At(0).Timestamp())
			assert.Equal(t, dp.StartTimestamp(), maxMetric.Gauge().DataPoints().At(0).Timestamp())
			for k, v := range test.attributes {
				minExpectedValue, _ := minMetric.Gauge().DataPoints().At(0).Attributes().Get(k)
				maxExpectedValue, _ := maxMetric.Gauge().DataPoints().At(0).Attributes().Get(k)
				assert.Equal(t, v, minExpectedValue.AsString())
				assert.Equal(t, v, maxExpectedValue.AsString())
			}
		})
	}
}

func TestHandleRequest(t *testing.T) {
	var metricCount = 0
	handleFunc := func(w http.ResponseWriter, r *http.Request, code int) {
		// The following is a handler function that reads the sent httpRequest, unmarshal, and checks if the WriteRequest
		// preserves the TimeSeries data correctly
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		require.NotNil(t, body)
		// Receives the http requests and unzip, unmarshalls, and extracts TimeSeries
		assert.Equal(t, "0.1.0", r.Header.Get("X-Prometheus-Remote-Write-Version"))
		assert.Equal(t, "snappy", r.Header.Get("Content-Encoding"))
		assert.Equal(t, "logziometricstream/1.0", r.Header.Get("User-Agent"))
		writeReq := &prompb.WriteRequest{}
		var unzipped []byte
		dest, err := snappy.Decode(unzipped, body)
		require.NoError(t, err)
		ok := proto.Unmarshal(dest, writeReq)
		require.NoError(t, ok)
		require.NotNil(t, writeReq.GetTimeseries())
		metricCount += len(writeReq.Timeseries)
		w.WriteHeader(code)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if handleFunc != nil {
			handleFunc(w, r, http.StatusOK)
		}
	}))
	defer server.Close()
	jsonFile, err := os.Open("../testdata/otlp-1.0/customUrlEvent.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()
	ctx := context.Background()
	// Create a new context with the AwsRequestID key-value pair
	ctx = context.WithValue(ctx, "AwsRequestID", "12345")
	byteValue, _ := ioutil.ReadAll(jsonFile)
	request := events.APIGatewayProxyRequest{}
	json.Unmarshal(byteValue, &request)
	request.Headers["x-amz-firehose-common-attributes"] = fmt.Sprintf("{\"commonAttributes\":{\"LOGZIO_TOKEN\":\"token\",\"CUSTOM_LISTENER\":\"%s\"}}", server.URL)
	result, err := HandleRequest(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, result.StatusCode)
	assert.Greater(t, metricCount, 0)
}

func useFastRemoteWriteRetry(t *testing.T) {
	original := remoteWriteRetry
	remoteWriteRetry.InitialInterval = 10 * time.Millisecond
	remoteWriteRetry.MaxInterval = 20 * time.Millisecond
	remoteWriteRetry.MaxElapsedTime = 1 * time.Second
	originalTimeout := remoteWriteTimeout
	remoteWriteTimeout = 200 * time.Millisecond
	t.Cleanup(func() {
		remoteWriteRetry = original
		remoteWriteTimeout = originalTimeout
	})
}

func newCustomUrlRequest(t *testing.T, listenerUrl string) events.APIGatewayProxyRequest {
	byteValue, err := os.ReadFile("../testdata/otlp-1.0/customUrlEvent.json")
	require.NoError(t, err)
	request := events.APIGatewayProxyRequest{}
	require.NoError(t, json.Unmarshal(byteValue, &request))
	request.Headers["x-amz-firehose-common-attributes"] = fmt.Sprintf(
		"{\"commonAttributes\":{\"CUSTOM_LISTENER\":\"%s\"}}", listenerUrl)
	return request
}

func TestHandleRequestReportsRemoteWriteFailureToFirehose(t *testing.T) {
	tests := []struct {
		name               string
		listenerStatus     int
		expectedStatus     int
		expectedRequests   int32
		expectRetriedSends bool
	}{
		{name: "listener 5xx is retried, then reported", listenerStatus: http.StatusInternalServerError,
			expectedStatus: http.StatusInternalServerError, expectRetriedSends: true},
		{name: "listener 4xx is not retried and is reported", listenerStatus: http.StatusBadRequest,
			expectedStatus: http.StatusInternalServerError, expectedRequests: 1},
		{name: "listener 401 is reported as 401", listenerStatus: http.StatusUnauthorized,
			expectedStatus: http.StatusUnauthorized, expectedRequests: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useFastRemoteWriteRetry(t)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(test.listenerStatus)
			}))
			defer server.Close()

			result, err := HandleRequest(context.Background(), newCustomUrlRequest(t, server.URL))

			assert.NoError(t, err)
			assert.Equal(t, test.expectedStatus, result.StatusCode)
			assert.Contains(t, result.Body, "Error while sending metrics")
			if test.expectRetriedSends {
				assert.Greater(t, requests.Load(), int32(1))
			} else {
				assert.Equal(t, test.expectedRequests, requests.Load())
			}
		})
	}
}

func TestHandleRequestRecoversFromTransientRemoteWriteFailure(t *testing.T) {
	useFastRemoteWriteRetry(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	result, err := HandleRequest(context.Background(), newCustomUrlRequest(t, server.URL))

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, result.StatusCode)
	assert.Greater(t, requests.Load(), int32(2))
}

func TestHandleRequestRetriesTimedOutRemoteWrite(t *testing.T) {
	useFastRemoteWriteRetry(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			time.Sleep(2 * remoteWriteTimeout)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	result, err := HandleRequest(context.Background(), newCustomUrlRequest(t, server.URL))

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, result.StatusCode)
	assert.Greater(t, requests.Load(), int32(1))
}

func TestHandleRequestStopsExportingAtDeadline(t *testing.T) {
	useFastRemoteWriteRetry(t)
	originalDeadline := exportDeadline
	exportDeadline = 300 * time.Millisecond
	t.Cleanup(func() { exportDeadline = originalDeadline })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	start := time.Now()
	result, err := HandleRequest(context.Background(), newCustomUrlRequest(t, server.URL))

	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, result.StatusCode)
	assert.Less(t, time.Since(start), remoteWriteRetry.MaxElapsedTime)
}

func TestHandleRequestSkipsRecordsAfterDeadline(t *testing.T) {
	originalDeadline := exportDeadline
	exportDeadline = 0
	t.Cleanup(func() { exportDeadline = originalDeadline })
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	result, err := HandleRequest(context.Background(), newCustomUrlRequest(t, server.URL))

	assert.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, result.StatusCode)
	assert.Contains(t, result.Body, "export deadline reached")
	assert.Equal(t, int32(0), requests.Load())
}

func TestHandleRequestErrors(t *testing.T) {
	type getListenerUrlTest struct {
		file     string
		expected int
	}
	var getListenerUrlTests = []getListenerUrlTest{
		{"noToken", 400},
		{"malformedBody", 400},
		{"simpleevent", 400},
		{"kinesisDemoData", 200},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	for _, test := range getListenerUrlTests {
		jsonFile, err := os.Open(fmt.Sprintf("../testdata/otlp-1.0/%s.json", test.file))
		if err != nil {
			fmt.Println(err)
		}
		ctx := context.Background()
		byteValue, _ := ioutil.ReadAll(jsonFile)
		request := events.APIGatewayProxyRequest{}
		json.Unmarshal(byteValue, &request)
		if request.Headers != nil {
			request.Headers["x-amz-firehose-common-attributes"] = fmt.Sprintf(
				"{\"commonAttributes\":{\"CUSTOM_LISTENER\":\"%s\"}}", server.URL)
		}
		result, err := HandleRequest(ctx, request)
		assert.Equal(t, test.expected, result.StatusCode)
		err = jsonFile.Close()
		assert.NoError(t, err)
	}
}

func TestRemoveDuplicateValues(t *testing.T) {
	tests := []struct {
		name     string
		intSlice []string
		expected []string
	}{
		{
			name:     "no duplicates",
			intSlice: []string{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "all duplicates",
			intSlice: []string{"a", "a", "a"},
			expected: []string{"a"},
		},
		{
			name:     "some duplicates",
			intSlice: []string{"a", "b", "c", "a", "b"},
			expected: []string{"a", "b", "c"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := removeDuplicateValues(test.intSlice)
			assert.Equal(t, result, test.expected, "Expected %v, got %v", test.expected, result)
		})
	}
}

func loadFixtureMessages(t *testing.T) [][]byte {
	t.Helper()
	raw, err := os.ReadFile("../testdata/otlp-1.0/validEvent.json")
	require.NoError(t, err)
	var event struct {
		Body string `json:"body"`
	}
	require.NoError(t, json.Unmarshal(raw, &event))
	var body struct {
		Records []struct {
			Data string `json:"data"`
		} `json:"records"`
	}
	require.NoError(t, json.Unmarshal([]byte(event.Body), &body))
	require.Greater(t, len(body.Records), 1, "fixture needs several single-message records")
	messages := make([][]byte, 0, len(body.Records))
	for _, record := range body.Records {
		decoded, err := base64.StdEncoding.DecodeString(record.Data)
		require.NoError(t, err)
		messages = append(messages, decoded)
	}
	return messages
}

func processRaw(t *testing.T, raw []byte) (pmetric.Metrics, error) {
	t.Helper()
	return processRecord(golangproto.NewBuffer(raw), zap.NewNop())
}

func metricNames(metrics pmetric.Metrics) []string {
	var names []string
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		scopeMetrics := metrics.ResourceMetrics().At(i).ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			for k := 0; k < scopeMetrics.At(j).Metrics().Len(); k++ {
				names = append(names, scopeMetrics.At(j).Metrics().At(k).Name())
			}
		}
	}
	return names
}

func TestProcessRecordDecodesEveryMessageInRecord(t *testing.T) {
	messages := loadFixtureMessages(t)

	var expectedDataPoints, expectedMetrics int
	var expectedNames []string
	for _, message := range messages {
		metrics, err := processRaw(t, message)
		require.NoError(t, err)
		expectedDataPoints += metrics.DataPointCount()
		expectedMetrics += metrics.MetricCount()
		expectedNames = append(expectedNames, metricNames(metrics)...)
	}

	combined, err := processRaw(t, bytes.Join(messages, nil))

	require.NoError(t, err)
	assert.Equal(t, expectedDataPoints, combined.DataPointCount())
	assert.Equal(t, expectedMetrics, combined.MetricCount())
	assert.Equal(t, expectedNames, metricNames(combined))
}

func TestProcessRecordSingleMessage(t *testing.T) {
	messages := loadFixtureMessages(t)

	metrics, err := processRaw(t, messages[0])

	require.NoError(t, err)
	assert.Greater(t, metrics.DataPointCount(), 0)
}

func TestProcessRecordRejectsRecordWithMalformedFirstMessage(t *testing.T) {
	messages := loadFixtureMessages(t)
	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "empty record", raw: []byte{}},
		{name: "truncated first message", raw: messages[0][:len(messages[0])/2]},
		{name: "length prefix without message", raw: []byte{0x80}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := processRaw(t, test.raw)

			assert.Error(t, err)
		})
	}
}

func TestProcessRecordKeepsDecodedMessagesWhenLaterMessageIsMalformed(t *testing.T) {
	messages := loadFixtureMessages(t)
	firstOnly, err := processRaw(t, messages[0])
	require.NoError(t, err)
	firstTwo, err := processRaw(t, bytes.Join(messages[:2], nil))
	require.NoError(t, err)
	tests := []struct {
		name     string
		raw      []byte
		expected pmetric.Metrics
	}{
		{name: "truncated second message", raw: append(append([]byte{}, messages[0]...), messages[1][:len(messages[1])/2]...), expected: firstOnly},
		{name: "trailing length prefix without message", raw: append(append([]byte{}, messages[0]...), 0x80), expected: firstOnly},
		{name: "truncated third message", raw: append(bytes.Join(messages[:2], nil), messages[0][:len(messages[0])/2]...), expected: firstTwo},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)

			metrics, err := processRecord(golangproto.NewBuffer(test.raw), zap.New(core))

			require.NoError(t, err)
			assert.Equal(t, test.expected.DataPointCount(), metrics.DataPointCount())
			assert.Equal(t, metricNames(test.expected), metricNames(metrics))
			assert.Equal(t, 1, logs.FilterMessage("Skipping the rest of the record after a malformed message").Len())
		})
	}
}

func countRemoteWriteSamples(t *testing.T, r *http.Request) int {
	t.Helper()
	body, err := ioutil.ReadAll(r.Body)
	require.NoError(t, err)
	decoded, err := snappy.Decode(nil, body)
	require.NoError(t, err)
	writeReq := &prompb.WriteRequest{}
	require.NoError(t, proto.Unmarshal(decoded, writeReq))
	samples := 0
	for _, ts := range writeReq.Timeseries {
		samples += len(ts.Samples)
	}
	return samples
}

func handleRecords(t *testing.T, records [][]byte) (int, int) {
	t.Helper()
	var samples atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		samples.Add(int64(countRemoteWriteSamples(t, r)))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	request := newCustomUrlRequest(t, server.URL)
	encoded := make([]map[string]string, 0, len(records))
	for _, record := range records {
		encoded = append(encoded, map[string]string{"data": base64.StdEncoding.EncodeToString(record)})
	}
	body, err := json.Marshal(map[string]interface{}{"requestId": "test", "timestamp": 0, "records": encoded})
	require.NoError(t, err)
	request.Body = string(body)

	result, err := HandleRequest(context.Background(), request)
	require.NoError(t, err)
	return result.StatusCode, int(samples.Load())
}

func TestHandleRequestShipsEveryMessageOfMultiMessageRecord(t *testing.T) {
	messages := loadFixtureMessages(t)

	separateStatus, separateSamples := handleRecords(t, messages)
	combinedStatus, combinedSamples := handleRecords(t, [][]byte{bytes.Join(messages, nil)})

	assert.Equal(t, http.StatusOK, separateStatus)
	assert.Equal(t, http.StatusOK, combinedStatus)
	assert.Greater(t, separateSamples, 0)
	assert.Equal(t, separateSamples, combinedSamples)
}

func TestHandleRequestRejectsRecordWithMalformedFirstMessage(t *testing.T) {
	messages := loadFixtureMessages(t)

	status, samples := handleRecords(t, [][]byte{messages[0][:len(messages[0])/2]})

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, 0, samples)
}

func TestHandleRequestShipsDecodedMessagesOfRecordWithMalformedTail(t *testing.T) {
	messages := loadFixtureMessages(t)
	_, firstMessageSamples := handleRecords(t, [][]byte{messages[0]})
	truncated := append(append([]byte{}, messages[0]...), messages[1][:len(messages[1])/2]...)

	status, samples := handleRecords(t, [][]byte{truncated})

	assert.Equal(t, http.StatusOK, status)
	assert.Greater(t, firstMessageSamples, 0)
	assert.Equal(t, firstMessageSamples, samples)
}
