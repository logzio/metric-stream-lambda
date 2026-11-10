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
	pdata "go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"io/ioutil"
	log_old "log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

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
		assert.Equal(t, "opentelemetry/0.7", r.Header.Get("User-Agent"))
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
	useTestListener(t, server.URL)
	jsonFile, err := os.Open("../testdata/customUrlEvent.json")
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
	_, err = HandleRequest(ctx, request)
	assert.NoError(t, err)

}

func TestHandleRequestErrors(t *testing.T) {
	type getListenerUrlTest struct {
		file     string
		expected int
	}
	var getListenerUrlTests = []getListenerUrlTest{
		{"noValidToken", 400},
		{"noToken", 400},
		{"malformedBody", 400},
		{"simpleevent", 400},
		{"kinesisDemoData", 200},
	}
	for _, test := range getListenerUrlTests {
		jsonFile, err := os.Open(fmt.Sprintf("../testdata/%s.json", test.file))
		if err != nil {
			fmt.Println(err)
		}
		ctx := context.Background()
		byteValue, _ := ioutil.ReadAll(jsonFile)
		request := events.APIGatewayProxyRequest{}
		json.Unmarshal(byteValue, &request)
		result, err := HandleRequest(ctx, request)
		assert.Equal(t, test.expected, result.StatusCode)
		err = jsonFile.Close()
		assert.NoError(t, err)
	}
}

func TestCreateMetricFromAttributes(t *testing.T) {
	expected := pdata.NewMetric()
	expected.SetUnit("test_unit")
	expected.SetName("test_name_suffix")
	expected.SetDataType(pdata.MetricDataTypeDoubleSum)
	expected.DoubleSum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)

	metric := pdata.NewMetric()
	metric.SetUnit("test_unit")
	metric.SetName("test_name")
	metric.SetDataType(pdata.MetricDataTypeSummary)

	result := createMetricFromAttributes(metric, "_suffix")
	if result.Name() != expected.Name() {
		t.Fatalf("Name does not match %s != %s", result.Name(), expected.Name())
	}
	if result.DataType() != expected.DataType() {
		t.Fatalf("DataType does not match %s != %s", result.DataType(), expected.DataType())
	}
	if result.Unit() != expected.Unit() {
		t.Fatalf("Unit does not match %s != %s", result.Unit(), expected.Unit())
	}
	if result.DoubleSum().AggregationTemporality() != expected.DoubleSum().AggregationTemporality() {
		t.Fatalf("AggregationTemporality does not match %s != %s", result.DoubleSum().AggregationTemporality(), expected.DoubleSum().AggregationTemporality())
	}
}

func TestGetListenerUrl(t *testing.T) {
	type getListenerUrlTest struct {
		region   string
		expected string
	}
	var getListenerUrlTests = []getListenerUrlTest{
		{"us-east-1", "https://listener.logz.io:8053"},
		{"ca-central-1", "https://listener-ca.logz.io:8053"},
		{"eu-central-1", "https://listener-eu.logz.io:8053"},
		{"eu-west-2", "https://listener-uk.logz.io:8053"},
		{"ap-southeast-2", "https://listener-au.logz.io:8053"},
		{"", "https://listener.logz.io:8053"},
		{"not-valid", "https://listener.logz.io:8053"},
	}
	config := zap.NewProductionConfig()
	logger, configErr := config.Build()
	if configErr != nil {
		log_old.Fatal(configErr)
	}
	for _, test := range getListenerUrlTests {
		os.Setenv("AWS_REGION", test.region)
		output := getListenerUrl(*logger.Sugar())
		require.Equal(t, output, test.expected)
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

func TestSummaryValuesToMetrics(t *testing.T) {
	testMetric := pdata.NewMetric()
	testMetric.SetDataType(pdata.MetricDataTypeSummary)
	testMetric.SetName("test")
	testDp := testMetric.Summary().DataPoints().AppendEmpty()
	testDp.SetCount(2)
	testDp.SetSum(10)
	testQuantiles := testDp.QuantileValues()
	testQuantileMax := testQuantiles.AppendEmpty()
	testQuantileMax.SetValue(8)
	testQuantileMax.SetQuantile(1)
	testQuantileMin := testQuantiles.AppendEmpty()
	testQuantileMin.SetValue(0)
	testQuantileMin.SetQuantile(0)
	testQuantile99 := testQuantiles.AppendEmpty()
	testQuantile99.SetValue(6)
	testQuantile99.SetQuantile(0.99)
	testResourceAttributes := pdata.NewAttributeMap()
	testResourceAttributes.Insert("k", pdata.NewAttributeValueInt(1))
	testMetricsToSend := pdata.NewMetrics()
	testAggregatedInstrumentationLibraryMetrics := testMetricsToSend.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics()
	summaryValuesToMetrics(testAggregatedInstrumentationLibraryMetrics, testMetric, testResourceAttributes)
	assert.Equal(t, 5, testAggregatedInstrumentationLibraryMetrics.Len())

}

func useTestListener(t *testing.T, url string) {
	t.Helper()
	original := resolveListenerUrl
	resolveListenerUrl = func(zap.SugaredLogger) string { return url }
	t.Cleanup(func() { resolveListenerUrl = original })
}

func useFastRetries(t *testing.T) {
	t.Helper()
	originalInitial, originalMax, originalElapsed := remoteWriteRetryInitialInterval, remoteWriteRetryMaxInterval, remoteWriteRetryMaxElapsedTime
	originalTimeout, originalDeadline := remoteWriteTimeout, exportDeadline
	remoteWriteRetryInitialInterval = 10 * time.Millisecond
	remoteWriteRetryMaxInterval = 20 * time.Millisecond
	remoteWriteRetryMaxElapsedTime = time.Second
	remoteWriteTimeout = 200 * time.Millisecond
	t.Cleanup(func() {
		remoteWriteRetryInitialInterval, remoteWriteRetryMaxInterval, remoteWriteRetryMaxElapsedTime = originalInitial, originalMax, originalElapsed
		remoteWriteTimeout, exportDeadline = originalTimeout, originalDeadline
	})
}

func loadFixtureMessages(t *testing.T) [][]byte {
	t.Helper()
	raw, err := os.ReadFile("../testdata/validEvent.json")
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
	require.Greater(t, len(body.Records), 2, "fixture needs several single-message records")
	messages := make([][]byte, 0, len(body.Records))
	for _, record := range body.Records {
		decoded, err := base64.StdEncoding.DecodeString(record.Data)
		require.NoError(t, err)
		messages = append(messages, decoded)
	}
	return messages
}

func processRaw(t *testing.T, raw []byte) (pdata.Metrics, error) {
	t.Helper()
	return processRecord(golangproto.NewBuffer(raw), *zap.NewNop().Sugar())
}

func dataPointCount(metrics pdata.Metrics) int {
	_, dataPoints := metrics.MetricAndDataPointCount()
	return dataPoints
}

func metricNames(metrics pdata.Metrics) []string {
	var names []string
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		libraryMetrics := metrics.ResourceMetrics().At(i).InstrumentationLibraryMetrics()
		for j := 0; j < libraryMetrics.Len(); j++ {
			for k := 0; k < libraryMetrics.At(j).Metrics().Len(); k++ {
				names = append(names, libraryMetrics.At(j).Metrics().At(k).Name())
			}
		}
	}
	return names
}

func TestProcessRecordDecodesEveryMessageInRecord(t *testing.T) {
	messages := loadFixtureMessages(t)

	var expectedDataPoints int
	var expectedNames []string
	for _, message := range messages {
		metrics, err := processRaw(t, message)
		require.NoError(t, err)
		expectedDataPoints += dataPointCount(metrics)
		expectedNames = append(expectedNames, metricNames(metrics)...)
	}

	combined, err := processRaw(t, bytes.Join(messages, nil))

	require.NoError(t, err)
	assert.Equal(t, expectedDataPoints, dataPointCount(combined))
	assert.Equal(t, expectedNames, metricNames(combined))
}

func TestProcessRecordSingleMessage(t *testing.T) {
	messages := loadFixtureMessages(t)

	metrics, err := processRaw(t, messages[0])

	require.NoError(t, err)
	assert.Greater(t, dataPointCount(metrics), 0)
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
			assert.NotErrorIs(t, err, errConvertingRecord)
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
		expected pdata.Metrics
	}{
		{name: "truncated second message", raw: append(append([]byte{}, messages[0]...), messages[1][:len(messages[1])/2]...), expected: firstOnly},
		{name: "trailing length prefix without message", raw: append(append([]byte{}, messages[0]...), 0x80), expected: firstOnly},
		{name: "truncated third message", raw: append(bytes.Join(messages[:2], nil), messages[2][:len(messages[2])/2]...), expected: firstTwo},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)

			metrics, err := processRecord(golangproto.NewBuffer(test.raw), *zap.New(core).Sugar())

			require.NoError(t, err)
			assert.Equal(t, dataPointCount(test.expected), dataPointCount(metrics))
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

func newRecordsRequest(t *testing.T, records [][]byte) events.APIGatewayProxyRequest {
	t.Helper()
	raw, err := os.ReadFile("../testdata/validEvent.json")
	require.NoError(t, err)
	request := events.APIGatewayProxyRequest{}
	// the fixture has a non-map queryStringParameters, the headers are still loaded
	_ = json.Unmarshal(raw, &request)
	require.NotEmpty(t, request.Headers["X-Amz-Firehose-Access-Key"])
	encoded := make([]map[string]string, 0, len(records))
	for _, record := range records {
		encoded = append(encoded, map[string]string{"data": base64.StdEncoding.EncodeToString(record)})
	}
	body, err := json.Marshal(map[string]interface{}{"requestId": "test", "timestamp": 0, "records": encoded})
	require.NoError(t, err)
	request.Body = string(body)
	return request
}

// Returns the Firehose status code, the samples received by the listener and the number of remote write requests.
func handleRecordsWithListener(t *testing.T, records [][]byte, respond func(attempt int64, w http.ResponseWriter)) (int, int, int) {
	t.Helper()
	var samples, attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		recorder := httptest.NewRecorder()
		respond(attempt, recorder)
		if recorder.Code >= 200 && recorder.Code < 300 {
			samples.Add(int64(countRemoteWriteSamples(t, r)))
		}
		w.WriteHeader(recorder.Code)
	}))
	defer server.Close()
	useTestListener(t, server.URL)

	result, err := HandleRequest(context.Background(), newRecordsRequest(t, records))
	require.NoError(t, err)
	return result.StatusCode, int(samples.Load()), int(attempts.Load())
}

func respondWith(code int) func(int64, http.ResponseWriter) {
	return func(_ int64, w http.ResponseWriter) { w.WriteHeader(code) }
}

func TestHandleRequestShipsEveryMessageOfMultiMessageRecord(t *testing.T) {
	messages := loadFixtureMessages(t)

	separateStatus, separateSamples, _ := handleRecordsWithListener(t, messages, respondWith(http.StatusOK))
	combinedStatus, combinedSamples, _ := handleRecordsWithListener(t, [][]byte{bytes.Join(messages, nil)}, respondWith(http.StatusOK))

	assert.Equal(t, http.StatusOK, separateStatus)
	assert.Equal(t, http.StatusOK, combinedStatus)
	assert.Greater(t, separateSamples, 0)
	assert.Equal(t, separateSamples, combinedSamples)
}

func TestHandleRequestRejectsRecordWithMalformedFirstMessage(t *testing.T) {
	messages := loadFixtureMessages(t)

	status, samples, _ := handleRecordsWithListener(t, [][]byte{messages[0][:len(messages[0])/2]}, respondWith(http.StatusOK))

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, 0, samples)
}

func TestHandleRequestShipsDecodedMessagesOfRecordWithMalformedTail(t *testing.T) {
	messages := loadFixtureMessages(t)
	_, firstMessageSamples, _ := handleRecordsWithListener(t, [][]byte{messages[0]}, respondWith(http.StatusOK))
	truncated := append(append([]byte{}, messages[0]...), messages[1][:len(messages[1])/2]...)

	status, samples, _ := handleRecordsWithListener(t, [][]byte{truncated}, respondWith(http.StatusOK))

	assert.Equal(t, http.StatusOK, status)
	assert.Greater(t, firstMessageSamples, 0)
	assert.Equal(t, firstMessageSamples, samples)
}

func TestHandleRequestReportsRemoteWriteFailureToFirehose(t *testing.T) {
	useFastRetries(t)
	messages := loadFixtureMessages(t)
	tests := []struct {
		name             string
		listenerStatus   int
		expectedStatus   int
		expectedAttempts func(t *testing.T, attempts int)
	}{
		{name: "persistent 5xx is retried", listenerStatus: http.StatusServiceUnavailable, expectedStatus: http.StatusInternalServerError,
			expectedAttempts: func(t *testing.T, attempts int) { assert.Greater(t, attempts, 1) }},
		{name: "4xx is not retried", listenerStatus: http.StatusBadRequest, expectedStatus: http.StatusInternalServerError,
			expectedAttempts: func(t *testing.T, attempts int) { assert.Equal(t, 1, attempts) }},
		{name: "401 is not retried", listenerStatus: http.StatusUnauthorized, expectedStatus: http.StatusBadRequest,
			expectedAttempts: func(t *testing.T, attempts int) { assert.Equal(t, 1, attempts) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, samples, attempts := handleRecordsWithListener(t, messages[:1], respondWith(test.listenerStatus))

			assert.Equal(t, test.expectedStatus, status)
			assert.Equal(t, 0, samples)
			test.expectedAttempts(t, attempts)
		})
	}
}

func TestHandleRequestStopsAfterFirstFailedBulk(t *testing.T) {
	useFastRetries(t)
	messages := loadFixtureMessages(t)

	status, _, attempts := handleRecordsWithListener(t, messages, respondWith(http.StatusBadRequest))

	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Equal(t, 1, attempts)
}

func TestHandleRequestRecoversFromTransientRemoteWriteFailure(t *testing.T) {
	useFastRetries(t)
	messages := loadFixtureMessages(t)
	_, expectedSamples, _ := handleRecordsWithListener(t, messages[:1], respondWith(http.StatusOK))

	status, samples, attempts := handleRecordsWithListener(t, messages[:1], func(attempt int64, w http.ResponseWriter) {
		if attempt <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, 3, attempts)
	assert.Equal(t, expectedSamples, samples)
}

func TestHandleRequestRetriesTimedOutRemoteWrite(t *testing.T) {
	useFastRetries(t)
	messages := loadFixtureMessages(t)

	status, samples, attempts := handleRecordsWithListener(t, messages[:1], func(attempt int64, w http.ResponseWriter) {
		if attempt == 1 {
			time.Sleep(2 * remoteWriteTimeout)
		}
		w.WriteHeader(http.StatusOK)
	})

	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, 2, attempts)
	assert.Greater(t, samples, 0)
}

func TestHandleRequestRetriesRefusedConnection(t *testing.T) {
	useFastRetries(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedUrl := server.URL
	server.Close()
	useTestListener(t, closedUrl)

	start := time.Now()
	result, err := HandleRequest(context.Background(), newRecordsRequest(t, loadFixtureMessages(t)[:1]))

	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, result.StatusCode)
	assert.GreaterOrEqual(t, time.Since(start), remoteWriteRetryMaxElapsedTime/2)
}

func TestHandleRequestStopsExportingAtDeadline(t *testing.T) {
	useFastRetries(t)
	exportDeadline = 300 * time.Millisecond
	messages := loadFixtureMessages(t)

	start := time.Now()
	status, _, _ := handleRecordsWithListener(t, messages, respondWith(http.StatusServiceUnavailable))

	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Less(t, time.Since(start), remoteWriteRetryMaxElapsedTime)
}

func TestHandleRequestSkipsRecordsAfterDeadline(t *testing.T) {
	useFastRetries(t)
	exportDeadline = 0
	messages := loadFixtureMessages(t)

	status, _, attempts := handleRecordsWithListener(t, messages, respondWith(http.StatusOK))

	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Equal(t, 0, attempts)
}

func TestHandleRequestResetsDetectedNamespacesPerInvocation(t *testing.T) {
	messages := loadFixtureMessages(t)
	status, _, _ := handleRecordsWithListener(t, messages[:1], respondWith(http.StatusOK))
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, detectedNamespaces)

	status, _, _ = handleRecordsWithListener(t, [][]byte{}, respondWith(http.StatusOK))

	assert.Equal(t, http.StatusOK, status)
	assert.Empty(t, detectedNamespaces)
}
