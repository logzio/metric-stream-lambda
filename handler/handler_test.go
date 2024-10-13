package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
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
			result := getListenerUrl(*log)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestExtractHeaders(t *testing.T) {
	tests := []struct {
		name          string
		input         events.APIGatewayProxyRequest
		expectedReqId string
		expectedToken string
		expectedEnvID string
	}{
		{
			name: "valid headers",
			input: events.APIGatewayProxyRequest{
				Headers: map[string]string{
					"X-Amz-Firehose-Request-Id":        "request-id",
					"X-Amz-Firehose-Access-Key":        "access-key",
					"X-Amz-Firehose-Common-Attributes": `{"commonAttributes":{"p8s_logzio_name":"env-id"}}`,
				},
			},
			expectedReqId: "request-id",
			expectedToken: "access-key",
			expectedEnvID: "env-id",
		},
		{
			name: "missing headers",
			input: events.APIGatewayProxyRequest{
				Headers: map[string]string{},
			},
			expectedReqId: "",
			expectedToken: "",
			expectedEnvID: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reqId, token, envID := extractHeaders(test.input)
			assert.Equal(t, test.expectedReqId, reqId)
			assert.Equal(t, test.expectedToken, token)
			assert.Equal(t, test.expectedEnvID, envID)
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
				return m
			}(),
			expected: func() pcommon.Map {
				m := pcommon.NewMap()
				m.PutStr("account", "12345")
				m.PutStr("region", "us-east-1")
				m.PutStr("other_key", "value")
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

			minMetric, maxMetric := createMinMaxMetrics(test.metricName, dp)

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

func TestHandleRequestOTLP10(t *testing.T) {
	jsonFile, err := os.Open("../testdata/otlp-1.0/validEvent.json")
	if err != nil {
		t.Fatal(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	request := events.APIGatewayProxyRequest{}
	json.Unmarshal(byteValue, &request)

	ctx := context.Background()
	// Create a new context with the AwsRequestID key-value pair
	ctx = context.WithValue(ctx, "AwsRequestID", "12345")

	_, err = HandleRequest(ctx, request)
	assert.NoError(t, err)
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
	jsonFile, err := os.Open("../testdata/otlp-0.7/customUrlEvent.json")
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
		jsonFile, err := os.Open(fmt.Sprintf("../testdata/otlp-0.7/%s.json", test.file))
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
