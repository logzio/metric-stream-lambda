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
	pdata "go.opentelemetry.io/collector/consumer/pdata"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
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
	jsonFile, err := os.Open("../testdata/customUrlEvent.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()
	ctx := context.Background()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	request := events.APIGatewayProxyRequest{}
	json.Unmarshal(byteValue, &request)
	request.Headers["x-amz-firehose-common-attributes"] = fmt.Sprintf("{\"commonAttributes\":{\"LOGZIO_TOKEN\":\"token\",\"CUSTOM_LISTENER\":\"%s\"}}", server.URL)
	result, err := HandleRequest(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, result.StatusCode, http.StatusOK)
	assert.Equal(t, 3688, metricCount)

}

func TestHandleRequestErrors(t *testing.T) {
	type getListenerUrlTest struct {
		file     string
		expected int
	}
	var getListenerUrlTests = []getListenerUrlTest{
		{"noValidToken", 500},
		{"noToken", 400},
		{"malformedBody", 500},
		{"simpleevent", 400},
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
		{"", "https://listener.logz.io:8053"},
		{"not-valid", "https://listener.logz.io:8053"},
	}
	for _, test := range getListenerUrlTests {
		os.Setenv("AWS_REGION", test.region)
		output := getListenerUrl()
		require.Equal(t, output, test.expected)
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
