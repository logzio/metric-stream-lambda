package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	pdata "go.opentelemetry.io/collector/consumer/pdata"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestHandleRequest(t *testing.T) {
	jsonFile, err := os.Open("../testdata/customUrlEvent.json")
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()
	ctx := context.Background()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	request := events.APIGatewayProxyRequest{}
	json.Unmarshal(byteValue, &request)
	result, _ := HandleRequest(ctx, request)
	log.Println(result)
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

}
