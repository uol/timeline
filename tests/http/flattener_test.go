package timeline_http_test

import (
	"testing"
	"time"

	"github.com/uol/funks"
	gotesthttp "github.com/uol/gotest/http"
	"github.com/uol/hashing"
	serializer "github.com/uol/serializer/json"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManagerF - creates a new timeline manager
func createTimelineManagerF(start, manualMode bool) *timeline.Manager {

	backend := timeline.Backend{
		Host: testServerHost,
		Port: testServerPort,
	}

	transport := createHTTPTransport(defaultTransportSize, time.Second, applicationJSON, nil)

	conf := &timeline.DataTransformerConfig{
		CycleDuration: funks.Duration{
			Duration: time.Millisecond * 900,
		},
		HashingAlgorithm: hashing.SHA256,
	}

	flattener := timeline.NewFlattener(conf)

	manager, err := timeline.NewManager(transport, flattener, nil, &backend)
	if err != nil {
		panic(err)
	}

	if start {
		err = manager.Start(manualMode)
		if err != nil {
			panic(err)
		}

	}

	return manager
}

// toGenericParameters - converts a number point to generic parameters
func toGenericParameters(point *serializer.NumberPoint) []interface{} {

	return []interface{}{
		"metric", point.Metric,
		"tags", point.Tags,
		"value", point.Value,
		"timestamp", point.Timestamp,
	}
}

// testFlatOperation - tests some operation
func testFlatOperation(t *testing.T, manualMode bool, operation timeline.FlatOperation, expectedValue float64, opValues ...float64) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManagerF(true, manualMode)
	defer m.Shutdown()

	number := newNumberPoint(expectedValue)

	for _, v := range opValues {

		number.Value = v
		m.FlattenJSON(operation, numberPoint, toGenericParameters(number)...)
	}

	var waitFor time.Duration
	if !manualMode {
		<-time.After(2 * time.Second)
		waitFor = time.Second
	} else {
		m.ProcessCycle()
		m.SendData()
		waitFor = time.Millisecond
	}

	number.Value = expectedValue

	requestData := gotesthttp.WaitForServerRequest(s, waitFor, 10*time.Second)
	testRequestData(t, requestData, []*serializer.NumberPoint{number}, true, false, applicationJSON)
}

// TestSendSum - tests the sum operation
func TestSendSum(t *testing.T) {

	for _, v := range manualModeArray {
		testFlatOperation(t, v, timeline.Sum, 11, 5.5, 1.24, 3.76, 0.5)
	}
}

// TestSendAvg - tests the avg operation
func TestSendAvg(t *testing.T) {

	for _, v := range manualModeArray {
		testFlatOperation(t, v, timeline.Avg, 40, 25, 25, 25, 25, 100)
	}
}

// TestSendMax - tests the max operation
func TestSendMax(t *testing.T) {

	for _, v := range manualModeArray {
		testFlatOperation(t, v, timeline.Max, 10.8, 1, -200, 10.7, 10.8, 0, 5)
	}
}

// TestSendMin - tests the min operation
func TestSendMin(t *testing.T) {

	for _, v := range manualModeArray {
		testFlatOperation(t, v, timeline.Min, -200, 1, -200, 10.7, 10.8, 0)
	}
}

// TestCountMin - tests the count operation
func TestCountMin(t *testing.T) {

	for _, v := range manualModeArray {
		testFlatOperation(t, v, timeline.Count, 5, 1, -200, 10.7, 10.8, 0)
	}
}

// TestSendSum - tests the sum operation
func TestManualModeSendSum(t *testing.T) {

	for _, v := range manualModeArray {
		testFlatOperation(t, v, timeline.Sum, 11, 5.5, 1.24, 3.76, 0.5)
	}
}
