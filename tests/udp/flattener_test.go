package timeline_udp_test

import (
	"testing"
	"time"

	"github.com/uol/funks"
	"github.com/uol/gotest/tcpudp"
	"github.com/uol/hashing"
	serializer "github.com/uol/serializer/json"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManagerF - creates a new timeline manager
func createTimelineManagerF(port int, start bool) *timeline.Manager {

	backend := timeline.Backend{
		Host: defaultConf.Host,
		Port: port,
	}

	transport := createUDPTransport(defaultTransportSize, time.Second, nil)

	conf := &timeline.DataTransformerConfig{
		CycleDuration: funks.Duration{
			Duration: time.Millisecond * 900,
		},
		HashingAlgorithm:     hashing.SHA256,
		PointValueBufferSize: 1000,
	}

	flattener := timeline.NewFlattener(conf)

	manager, err := timeline.NewManager(transport, flattener, nil, &backend)
	if err != nil {
		panic(err)
	}

	if start {
		err = manager.Start()
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
func testFlatOperation(t *testing.T, operation timeline.FlatOperation, expectedValue float64, opValues ...float64) {

	s, port := tcpudp.NewUDPServer(&defaultConf, true)
	defer s.Stop()

	m := createTimelineManagerF(port, true)
	defer m.Shutdown()

	number := newNumberPoint(expectedValue)

	for _, v := range opValues {

		number.Value = v
		m.FlattenJSON(operation, numberPoint, toGenericParameters(number)...)
	}

	number.Value = expectedValue

	message := <-s.MessageChannel()
	testReceivedData(t, &message, number, true)
}

// TestSendSum - tests the sum operation
func TestSendSum(t *testing.T) {

	testFlatOperation(t, timeline.Sum, 11, 5.5, 1.24, 3.76, 0.5)
}

// TestSendAvg - tests the avg operation
func TestSendAvg(t *testing.T) {

	testFlatOperation(t, timeline.Avg, 40, 25, 25, 25, 25, 100)
}

// TestSendMax - tests the max operation
func TestSendMax(t *testing.T) {

	testFlatOperation(t, timeline.Max, 10.8, 1, -200, 10.7, 10.8, 0, 5)
}

// TestSendMin - tests the min operation
func TestSendMin(t *testing.T) {

	testFlatOperation(t, timeline.Min, -200, 1, -200, 10.7, 10.8, 0)
}

// TestCountMin - tests the count operation
func TestCountMin(t *testing.T) {

	testFlatOperation(t, timeline.Count, 5, 1, -200, 10.7, 10.8, 0)
}
