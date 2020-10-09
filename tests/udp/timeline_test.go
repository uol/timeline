package timeline_udp_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uol/gotest/tcpudp"
	utils "github.com/uol/gotest/utils"
	jsonserializer "github.com/uol/serializer/json"
	otsdbserializer "github.com/uol/serializer/opentsdb"
	serializer "github.com/uol/serializer/serializer"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

// createTimelineManager - creates a new timeline manager
func createTimelineManager(port int, start, manualMode bool, transportSize int, batchSendInterval time.Duration, s serializer.Serializer) *timeline.Manager {

	backend := timeline.Backend{
		Host: defaultConf.Host,
		Port: port,
	}

	transport := createUDPTransport(transportSize, batchSendInterval, s)

	manager, err := timeline.NewManager(transport, nil, nil, &backend)
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

// testReceivedData - tests the request data
func testReceivedData(t *testing.T, message *tcpudp.MessageData, expected *jsonserializer.NumberPoint, ignoreTimestamp bool) bool {

	var actual jsonserializer.NumberPoint
	err := json.Unmarshal([]byte(message.Message), &actual)
	if !assert.Nil(t, err, "error unmarshalling to number point") {
		return false
	}

	if !assert.NotNil(t, expected, "expected value cannot be null") {
		return false
	}

	if !assert.NotNil(t, actual, "actual value cannot be null") {
		return false
	}

	result := assert.Equal(t, expected.Metric, actual.Metric, "number point's metric differs")

	if !ignoreTimestamp {
		result = result && assert.Equal(t, expected.Timestamp, actual.Timestamp, "number point's timestamp differs")
	} else {
		result = result && assert.Greater(t, expected.Timestamp, time.Now().Unix()-(60*1000), "expected a valid timestamp")
		result = result && assert.Greater(t, actual.Timestamp, time.Now().Unix()-(60*1000), "expected a valid timestamp")
	}

	result = result && assert.True(t, reflect.DeepEqual(expected.Tags, actual.Tags), "number point's tags differs")
	result = result && assert.Equal(t, expected.Value, actual.Value, "number point's value differs")

	if !result {
		return false
	}

	return result
}

// toGenericParametersN - converts a number point to generic parameters
func toGenericParametersN(point *jsonserializer.NumberPoint) []interface{} {

	return []interface{}{
		"metric", point.Metric,
		"timestamp", point.Timestamp,
		"value", point.Value,
		"tags", point.Tags,
	}
}

// TestSendSingle - test single point
func TestSendSingle(t *testing.T) {

	s, port := tcpudp.NewUDPServer(&defaultConf, true)
	defer s.Stop()

	m := createTimelineManager(port, true, false, defaultTransportSize, time.Second, nil)
	defer m.Shutdown()

	number := newNumberPoint(1)

	err := m.SendJSON(numberPoint, toGenericParametersN(number)...)
	if !assert.NoError(t, err, "no error expected when sending number") {
		return
	}

	message := <-s.MessageChannel()
	testReceivedData(t, &message, number, true)
}

// TestSendMultiple - test multiple points
func TestSendMultiple(t *testing.T) {

	s, port := tcpudp.NewUDPServer(&defaultConf, true)
	defer s.Stop()

	m := createTimelineManager(port, true, false, defaultTransportSize, time.Second, nil)
	defer m.Shutdown()

	numbers := []*jsonserializer.NumberPoint{newNumberPoint(1), newNumberPoint(2), newNumberPoint(3)}

	for _, n := range numbers {
		err := m.SendJSON(numberPoint, toGenericParametersN(n)...)
		assert.NoError(t, err, "no error expected when sending number")
	}

	for i := 0; i < len(numbers); i++ {
		message := <-s.MessageChannel()
		testReceivedData(t, &message, numbers[i], true)
	}
}

// TestSendCustomJSON - tests configuring the json variables
func TestSendCustomJSON(t *testing.T) {

	s, port := tcpudp.NewUDPServer(&defaultConf, true)
	defer s.Stop()

	const custom = "customPoint"

	cs := jsonserializer.New(128)

	number := newNumberPoint(1.0)

	// only value is variable
	err := cs.Add(custom, *number, "value")
	if !assert.NoError(t, err, "no error adding custom configuration") {
		return
	}

	m := createTimelineManager(port, false, false, defaultTransportSize, time.Second, cs)
	defer m.Shutdown()

	m.Start(false)

	err = m.SendJSON(custom, "value", 5.0)
	if !assert.NoError(t, err, "no error expected when sending number") {
		return
	}

	number.Value = 5.0

	message := <-s.MessageChannel()
	testReceivedData(t, &message, number, true)
}

// TestSerialization - tests configuring the json variables
func TestSerialization(t *testing.T) {

	s, port := tcpudp.NewUDPServer(&defaultConf, true)
	defer s.Stop()

	m := createTimelineManager(port, false, false, defaultTransportSize, time.Second, nil)
	defer m.Shutdown()

	number := newNumberPoint(15)

	serialized, err := m.SerializeJSON(numberPoint, toGenericParametersN(number)...)
	if !assert.NoError(t, err, "no error expected when serializing number") {
		return
	}

	message := tcpudp.MessageData{
		Message: serialized,
	}

	testReceivedData(t, &message, number, false)
}

// TestExceedingBufferSize - tests when the buffer exceeds its size
func TestExceedingBufferSize(t *testing.T) {

	s, port := tcpudp.NewUDPServer(&defaultConf, true)
	defer s.Stop()

	bufferSize := 2
	numPoints := 6
	numRequests := numPoints / bufferSize
	requests := make([]tcpudp.MessageData, numRequests)
	batchSendInterval := 5 * time.Second
	wc := sync.WaitGroup{}
	wc.Add(numRequests)

	go func() {
		for i := 0; i < numRequests; i++ {
			requests[i] = <-s.MessageChannel()
			wc.Done()
		}
	}()

	m := createTimelineManager(port, true, false, bufferSize, batchSendInterval, nil)
	defer m.Shutdown()

	numbers := make([]*jsonserializer.NumberPoint, numPoints)
	firstPointTime := time.Now().Unix()

	for i := 0; i < numPoints; i++ {

		numbers[i] = newNumberPoint(float64(i))

		err := m.SendJSON(numberPoint, toGenericParametersN(numbers[i])...)
		if !assert.NoError(t, err, "no error expected when sending number") {
			return
		}
	}

	wc.Wait()

	// the time between requests must be minimal and the last request will be $batchSendDuration
	for i := 0; i < numRequests-1; i++ {
		if !assert.LessOrEqual(t, requests[i].Date.Unix()-firstPointTime, int64(10), "expected less than 10 milisecond") {
			return
		}
	}

	// the last time will be the same as $batchSendDuration
	assert.GreaterOrEqual(t, requests[numRequests-1].Date.Unix()-firstPointTime, int64(batchSendInterval.Seconds()), "expected greater than batch interval")
	assert.LessOrEqual(t, requests[numRequests-1].Date.Unix()-firstPointTime, int64(batchSendInterval.Seconds()+1), "expected less than batch interval plus one second")
}

// TestSendOpenTSDBFormat - tests using the opentsdb serializer
func TestSendOpenTSDBFormat(t *testing.T) {

	s, port := tcpudp.NewUDPServer(&defaultConf, true)
	defer s.Stop()

	cs := otsdbserializer.New(128)

	m := createTimelineManager(port, false, false, defaultTransportSize, time.Second, cs)
	defer m.Shutdown()

	m.Start(false)

	items := []*otsdbserializer.ArrayItem{
		{
			Metric:    "opentsdb-metric1",
			Timestamp: time.Now().Unix(),
			Value:     float64(utils.RandomInt(0, 1000)),
			Tags: []interface{}{
				"tag1", "val1",
				"tag2", "val2",
				"tag3", "val3",
			},
		},
		{
			Metric:    "opentsdb-metric2",
			Timestamp: time.Now().Unix(),
			Value:     float64(utils.RandomInt(0, 1000)),
			Tags: []interface{}{
				"tag4", "val4",
				"tag5", "val5",
			},
		},
		{
			Metric:    "opentsdb-metric3",
			Timestamp: time.Now().Unix(),
			Value:     float64(utils.RandomInt(0, 1000)),
			Tags: []interface{}{
				"tag6", "val6",
			},
		},
	}

	m.Send(items)

	<-time.After(2 * time.Second)

	expected := make([]string, len(items))
	expected[0] = fmt.Sprintf("put %s %d %d tag1=val1 tag2=val2 tag3=val3\n", items[0].Metric, items[0].Timestamp, int(items[0].Value))
	expected[1] = fmt.Sprintf("put %s %d %d tag4=val4 tag5=val5\n", items[1].Metric, items[1].Timestamp, int(items[1].Value))
	expected[2] = fmt.Sprintf("put %s %d %d tag6=val6\n", items[2].Metric, items[2].Timestamp, int(items[2].Value))

	for i := 0; i < len(items); i++ {
		message := <-s.MessageChannel()
		assert.Equal(t, expected[i], message.Message, "expected same data")
	}
}
