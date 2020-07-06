package timeline_http_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	gotesthttp "github.com/uol/gotest/http"
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
func createTimelineManager(start bool, transportSize int, batchSendInterval time.Duration, ctype contentType, s serializer.Serializer) *timeline.Manager {

	backend := timeline.Backend{
		Host: testServerHost,
		Port: testServerPort,
	}

	transport := createHTTPTransport(transportSize, batchSendInterval, ctype, s)

	manager, err := timeline.NewManager(transport, nil, nil, &backend)
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

// testSerializeCompareNumber - compares a serialized json and a json struct
func testSerializeCompareNumber(t *testing.T, text string, expected interface{}, ignoreTimestamp bool) bool {

	var actual []jsonserializer.NumberPoint
	err := json.Unmarshal([]byte(text), &actual)
	if !assert.Nil(t, err, "error unmarshalling to number point") {
		return false
	}

	return testNumberPoint(t, expected, actual, ignoreTimestamp)
}

// testSerializeCompareText - compares a serialized json and a json struct
func testSerializeCompareText(t *testing.T, text string, expected interface{}) bool {

	var actual []jsonserializer.TextPoint
	err := json.Unmarshal([]byte(text), &actual)
	if !assert.Nil(t, err, "error unmarshalling to text point") {
		return false
	}

	return testTextPoint(t, expected, actual)
}

// testRequestMetadata - tests only the request metadata
func testRequestMetadata(t *testing.T, requestData *gotesthttp.RequestData, expectedContentType contentType) bool {

	result := true

	result = result && assert.NotNil(t, requestData, "request data cannot be null")
	result = result && assert.Equal(t, "/api/put", requestData.URI, "expected /api/put as endpoint")
	result = result && assert.Equal(t, "PUT", requestData.Method, "expected PUT as method")
	result = result && assert.Equal(t, string(expectedContentType), requestData.Headers.Get("Content-type"), "expected aplication/json as content-type header")

	return result
}

// testRequestData - tests the request data
func testRequestData(t *testing.T, requestData *gotesthttp.RequestData, expected interface{}, isNumber, ignoreTimestamp bool, expectedContentType contentType) bool {

	result := testRequestMetadata(t, requestData, expectedContentType)

	if result {

		if isNumber {
			return testSerializeCompareNumber(t, requestData.Body, expected, ignoreTimestamp)
		}

		return testSerializeCompareText(t, requestData.Body, expected)
	}

	return result
}

// testTextPoint - compares two points
func testTextPoint(t *testing.T, expected interface{}, actual interface{}) bool {

	if !assert.NotNil(t, expected, "expected value cannot be null") {
		return false
	}

	if !assert.NotNil(t, actual, "actual value cannot be null") {
		return false
	}

	expectedNumbers, ok := expected.([]*jsonserializer.TextPoint)
	if !ok && !assert.True(t, ok, "expected value must be a text point type") {
		return false
	}

	actualNumbers, ok := actual.([]jsonserializer.TextPoint)
	if !ok && !assert.True(t, ok, "actual value must be a text point type") {
		return false
	}

	if !assert.Len(t, actualNumbers, len(expectedNumbers), "expected %d text points", len(expectedNumbers)) {
		return false
	}

	result := true

	for i := 0; i < len(expectedNumbers); i++ {

		result = result && assert.Equal(t, expectedNumbers[i].Metric, actualNumbers[i].Metric, "text point's metric differs")
		result = result && assert.Equal(t, expectedNumbers[i].Timestamp, actualNumbers[i].Timestamp, "text point's timestamp differs")
		result = result && assert.True(t, reflect.DeepEqual(expectedNumbers[i].Tags, actualNumbers[i].Tags), "text point's tags differs")
		result = result && assert.Equal(t, expectedNumbers[i].Text, actualNumbers[i].Text, "text point's value differs")

		if !result {
			return false
		}
	}

	return result
}

// testNumberPoint - compares two points
func testNumberPoint(t *testing.T, expected interface{}, actual interface{}, ignoreTimestamp bool) bool {

	if !assert.NotNil(t, expected, "expected value cannot be null") {
		return false
	}

	if !assert.NotNil(t, actual, "actual value cannot be null") {
		return false
	}

	expectedNumbers, ok := expected.([]*jsonserializer.NumberPoint)
	if !ok && !assert.True(t, ok, "expected value must be a number point type") {
		return false
	}

	actualNumbers, ok := actual.([]jsonserializer.NumberPoint)
	if !ok && !assert.True(t, ok, "actual value must be a number point type") {
		return false
	}

	sort.Sort(ByMetricP(expectedNumbers))
	sort.Sort(ByMetric(actualNumbers))

	if !assert.Len(t, actualNumbers, len(expectedNumbers), "expected %d number points", len(expectedNumbers)) {
		return false
	}

	result := true

	for i := 0; i < len(expectedNumbers); i++ {

		result = result && assert.Equal(t, expectedNumbers[i].Metric, actualNumbers[i].Metric, "number point's metric differs")

		if !ignoreTimestamp {
			result = result && assert.Equal(t, expectedNumbers[i].Timestamp, actualNumbers[i].Timestamp, "number point's timestamp differs")
		} else {
			result = result && assert.Greater(t, expectedNumbers[i].Timestamp, time.Now().Unix()-(60*1000), "expected a valid timestamp")
			result = result && assert.Greater(t, actualNumbers[i].Timestamp, time.Now().Unix()-(60*1000), "expected a valid timestamp")
		}

		result = result && assert.True(t, reflect.DeepEqual(expectedNumbers[i].Tags, actualNumbers[i].Tags), "number point's tags differs")
		result = result && assert.Equal(t, expectedNumbers[i].Value, actualNumbers[i].Value, "number point's value differs")

		if !result {
			return false
		}
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

// toGenericParametersT - converts a number point to generic parameters
func toGenericParametersT(point *jsonserializer.TextPoint) []interface{} {

	return []interface{}{
		"metric", point.Metric,
		"timestamp", point.Timestamp,
		"text", point.Text,
		"tags", point.Tags,
	}
}

// TestSendNumber - tests when the lib fires a event
func TestSendNumber(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true, defaultTransportSize, time.Second, applicationJSON, nil)
	defer m.Shutdown()

	number := newNumberPoint(1)

	err := m.SendJSON(numberPoint, toGenericParametersN(number)...)
	if !assert.NoError(t, err, "no error expected when sending number") {
		return
	}

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, []*jsonserializer.NumberPoint{number}, true, false, applicationJSON)
}

// TestSendText - tests when the lib fires a event
func TestSendText(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true, defaultTransportSize, time.Second, applicationJSON, nil)
	defer m.Shutdown()

	text := newTextPoint("test")

	err := m.SendJSON(textPoint, toGenericParametersT(text)...)
	assert.NoError(t, err, "no error expected when sending text")

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, []*jsonserializer.TextPoint{text}, false, false, applicationJSON)
}

// TestSendNumberArray - tests when the lib fires a event
func TestSendNumberArray(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true, defaultTransportSize, time.Second, applicationJSON, nil)
	defer m.Shutdown()

	numbers := []*jsonserializer.NumberPoint{newNumberPoint(1), newNumberPoint(2), newNumberPoint(3)}

	for _, n := range numbers {
		err := m.SendJSON(numberPoint, toGenericParametersN(n)...)
		assert.NoError(t, err, "no error expected when sending number")
	}

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, numbers, true, false, applicationJSON)
}

// TestSendTextArray - tests when the lib fires a event
func TestSendTextArray(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManager(true, defaultTransportSize, time.Second, applicationJSON, nil)
	defer m.Shutdown()

	texts := []*jsonserializer.TextPoint{newTextPoint("1"), newTextPoint("2"), newTextPoint("3")}

	for _, n := range texts {
		err := m.SendJSON(textPoint, toGenericParametersT(n)...)
		assert.NoError(t, err, "no error expected when sending text")
	}

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, texts, false, false, applicationJSON)
}

// TestSendCustomNumber - tests configuring the json variables
func TestSendCustomNumber(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	const custom = "customPoint"

	cs := jsonserializer.New(128)

	number := newNumberPoint(1.0)

	// only value is variable
	err := cs.Add(custom, *number, "value")
	if !assert.NoError(t, err, "no error adding custom configuration") {
		return
	}

	m := createTimelineManager(false, defaultTransportSize, time.Second, applicationCustom, cs)
	defer m.Shutdown()

	m.Start()

	err = m.SendJSON(custom, "value", 5.0)
	if !assert.NoError(t, err, "no error expected when sending number") {
		return
	}

	number.Value = 5.0

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, []*jsonserializer.NumberPoint{number}, true, false, applicationCustom)
}

// TestSendCustomText - tests configuring the json variables
func TestSendCustomText(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	cs := jsonserializer.New(128)

	text := newTextPoint("woohoo")

	const custom = "customPoint"

	// only value is variable
	err := cs.Add(custom, *text, "text")
	if !assert.NoError(t, err, "no error adding custom configuration") {
		return
	}

	m := createTimelineManager(false, defaultTransportSize, time.Second, applicationCustom, cs)
	defer m.Shutdown()

	m.Start()

	err = m.SendJSON(custom, "text", "modified")
	if !assert.NoError(t, err, "no error expected when sending text") {
		return
	}

	text.Text = "modified"

	<-time.After(2 * time.Second)

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)
	testRequestData(t, requestData, []*jsonserializer.TextPoint{text}, false, false, applicationCustom)
}

// TestNumberSerialization - tests configuring the json variables
func TestNumberSerialization(t *testing.T) {

	m := createTimelineManager(false, defaultTransportSize, time.Second, applicationJSON, nil)
	defer m.Shutdown()

	number := newNumberPoint(15)

	serialized, err := m.SerializeJSON(numberPoint, toGenericParametersN(number)...)
	if !assert.NoError(t, err, "no error expected when serializing number") {
		return
	}

	testSerializeCompareNumber(t, fmt.Sprintf("[%s]", serialized), []*jsonserializer.NumberPoint{number}, false)
}

// TestTextSerialization - tests configuring the json variables
func TestTextSerialization(t *testing.T) {

	m := createTimelineManager(false, defaultTransportSize, time.Second, applicationJSON, nil)
	defer m.Shutdown()

	text := newTextPoint("serialization")

	serialized, err := m.SerializeJSON(textPoint, toGenericParametersT(text)...)
	if !assert.NoError(t, err, "no error expected when serializing text") {
		return
	}

	testSerializeCompareText(t, fmt.Sprintf("[%s]", serialized), []*jsonserializer.TextPoint{text})
}

// TestExceedingBufferSize - tests when the buffer exceeds its size
func TestExceedingBufferSize(t *testing.T) {

	s := createTimeseriesBackend()
	defer s.Close()

	bufferSize := 2
	numPoints := 6
	numRequests := numPoints / bufferSize
	requests := make([]*gotesthttp.RequestData, numRequests)
	batchSendInterval := 5 * time.Second
	wc := sync.WaitGroup{}
	wc.Add(numRequests)

	go func() {
		for i := 0; i < numRequests; i++ {
			requests[i] = <-s.RequestChannel()
			wc.Done()
		}
	}()

	m := createTimelineManager(true, bufferSize, batchSendInterval, applicationJSON, nil)
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

	s := createTimeseriesBackend()
	defer s.Close()

	cs := otsdbserializer.New(128)

	m := createTimelineManager(false, defaultTransportSize, time.Second, applicationOpenTSDB, cs)
	defer m.Shutdown()

	m.Start()

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

	requestData := gotesthttp.WaitForServerRequest(s, time.Second, 10*time.Second)

	result := testRequestMetadata(t, requestData, applicationOpenTSDB)
	if !result {
		return
	}

	assert.Equal(t,
		fmt.Sprintf(
			strings.ReplaceAll(`put %s %d %d tag1=val1 tag2=val2 tag3=val3
			put %s %d %d tag4=val4 tag5=val5
			put %s %d %d tag6=val6
			`, "\t", ""),
			items[0].Metric, items[0].Timestamp, int(items[0].Value),
			items[1].Metric, items[1].Timestamp, int(items[1].Value),
			items[2].Metric, items[2].Timestamp, int(items[2].Value),
		),
		requestData.Body,
		"expected same item",
	)
}
