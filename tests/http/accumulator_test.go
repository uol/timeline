package timeline_http_test

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

// createTimelineManagerA - creates a new timeline manager
func createTimelineManagerA(transportBufferSize int, manualMode bool) *timeline.Manager {

	rand.Seed(time.Now().Unix())

	backend := timeline.Backend{
		Host: testServerHost,
		Port: testServerPort,
	}

	transport := createHTTPTransport(transportBufferSize, time.Second, applicationJSON, nil)

	conf := &timeline.DataTransformerConfig{
		CycleDuration: funks.Duration{
			Duration: time.Millisecond * 100,
		},
		HashingAlgorithm: hashing.SHAKE128,
		HashSize:         12,
	}

	accumulator := timeline.NewAccumulator(conf)

	manager, err := timeline.NewManager(transport, nil, accumulator, &backend)
	if err != nil {
		panic(err)
	}

	err = manager.Start(manualMode)
	if err != nil {
		panic(err)
	}

	return manager
}

// storeNumber - stores a new number
func storeNumber(t *testing.T, ttl time.Duration, m *timeline.Manager, n *serializer.NumberPoint, customHash bool) (hash string, ok bool) {

	var err error

	if customHash {
		hash = genCustomHash()
		err = m.StoreHashedDataToAccumulateJSON(hash, ttl, numberPoint, toGenericParameters(n)...)
	} else {
		hash, err = m.StoreDataToAccumulateJSON(ttl, numberPoint, toGenericParameters(n)...)
	}

	if !assert.NoError(t, err, "error storing data") {
		return "", false
	}

	return hash, true
}

func genCustomHash() string {
	return "http-custom-hash-" + strconv.FormatInt(rand.Int63(), 10)
}

func testStorage(t *testing.T, customStorage, manualMode bool) {

	m := createTimelineManagerA(defaultTransportSize, manualMode)
	defer m.Shutdown()

	n := newNumberPoint(0)

	if hash, ok := storeNumber(t, time.Second, m, n, customStorage); !ok {
		return
	} else {
		assert.Truef(t, len(hash) > 0, "the generated hash length must be large than zero: %s", hash)
	}
}

// TestStorage - tests the hash storage operation
func TestStorage(t *testing.T) {

	for _, v := range manualModeArray {
		testStorage(t, false, v)
	}
}

// TestCustomHashStorage - tests the custom hash storage operation
func TestCustomHashStorage(t *testing.T) {

	for _, v := range manualModeArray {
		testStorage(t, true, v)
	}
}

// incAccumulatedData - increments the accumulated data N times
func incAccumulatedData(t *testing.T, m *timeline.Manager, hash string, times int) {

	wg := sync.WaitGroup{}

	for i := 0; i < times; i++ {
		wg.Add(1)
		go func() {
			err := m.IncrementAccumulatedData(hash)
			wg.Done()
			assert.NoError(t, err, "error incrementing accumulated data")
		}()
	}

	wg.Wait()
}

type accumParam struct {
	point      *serializer.NumberPoint
	number     uint64
	customHash bool
}

// testAdd - tests the add operation
func testAdd(t *testing.T, manualMode bool, params ...accumParam) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManagerA(defaultTransportSize, manualMode)
	defer m.Shutdown()

	expected := []*serializer.NumberPoint{}

	for i := 0; i < len(params); i++ {

		var hash string
		var ok bool

		if hash, ok = storeNumber(t, time.Second, m, params[i].point, params[i].customHash); !ok {
			return
		}

		params[i].point.Value = float64(params[i].number)

		incAccumulatedData(t, m, hash, int(params[i].number))

		expected = append(expected, params[i].point)
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

	requestData := gotesthttp.WaitForServerRequest(s, waitFor, 10*time.Second)
	testRequestData(t, requestData, expected, true, true, applicationJSON)
}

// TestAccumulateOneTypeOneTime - tests the add operation
func TestAccumulateOneTypeOneTime(t *testing.T) {

	for _, v := range manualModeArray {
		testAdd(t, v, accumParam{
			point:  newNumberPoint(0),
			number: 1,
		})
	}
}

// TestAccumulateOneTypeOneTimeCustomHash - tests the add operation using custom hash
func TestAccumulateOneTypeOneTimeCustomHash(t *testing.T) {

	for _, v := range manualModeArray {
		testAdd(t, v, accumParam{
			point:      newNumberPoint(0),
			number:     1,
			customHash: true,
		})
	}
}

// TestAccumulateOneTypeMultipleTimes - tests the add operation
func TestAccumulateOneTypeMultipleTimes(t *testing.T) {

	for _, v := range manualModeArray {
		testAdd(t, v, accumParam{
			point:  newNumberPoint(0),
			number: 100 + uint64(rand.Int63n(100)),
		})
	}
}

// TestAccumulateOneTypeMultipleTimesCustomHash - tests the add operation using custom hash
func TestAccumulateOneTypeMultipleTimesCustomHash(t *testing.T) {

	for _, v := range manualModeArray {
		testAdd(t, v, accumParam{
			point:      newNumberPoint(0),
			number:     100 + uint64(rand.Int63n(100)),
			customHash: true,
		})
	}
}

// buildAccumParameters - builds the accumulated parameters
func buildAccumParameters(initial, max int, times uint64, customHash bool) []accumParam {

	numParams := initial + rand.Intn(max)
	parameters := make([]accumParam, numParams)
	for i := 0; i < numParams; i++ {
		n := newNumberPoint(0)
		n.Metric = n.Metric + strconv.Itoa(i)
		parameters[i] = accumParam{
			point:  n,
			number: times,
		}

		if customHash {
			parameters[i].customHash = true
		}
	}

	return parameters
}

// TestAccumulateMultipleTypesOneTime - tests the add operation
func TestAccumulateMultipleTypesOneTime(t *testing.T) {

	for _, v := range manualModeArray {
		testAdd(t, v, buildAccumParameters(1, 5, 1, false)...)
	}
}

// TestAccumulateMultipleTypesOneTimeCustomHash - tests the add operation using custom hash
func TestAccumulateMultipleTypesOneTimeCustomHash(t *testing.T) {

	for _, v := range manualModeArray {
		testAdd(t, v, buildAccumParameters(1, 5, 1, true)...)
	}
}

// TestAccumulateMultipleTypesMultipleTimes - tests the add operation
func TestAccumulateMultipleTypesMultipleTimes(t *testing.T) {

	for _, v := range manualModeArray {
		testAdd(t, v, buildAccumParameters(1, 5, 2+uint64(rand.Int63n(5)), false)...)
	}
}

// TestAccumulateMultipleTypesMultipleTimesCustomHash - tests the add operation using custom hash
func TestAccumulateMultipleTypesMultipleTimesCustomHash(t *testing.T) {

	for _, v := range manualModeArray {
		testAdd(t, v, buildAccumParameters(1, 5, 2+uint64(rand.Int63n(5)), true)...)
	}
}

func testDataTTL(t *testing.T, customHash, manualMode bool) {
	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManagerA(defaultTransportSize, manualMode)
	defer m.Shutdown()

	n1 := newNumberPoint(0)
	n2 := newNumberPoint(0)
	n2.Metric = "updated"

	var hash1, hash2 string
	var ok bool

	if hash1, ok = storeNumber(t, time.Second, m, n1, customHash); !ok {
		return
	}

	if hash2, ok = storeNumber(t, time.Second, m, n2, customHash); !ok {
		return
	}

	err := m.IncrementAccumulatedData(hash1)
	if !assert.NoError(t, err, "error was not expected incrementing hash1 (manual mode = %t)", manualMode) {
		return
	}

	for i := 0; i < 3; i++ {

		err = m.IncrementAccumulatedData(hash2)
		if !assert.NoErrorf(t, err, "error was not expected incrementing hash2 (manual mode = %t)", manualMode) {
			return
		}

		<-time.After(900 * time.Millisecond)
		if manualMode {
			m.ProcessCycle()
		}
	}

	err = m.IncrementAccumulatedData(hash1)
	if !assert.Equal(t, timeline.ErrNotStored, err, "expected hash1 to be expired") {
		return
	}

	err = m.IncrementAccumulatedData(hash2)
	if !assert.NoError(t, err, "error was not expected incrementing hash2") {
		return
	}

	<-time.After(3 * time.Second)
}

// TestDataTTL - tests the data TTL
func TestDataTTL(t *testing.T) {

	for _, v := range manualModeArray {
		testDataTTL(t, false, v)
	}
}

// TestDataTTLCustomHash - tests the data TTL using custom hash
func TestDataTTLCustomHash(t *testing.T) {

	for _, v := range manualModeArray {
		testDataTTL(t, true, v)
	}
}

func testDataNoTTL(t *testing.T, customHash, manualMode bool) {

	s := createTimeseriesBackend()
	defer s.Close()

	m := createTimelineManagerA(defaultTransportSize, manualMode)
	defer m.Shutdown()

	n1 := newNumberPoint(0)
	n2 := newNumberPoint(0)
	n2.Metric = "updated"

	var hash1, hash2 string
	var ok bool

	if hash1, ok = storeNumber(t, 0, m, n1, customHash); !ok {
		return
	}

	if hash2, ok = storeNumber(t, time.Second, m, n2, customHash); !ok {
		return
	}

	err := m.IncrementAccumulatedData(hash1)
	if !assert.NoError(t, err, "error was not expected incrementing hash1") {
		return
	}

	<-time.After(2 * time.Second)
	if manualMode {
		m.ProcessCycle()
		m.SendData()
	}

	err = m.IncrementAccumulatedData(hash2)
	if !assert.Equal(t, timeline.ErrNotStored, err, "expected hash2 to be expired") {
		return
	}

	err = m.IncrementAccumulatedData(hash1)
	if !assert.NoError(t, err, "error was not expected incrementing hash1") {
		return
	}
}

// TestDataNoTTL - tests the data without TTL
func TestDataNoTTL(t *testing.T) {

	for _, v := range manualModeArray {
		testDataNoTTL(t, false, v)
	}
}

// TestDataNoTTLCustomHash - tests the data without TTL using custom hash
func TestDataNoTTLCustomHash(t *testing.T) {

	for _, v := range manualModeArray {
		testDataNoTTL(t, true, v)
	}
}
