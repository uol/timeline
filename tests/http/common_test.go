package timeline_http_test

import (
	"net/http"
	"time"

	"github.com/uol/funks"
	gotesthttp "github.com/uol/gotest/http"
	jsonserializer "github.com/uol/serializer/json"
	serializer "github.com/uol/serializer/serializer"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

const (
	defaultTransportSize int    = 100
	testServerHost       string = "localhost"
	testServerPort       int    = 18080
	channelSize          int    = 10
)

var manualModeArray = []bool{false, true}

// createTimeseriesBackend - creates a new test server simulating a timeseries backend
func createTimeseriesBackend() *gotesthttp.Server {

	headers := http.Header{}
	headers.Add("Content-type", "application/json")

	responses := gotesthttp.ResponseData{
		RequestData: gotesthttp.RequestData{
			URI:     "/api/put",
			Method:  "PUT",
			Headers: headers,
		},
		Status: 201,
	}

	return gotesthttp.NewServer(
		&gotesthttp.Configuration{
			Host:        testServerHost,
			Port:        testServerPort,
			ChannelSize: channelSize,
			Responses: map[string][]gotesthttp.ResponseData{
				"default": {responses},
			},
		},
	)
}

type contentType string

const (
	numberPoint         string      = "numberJSON"
	textPoint           string      = "textJSON"
	applicationJSON     contentType = "application/json"
	applicationCustom   contentType = "application/custom"
	applicationOpenTSDB contentType = "application/opentsdb"
)

// createHTTPTransport - creates the http transport with custom batch send interval
func createHTTPTransport(transportBufferSize int, batchSendInterval time.Duration, ctype contentType, s serializer.Serializer) *timeline.HTTPTransport {

	transportConf := timeline.HTTPTransportConfig{
		DefaultTransportConfig: timeline.DefaultTransportConfig{
			RequestTimeout: funks.Duration{
				Duration: time.Second,
			},
			BatchSendInterval: funks.Duration{
				Duration: batchSendInterval,
			},
			TransportBufferSize:  transportBufferSize,
			SerializerBufferSize: 256,
			TimeBetweenBatches:   funks.Duration{Duration: 100 * time.Millisecond},
		},
		ServiceEndpoint:        "/api/put",
		Method:                 "PUT",
		ExpectedResponseStatus: 201,
		CustomSerializerConfig: timeline.CustomSerializerConfig{
			TimestampProperty: "timestamp",
			ValueProperty:     "value",
		},
		Headers: map[string]string{
			"content-type": string(ctype),
		},
	}

	if s == nil {
		jsons := jsonserializer.New(256)

		err := jsons.Add(
			numberPoint,
			jsonserializer.NumberPoint{},
			"metric",
			"value",
			"timestamp",
			"tags",
		)

		if err != nil {
			panic(err)
		}

		err = jsons.Add(
			textPoint,
			jsonserializer.TextPoint{},
			"metric",
			"text",
			"timestamp",
			"tags",
		)

		if err != nil {
			panic(err)
		}

		s = jsons
	}

	transport, err := timeline.NewHTTPTransport(&transportConf, s)
	if err != nil {
		panic(err)
	}

	return transport
}

// newNumberPoint - creates a new number point
func newNumberPoint(value float64) *jsonserializer.NumberPoint {

	return &jsonserializer.NumberPoint{
		Point: jsonserializer.Point{
			Metric:    "number-metric",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"type":      "number",
				"customTag": "number-test",
			},
		},
		Value: value,
	}
}

// newTextPoint - creates a new text point
func newTextPoint(text string) *jsonserializer.TextPoint {

	return &jsonserializer.TextPoint{
		Point: jsonserializer.Point{
			Metric:    "text-metric",
			Timestamp: time.Now().Unix(),
			Tags: map[string]string{
				"type":      "text",
				"customTag": "text-test",
			},
		},
		Text: text,
	}
}

type ByMetric []jsonserializer.NumberPoint

func (a ByMetric) Len() int           { return len(a) }
func (a ByMetric) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByMetric) Less(i, j int) bool { return a[i].Metric < a[j].Metric }

type ByMetricP []*jsonserializer.NumberPoint

func (a ByMetricP) Len() int           { return len(a) }
func (a ByMetricP) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByMetricP) Less(i, j int) bool { return a[i].Metric < a[j].Metric }
