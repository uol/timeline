package timeline_udp_test

import (
	"time"

	"github.com/uol/funks"
	"github.com/uol/gotest/tcpudp"
	jsonserializer "github.com/uol/serializer/json"
	serializer "github.com/uol/serializer/serializer"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

var (
	defaultConf tcpudp.ServerConfiguration = tcpudp.ServerConfiguration{
		Host:               "localhost",
		MessageChannelSize: 10,
		ReadBufferSize:     512,
	}
)

type contentType string

const (
	timeBetweenBatches   int    = 50
	defaultTransportSize int    = 50
	numberPoint          string = "numberJSON"
)

// createUDPTransport - creates the udp transport with custom batch send interval
func createUDPTransport(transportBufferSize int, batchSendInterval time.Duration, s serializer.Serializer) *timeline.UDPTransport {

	transportConf := timeline.UDPTransportConfig{
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
		TCPUDPTransportConfig: timeline.TCPUDPTransportConfig{
			ReconnectionTimeout:    funks.Duration{Duration: 1 * time.Second},
			MaxReconnectionRetries: 3,
		},
		CustomSerializerConfig: timeline.CustomSerializerConfig{
			TimestampProperty: "timestamp",
			ValueProperty:     "value",
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

		s = jsons
	}

	transport, err := timeline.NewUDPTransport(&transportConf, s)
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

type ByDate []tcpudp.MessageData

func (a ByDate) Len() int           { return len(a) }
func (a ByDate) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDate) Less(i, j int) bool { return a[i].Date.Unix() < a[j].Date.Unix() }
