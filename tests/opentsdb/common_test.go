package timeline_opentsdb_test

import (
	"time"

	"github.com/uol/funks"
	"github.com/uol/gotest/tcpudp"
	serializer "github.com/uol/serializer/opentsdb"
	"github.com/uol/timeline"
)

/**
* The timeline library tests.
* @author rnojiri
**/

var (
	defaultConf tcpudp.TCPConfiguration = tcpudp.TCPConfiguration{
		ServerConfiguration: tcpudp.ServerConfiguration{
			Host:               "localhost",
			MessageChannelSize: 10,
			ReadBufferSize:     512,
		},
		ReadTimeout: 3 * time.Second,
	}
	manualModeArray = []bool{false, true}
)

const (
	timeBetweenBatches   int = 50
	defaultTransportSize int = 50
)

// createOpenTSDBTransport - creates the opentsdb transport
func createOpenTSDBTransport(transportBufferSize int, batchSendInterval time.Duration) *timeline.OpenTSDBTransport {

	transportConf := timeline.OpenTSDBTransportConfig{
		DefaultTransportConfig: timeline.DefaultTransportConfig{
			BatchSendInterval: funks.Duration{
				Duration: batchSendInterval,
			},
			RequestTimeout: funks.Duration{
				Duration: time.Second,
			},
			TransportBufferSize:  transportBufferSize,
			SerializerBufferSize: 1024,
			TimeBetweenBatches:   funks.Duration{Duration: time.Duration(timeBetweenBatches) * time.Millisecond},
		},
		ReadBufferSize: 64,

		TCPUDPTransportConfig: timeline.TCPUDPTransportConfig{
			ReconnectionTimeout: funks.Duration{
				Duration: time.Second,
			},
			DisconnectAfterWrites:  false,
			MaxReconnectionRetries: 3,
		},
		MaxReadTimeout: funks.Duration{
			Duration: time.Second,
		},
	}

	transport, err := timeline.NewOpenTSDBTransport(&transportConf)
	if err != nil {
		panic(err)
	}

	return transport
}

// newArrayItem - creates a new array item
func newArrayItem(metric string, value float64) serializer.ArrayItem {

	return serializer.ArrayItem{
		Metric:    metric,
		Timestamp: time.Now().Unix(),
		Value:     value,
		Tags: []interface{}{
			"tagk1", "tagv1",
			"tagk2", "tagv2",
			"tagk3", "tagv3",
		},
	}
}
