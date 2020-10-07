package config_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	"github.com/uol/gofiles"
	"github.com/uol/hashing"
	"github.com/uol/timeline"
)

// MainConf - sample configuration
type MainConf struct {
	Backend           *timeline.Backend                 `json:"backend,omitempty"`
	DataTransformer   *timeline.DataTransformerConfig   `json:"dataTransformer,omitempty"`
	HTTPTransport     *timeline.HTTPTransportConfig     `json:"httpTransport,omitempty"`
	OpenTSDBTransport *timeline.OpenTSDBTransportConfig `json:"openTSDBTransport,omitempty"`
}

var expected = MainConf{
	OpenTSDBTransport: &timeline.OpenTSDBTransportConfig{
		ReadBufferSize: 64,
		MaxReadTimeout: *funks.ForceNewStringDuration("100ms"),
		TCPUDPTransportConfig: timeline.TCPUDPTransportConfig{
			ReconnectionTimeout:    *funks.ForceNewStringDuration("3s"),
			MaxReconnectionRetries: 5,
			DisconnectAfterWrites:  true,
		},
		DefaultTransportConfig: timeline.DefaultTransportConfig{
			TransportBufferSize:  1024,
			BatchSendInterval:    *funks.ForceNewStringDuration("30s"),
			RequestTimeout:       *funks.ForceNewStringDuration("5s"),
			SerializerBufferSize: 2048,
			DebugInput:           false,
			DebugOutput:          true,
			TimeBetweenBatches:   *funks.ForceNewStringDuration("10ms"),
			PrintStackOnError:    true,
		},
	},
	HTTPTransport: &timeline.HTTPTransportConfig{
		ServiceEndpoint:        "/api/put",
		Method:                 "POST",
		ExpectedResponseStatus: 204,
		CustomSerializerConfig: timeline.CustomSerializerConfig{
			TimestampProperty: "timestamp",
			ValueProperty:     "value",
		},
		DefaultTransportConfig: timeline.DefaultTransportConfig{
			TransportBufferSize:  64,
			BatchSendInterval:    *funks.ForceNewStringDuration("1m"),
			RequestTimeout:       *funks.ForceNewStringDuration("60s"),
			SerializerBufferSize: 512,
			DebugInput:           true,
			DebugOutput:          false,
			TimeBetweenBatches:   *funks.ForceNewStringDuration("5s"),
			PrintStackOnError:    false,
		},
	},
	DataTransformer: &timeline.DataTransformerConfig{
		HashingAlgorithm: hashing.SHAKE128,
		HashSize:         7,
		CycleDuration:    *funks.ForceNewStringDuration("15s"),
	},
	Backend: &timeline.Backend{
		Host: "host1",
		Port: 8123,
	},
}

// TestJsonConf - tests unmarshaling a JSON from file
func TestJsonConf(t *testing.T) {

	jsonBytes, err := gofiles.ReadFileBytes("./config.json")
	if !assert.NoError(t, err, "expected no error loading file") {
		return
	}

	mc := MainConf{}

	err = json.Unmarshal(jsonBytes, &mc)
	if !assert.NoError(t, err, "expected no error unmarshalling json") {
		return
	}

	assert.True(t, reflect.DeepEqual(expected, mc), "expected equal content")
}

// TestTOMLConf - tests unmarshaling a TOML from file
func TestTOMLConf(t *testing.T) {

	jsonBytes, err := gofiles.ReadFileBytes("./config.toml")
	if !assert.NoError(t, err, "expected no error loading file") {
		return
	}

	mc := MainConf{}

	err = toml.Unmarshal(jsonBytes, &mc)
	if !assert.NoError(t, err, "expected no error unmarshalling toml") {
		return
	}

	assert.True(t, reflect.DeepEqual(expected, mc), "expected equal content")
}
