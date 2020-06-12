package timeline

import (
	"github.com/uol/funks"
	"github.com/uol/hashing"
)

/**
* All exported structs used by the timeline library.
* @author rnojiri
**/

// Backend - the destiny opentsdb backend
type Backend struct {
	Host string `json:"host,omitempty"`
	Port int    `json:"port,omitempty"`
}

// DataTransformerConfig - flattener configuration
type DataTransformerConfig struct {
	CycleDuration    funks.Duration    `json:"cycleDuration,omitempty"`
	HashingAlgorithm hashing.Algorithm `json:"hashingAlgorithm,omitempty"`
	HashSize         int               `json:"hashSize,omitempty"`
	isSHAKE          bool
}

// DefaultTransportConfig - the default fields used by the transport configuration
type DefaultTransportConfig struct {
	TransportBufferSize  int            `json:"transportBufferSize,omitempty"`
	BatchSendInterval    funks.Duration `json:"batchSendInterval,omitempty"`
	RequestTimeout       funks.Duration `json:"requestTimeout,omitempty"`
	SerializerBufferSize int            `json:"serializerBufferSize,omitempty"`
	DebugInput           bool           `json:"debugInput,omitempty"`
	DebugOutput          bool           `json:"debugOutput,omitempty"`
	TimeBetweenBatches   funks.Duration `json:"timeBetweenBatches,omitempty"`
	PrintStackOnError    bool           `json:"printStackOnError,omitempty"`
}

// HTTPTransportConfig - has all HTTP event manager configurations
type HTTPTransportConfig struct {
	DefaultTransportConfig
	ServiceEndpoint        string `json:"serviceEndpoint,omitempty"`
	Method                 string `json:"method,omitempty"`
	ExpectedResponseStatus int    `json:"expectedResponseStatus,omitempty"`
	TimestampProperty      string `json:"timestampProperty,omitempty"`
	ValueProperty          string `json:"valueProperty,omitempty"`
}

// OpenTSDBTransportConfig - has all openTSDB event manager configurations
type OpenTSDBTransportConfig struct {
	DefaultTransportConfig
	ReadBufferSize         int            `json:"readBufferSize,omitempty"`
	MaxReadTimeout         funks.Duration `json:"maxReadTimeout,omitempty"`
	ReconnectionTimeout    funks.Duration `json:"reconnectionTimeout,omitempty"`
	MaxReconnectionRetries int            `json:"maxReconnectionRetries,omitempty"`
	DisconnectAfterWrites  bool           `json:"disconnectAfterWrites,omitempty"`
}
