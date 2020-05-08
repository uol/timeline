package timeline

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/uol/logh"
)

/**
* The transport interface to be implemented.
* @author rnojiri
**/

type transportType uint8

const (
	typeHTTP transportType = 0
	typeOpenTSDB
)

// Transport - the implementation type to send a event
type Transport interface {

	// Send - send a new point
	DataChannel() chan<- interface{}

	// ConfigureBackend - configures the backend
	ConfigureBackend(backend *Backend) error

	// TransferData - transfers the data using this specific implementation
	TransferData(dataList []interface{}) error

	// Start - starts this transport
	Start() error

	// Close - closes this transport
	Close()

	// MatchType - checks if this transport implementation matches the given type
	MatchType(tt transportType) bool

	// Serialize - renders the text using the configured serializer
	Serialize(item interface{}) (string, error)

	// DataChannelItemToFlattenerPoint - converts the data channel item to the flattened point
	DataChannelItemToFlattenerPoint(configuration *DataTransformerConf, item interface{}, operation FlatOperation) (Hashable, error)

	// FlattenerPointToDataChannelItem - converts the flattened point to the data channel item
	FlattenerPointToDataChannelItem(item *FlattenerPoint) (interface{}, error)

	// DataChannelItemToAccumulatedData - converts the data channel item to the accumulated data
	DataChannelItemToAccumulatedData(configuration *DataTransformerConf, item interface{}, calculateHash bool) (Hashable, error)

	// AccumulatedDataToDataChannelItem - converts the accumulated data to the data channel item
	AccumulatedDataToDataChannelItem(item *AccumulatedData) (interface{}, error)
}

// Hashable - a struct with hash function
type Hashable interface {

	// GetHash - return this instance hash
	GetHash() string
}

// transportCore - implements a default transport behaviour
type transportCore struct {
	transport            Transport
	batchSendInterval    time.Duration
	pointChannel         chan interface{}
	loggers              *logh.ContextualLogger
	started              bool
	isReleasingBuffer    uint32
	defaultConfiguration *DefaultTransportConfiguration
	pointBuffer          []interface{}
}

// DefaultTransportConfiguration - the default fields used by the transport configuration
type DefaultTransportConfiguration struct {
	TransportBufferSize  int
	BatchSendInterval    time.Duration
	RequestTimeout       time.Duration
	SerializerBufferSize int
	DebugInput           bool
	DebugOutput          bool
}

// Validate - validates the default itens from the configuration
func (c *DefaultTransportConfiguration) Validate() error {

	if c.TransportBufferSize <= 0 {
		return fmt.Errorf("invalid buffer size: %d", c.TransportBufferSize)
	}

	if c.SerializerBufferSize <= 0 {
		return fmt.Errorf("invalid serializer buffer size: %d", c.SerializerBufferSize)
	}

	if c.BatchSendInterval.Seconds() <= 0 {
		return fmt.Errorf("invalid batch send interval: %s", c.BatchSendInterval)
	}

	if c.RequestTimeout.Seconds() <= 0 {
		return fmt.Errorf("invalid request timeout interval: %s", c.RequestTimeout)
	}

	return nil
}

// Start - starts the transport
func (t *transportCore) Start() error {

	if t.started {
		return nil
	}

	if logh.InfoEnabled {
		t.loggers.Info().Msg("starting transport...")
	}

	t.createPointChannelBuffer()

	t.started = true

	go t.transferDataLoop()

	return nil
}

// createPointChannelBuffer - creates the point channel buffer and it's loop
func (t *transportCore) createPointChannelBuffer() {

	atomic.StoreUint32(&t.isReleasingBuffer, 0)
	t.pointChannel = make(chan interface{})
	t.pointBuffer = []interface{}{}

	if logh.InfoEnabled {
		t.loggers.Info().Msg("starting the point channel buffer loop...")
	}

	go func() {
		for {
			item, ok := <-t.pointChannel
			if !ok {
				if logh.InfoEnabled {
					t.loggers.Info().Msg("breaking the point channel buffer loop...")
				}

				return
			}

			if len(t.pointBuffer) >= t.defaultConfiguration.TransportBufferSize {

				if logh.WarnEnabled {
					t.loggers.Warn().Msg("point buffer is full! (check the transport buffer size configuration)")
				}

				t.releaseBuffer()
			}

			t.pointBuffer = append(t.pointBuffer, item)
		}
	}()
}

// transferDataLoop - transfers the data to the backend throught this transport
func (t *transportCore) transferDataLoop() {

	if logh.InfoEnabled {
		t.loggers.Info().Msg("initializing transfer data loop...")
	}

	for {
		<-time.After(t.batchSendInterval)

		t.releaseBuffer()
	}
}

// releaseBuffer - releases the point buffer
func (t *transportCore) releaseBuffer() {

	if atomic.LoadUint32(&t.isReleasingBuffer) == 1 {
		if logh.DebugEnabled {
			t.loggers.Debug().Msg("already releasing point buffer, skipping another call...")
		}
		return
	}

	atomic.StoreUint32(&t.isReleasingBuffer, 1)

	numPoints := len(t.pointBuffer)

	if numPoints == 0 {
		if logh.DebugEnabled {
			t.loggers.Debug().Msg("buffer is empty, no data will be send")
		}
		atomic.StoreUint32(&t.isReleasingBuffer, 0)
		return
	}

	if logh.InfoEnabled {
		t.loggers.Info().Msg(fmt.Sprintf("sending a batch of %d points...", numPoints))
	}

	err := t.transport.TransferData(t.pointBuffer)
	if err != nil {
		if logh.ErrorEnabled {
			t.loggers.Error().Err(err).Msg("error transferring data")
		}
	} else {
		if logh.InfoEnabled {
			t.loggers.Info().Msgf("batch of %d points were sent!", numPoints)
		}
	}

	t.pointBuffer = []interface{}{}

	atomic.StoreUint32(&t.isReleasingBuffer, 0)

	return
}

// Close - closes the transport
func (t *transportCore) Close() {

	if logh.InfoEnabled {
		t.loggers.Info().Msg("closing...")
	}

	if t.pointChannel != nil {
		close(t.pointChannel)
	}

	t.started = false
}

// debugInput - print the incoming points if enabled
func (t *transportCore) debugInput(array []interface{}) {

	if t.defaultConfiguration.DebugInput && logh.DebugEnabled {

		for _, item := range array {

			t.loggers.Debug().Str("point", "input").Msgf("%+v", item)
		}
	}
}

// debugOutput - print the outcoming points if enabled
func (t *transportCore) debugOutput(serialized string) {

	if t.defaultConfiguration.DebugOutput && logh.DebugEnabled {

		t.loggers.Debug().Str("point", "output").Msgf("--content-start--\n%s\n--content-end--\n", serialized)
	}
}
