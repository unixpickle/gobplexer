package gobplexer

import (
	"encoding/gob"
	"errors"
	"io"
	"sync"
)

const (
	multiplexerBuffer       = 10
	multiplexerAcceptBuffer = 5
)

// A Listener can accept new connections.
type Listener interface {
	// Accept accepts the next connection.
	Accept() (Connection, error)

	// Close closes the Listener.
	// Depending on the implementation, this may also
	// close all previously accepted connections.
	Close() error
}

// MultiplexListener creates a Listener that multiplexes
// a Connection into multiple incoming connections.
// The other end of the Connection should use
// MultiplexConnector.
func MultiplexListener(c Connection) Listener {
	return newMultiplexer(c, true)
}

// A Connector can establish new outgoing connections.
type Connector interface {
	// Connect creates a new outgoing connection.
	Connect() (Connection, error)

	// Close closes the connector.
	// Depending on the implementation, this may also
	// close all previously created connections.
	Close() error
}

// MultiplexConnector creates a Connector that multiplexes
// a Connection into multiple outgoing connections.
// The other end of the Connection should use
// MultiplexListener.
func MultiplexConnector(c Connection) Connector {
	return newMultiplexer(c, false)
}

func init() {
	gob.Register(multiplexerOutgoing{})
}

type multiplexerOutgoing struct {
	ID       int64
	Payload  interface{}
	Ack      bool
	Close    bool
	CloseAck bool
	Connect  bool
	Accept   bool
}

type multiplexerBin struct {
	closed   bool
	outgoing chan struct{}
	incoming chan interface{}
}

type multiplexer struct {
	conn Connection

	binLock sync.RWMutex
	bins    map[int64]*multiplexerBin
	curID   int64

	acceptIDs  chan int64
	connectIDs chan int64

	terminateLock sync.Mutex
	terminateChan chan struct{}
}

func newMultiplexer(c Connection, accept bool) *multiplexer {
	m := &multiplexer{
		conn:          c,
		bins:          map[int64]*multiplexerBin{},
		terminateChan: make(chan struct{}),
	}
	if accept {
		m.acceptIDs = make(chan int64, multiplexerAcceptBuffer)
	} else {
		m.connectIDs = make(chan int64, multiplexerAcceptBuffer)
		for i := int64(0); i < multiplexerAcceptBuffer; i++ {
			m.connectIDs <- i
		}
	}
	go m.readLoop()
	return m
}

func (m *multiplexer) Accept() (Connection, error) {
	select {
	case id := <-m.acceptIDs:
		ack := multiplexerOutgoing{
			ID:     id,
			Accept: true,
		}
		if err := m.conn.Send(ack); err != nil {
			m.Close()
			return nil, err
		}
		return &multiplexedConn{multiplexer: m, id: id}, nil
	case <-m.terminateChan:
		return nil, errors.New("multiplexer closed during accept")
	}
}

func (m *multiplexer) Connect() (Connection, error) {
	select {
	case id := <-m.connectIDs:
		if err := m.allocateBin(id); err != nil {
			return nil, err
		}
		err := m.conn.Send(multiplexerOutgoing{
			ID:      id,
			Connect: true,
		})
		if err != nil {
			m.Close()
			return nil, err
		}
		return &multiplexedConn{multiplexer: m, id: id}, nil
	case <-m.terminateChan:
		return nil, errors.New("multiplexer closed during connect")
	}
}

func (m *multiplexer) Close() error {
	m.terminateLock.Lock()
	select {
	case <-m.terminateChan:
		m.terminateLock.Unlock()
		return errors.New("multiplexer already closed")
	default:
		close(m.terminateChan)
		m.terminateLock.Unlock()
		return m.conn.Close()
	}
}

func (m *multiplexer) readLoop() {
	defer m.Close()
	nextConnID := int64(multiplexerAcceptBuffer)
	for {
		obj, err := m.conn.Receive()
		if err != nil {
			return
		}
		msg, ok := obj.(multiplexerOutgoing)
		if !ok {
			return
		}
		switch true {
		case msg.Accept:
			if m.connectIDs == nil {
				// They accepted when we should be accepting.
				return
			}
			nextConnID++
			select {
			case m.connectIDs <- nextConnID:
			default:
				// They accepted more than we connected.
				return
			}
			nextConnID++
		case msg.Ack:
			if bin := m.binForID(msg.ID); bin != nil && !bin.closed {
				select {
				case bin.outgoing <- struct{}{}:
				default:
					// They acked more than we sent.
					return
				}
			}
		case msg.Close:
			bin := m.binForID(msg.ID)
			if bin != nil && !bin.closed {
				bin.closed = true
				close(bin.incoming)
				close(bin.outgoing)
			}
			ack := multiplexerOutgoing{
				ID:       msg.ID,
				CloseAck: true,
			}
			go func() {
				if m.conn.Send(ack) != nil {
					m.Close()
				}
			}()
		case msg.CloseAck:
			m.binLock.Lock()
			bin := m.bins[msg.ID]
			delete(m.bins, msg.ID)
			m.binLock.Unlock()
			if bin != nil && !bin.closed {
				close(bin.incoming)
				close(bin.outgoing)
			}
		case msg.Connect:
			if m.acceptIDs == nil {
				// They tried to connect but we aren't accepting.
				return
			}
			if m.allocateBin(msg.ID) != nil {
				return
			}
			select {
			case m.acceptIDs <- msg.ID:
			default:
				// They made too many unacknowledged connections.
				return
			}
		default:
			if bin := m.binForID(msg.ID); bin != nil && !bin.closed {
				select {
				case bin.incoming <- msg.Payload:
				default:
					// They sent too many unacknowledged messages.
					return
				}
			}
		}
	}
}

func (m *multiplexer) send(id int64, obj interface{}) error {
	bin := m.binForID(id)
	if bin == nil {
		return errors.New("cannot send on closed connection")
	}
	select {
	case _, ok := <-bin.outgoing:
		if !ok {
			return errors.New("remote end has disconnected")
		}
	case <-m.terminateChan:
		return errors.New("multiplexer closed during send")
	}
	out := multiplexerOutgoing{
		ID:      id,
		Payload: obj,
	}
	return m.conn.Send(out)
}

func (m *multiplexer) receive(id int64) (interface{}, error) {
	bin := m.binForID(id)
	if bin == nil {
		return nil, io.EOF
	}
	select {
	case datum, ok := <-bin.incoming:
		if !ok {
			return nil, io.EOF
		}
		ack := multiplexerOutgoing{
			ID:  id,
			Ack: true,
		}
		if err := m.conn.Send(ack); err != nil {
			m.Close()
			return nil, err
		}
		return datum, nil
	case <-m.terminateChan:
		return nil, errors.New("multiplexer closed during receive")
	}
}

func (m *multiplexer) closeID(id int64) error {
	return m.conn.Send(multiplexerOutgoing{
		ID:    id,
		Close: true,
	})
}

func (m *multiplexer) binForID(id int64) *multiplexerBin {
	m.binLock.RLock()
	defer m.binLock.RUnlock()
	return m.bins[id]
}

func (m *multiplexer) allocateBin(id int64) error {
	m.binLock.Lock()
	if _, ok := m.bins[id]; ok {
		m.binLock.Unlock()
		return errors.New("attempt to overwrite existing bin")
	}
	m.bins[id] = &multiplexerBin{
		outgoing: make(chan struct{}, multiplexerBuffer),
		incoming: make(chan interface{}, multiplexerBuffer),
	}
	for i := 0; i < multiplexerBuffer; i++ {
		m.bins[id].outgoing <- struct{}{}
	}
	m.binLock.Unlock()
	return nil
}

type multiplexedConn struct {
	multiplexer *multiplexer
	id          int64
}

func (m *multiplexedConn) Send(obj interface{}) error {
	return m.multiplexer.send(m.id, obj)
}

func (m *multiplexedConn) Receive() (interface{}, error) {
	return m.multiplexer.receive(m.id)
}

func (m *multiplexedConn) Close() error {
	return m.multiplexer.closeID(m.id)
}
