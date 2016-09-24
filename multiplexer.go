package gobplexer

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"
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
	return newConnMultiplexer(c, true)
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
	return newConnMultiplexer(c, false)
}

func init() {
	gob.Register(multiplexMsg{})
	gob.Register(connMsg{})
}

type multiplexMsg struct {
	ID       int64
	New      bool
	Close    bool
	CloseAck bool
	Payload  interface{}
}

type connMsg struct {
	Payload interface{}
	EOF     bool
}

// A connMultiplexer multiplexes a Connection by giving
// IDs to different virtual connections.
type connMultiplexer struct {
	conn Connection

	lock    sync.RWMutex
	curID   int64
	chanMap map[int64]chan connMsg

	idChan chan int64

	termLock      sync.Mutex
	terminated    bool
	terminateChan chan struct{}

	errLock sync.Mutex
	err     error
}

func newConnMultiplexer(conn Connection, accepts bool) *connMultiplexer {
	c := &connMultiplexer{
		conn:          conn,
		chanMap:       map[int64]chan connMsg{},
		terminateChan: make(chan struct{}),
	}
	if accepts {
		c.idChan = make(chan int64)
	}
	go c.receiveLoop()
	return c
}

func (c *connMultiplexer) Accept() (Connection, error) {
	select {
	case id := <-c.idChan:
		return &subConn{multiplexer: c, id: id}, nil
	case <-c.terminateChan:
		return nil, c.firstError()
	}
}

func (c *connMultiplexer) Connect() (Connection, error) {
	c.lock.Lock()
	id := c.curID
	c.curID++
	c.chanMap[id] = make(chan connMsg, 1)
	c.lock.Unlock()

	select {
	case <-c.terminateChan:
		return nil, c.firstError()
	default:
	}

	err := c.conn.Send(multiplexMsg{
		ID:  id,
		New: true,
	})
	return &subConn{multiplexer: c, id: id}, err
}

func (c *connMultiplexer) CloseID(id int64) error {
	return c.conn.Send(multiplexMsg{
		ID:    id,
		Close: true,
	})
}

func (c *connMultiplexer) Send(id int64, obj interface{}) error {
	return c.conn.Send(multiplexMsg{
		ID:      id,
		Payload: obj,
	})
}

func (c *connMultiplexer) Receive(id int64) (interface{}, error) {
	c.lock.RLock()
	ch := c.chanMap[id]
	c.lock.RUnlock()

	if ch == nil {
		return nil, io.EOF
	}

	select {
	case res, ok := <-ch:
		if !ok {
			return nil, c.firstError()
		}
		if res.EOF {
			return nil, io.EOF
		}
		return res.Payload, nil
	case <-c.terminateChan:
		return nil, c.firstError()
	}
}

func (c *connMultiplexer) Close() error {
	c.gotError(errors.New("multiplexer closed"))
	c.termLock.Lock()
	if c.terminated {
		c.termLock.Unlock()
		return c.firstError()
	}
	c.terminated = true
	close(c.terminateChan)
	c.termLock.Unlock()
	return c.conn.Close()
}

func (c *connMultiplexer) receiveLoop() {
	defer func() {
		c.Close()
		c.lock.Lock()
		for _, ch := range c.chanMap {
			close(ch)
		}
		c.chanMap = map[int64]chan connMsg{}
		c.lock.Unlock()
	}()
	for {
		msg, err := c.conn.Receive()
		if err != nil {
			c.gotError(err)
			return
		}
		msgVal, ok := msg.(multiplexMsg)
		if !ok {
			c.gotError(fmt.Errorf("unexpected multiplexer message type: %T", msg))
			return
		}
		switch true {
		case msgVal.New:
			if c.idChan == nil {
				c.gotError(errors.New("multiplexer cannot accept connections"))
				return
			}
			c.lock.Lock()
			id := c.curID
			c.curID++
			c.chanMap[id] = make(chan connMsg, 1)
			c.lock.Unlock()
			select {
			case c.idChan <- id:
			case <-c.terminateChan:
				return
			}
		case msgVal.Close:
			c.conn.Send(multiplexMsg{
				ID:       msgVal.ID,
				CloseAck: true,
			})
			fallthrough
		case msgVal.CloseAck:
			c.lock.Lock()
			ch := c.chanMap[msgVal.ID]
			delete(c.chanMap, msgVal.ID)
			c.lock.Unlock()
			if ch != nil {
				select {
				case ch <- connMsg{EOF: true}:
					close(ch)
				case <-c.terminateChan:
					return
				}
			}
		default:
			c.lock.RLock()
			ch := c.chanMap[msgVal.ID]
			c.lock.RUnlock()
			if ch != nil {
				select {
				case ch <- connMsg{Payload: msgVal.Payload}:
				case <-c.terminateChan:
					return
				}
			}
		}
	}
}

func (c *connMultiplexer) gotError(e error) {
	c.errLock.Lock()
	defer c.errLock.Unlock()
	if c.err == nil {
		c.err = e
	}
}

func (c *connMultiplexer) firstError() error {
	c.errLock.Lock()
	defer c.errLock.Unlock()
	return c.err
}

type subConn struct {
	multiplexer *connMultiplexer
	id          int64
}

func (s *subConn) Send(obj interface{}) error {
	return s.multiplexer.Send(s.id, obj)
}

func (s *subConn) Receive() (interface{}, error) {
	return s.multiplexer.Receive(s.id)
}

func (s *subConn) Close() error {
	return s.multiplexer.CloseID(s.id)
}
