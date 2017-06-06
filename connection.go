package gobplexer

import (
	"encoding/gob"
	"io"
	"sync"
)

// A Connection is an abstract bidirectional stream of
// gob-encoded objects.
type Connection interface {
	// Send sends an object over the connection.
	// If the object cannot be encoded or transmitted,
	// an error is returned.
	Send(obj interface{}) error

	// Receive receives an object from the connection.
	// If the object cannot be received or decoded,
	// an error is returned.
	Receive() (interface{}, error)

	// Close closes the connection.
	// Any Send or Receive operations will be unblocked.
	// Close may be called multiple times, but any time
	// after the first will likely return an error.
	Close() error
}

// A gobConn is a Connection that wraps a data stream.
type gobConn struct {
	conn io.ReadWriteCloser

	readLock sync.Mutex
	dec      *gob.Decoder

	writeLock sync.Mutex
	enc       *gob.Encoder
}

// NetConnection creates a Connection around a raw
// communications channel.
// It is meant to be used to wrap things like net.Conn in
// a Connection.
//
// When the resulting Connection is closed, c will be
// closed as well.
func NetConnection(c io.ReadWriteCloser) Connection {
	return &gobConn{
		conn: c,
		dec:  gob.NewDecoder(c),
		enc:  gob.NewEncoder(c),
	}
}

func (g *gobConn) Send(obj interface{}) error {
	g.writeLock.Lock()
	defer g.writeLock.Unlock()
	return g.enc.Encode(&obj)
}

func (g *gobConn) Receive() (interface{}, error) {
	g.readLock.Lock()
	defer g.readLock.Unlock()
	var obj interface{}
	if err := g.dec.Decode(&obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (g *gobConn) Close() error {
	return g.conn.Close()
}
