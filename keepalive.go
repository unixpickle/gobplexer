package gobplexer

import (
	"fmt"
	"io"
	"time"
)

// KeepaliveListener creates a Connection on top of l that
// uses a keep-alive mechanism to close l when the remote
// end has been disconnected for too long.
// The remote end should use KeepaliveConnector.
//
// Closing the returned Connection will close l.
func KeepaliveListener(l Listener, interval, maxDelay time.Duration) (Connection, error) {
	keepalive, err := l.Accept()
	if err != nil {
		return nil, fmt.Errorf("failed to accept keepalive connection: %s", err)
	}
	mainConn, err := l.Accept()
	if err != nil {
		return nil, fmt.Errorf("failed to accept main connection: %s", err)
	}

	runKeepalive(l, keepalive, interval, maxDelay)

	return &parentCloser{mainConn, l}, nil
}

// KeepaliveConnector creates a Connection on top of c that
// uses a keep-alive mechanism to close c when the remote
// end has been disconnected for too long.
// The remote end should use KeepaliveListener.
//
// Closing the returned Connection will close c.
func KeepaliveConnector(c Connector, interval, maxDelay time.Duration) (Connection, error) {
	keepalive, err := c.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect keepalive connection: %s", err)
	}
	mainConn, err := c.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect main connection: %s", err)
	}

	runKeepalive(c, keepalive, interval, maxDelay)

	return &parentCloser{mainConn, c}, nil
}

func runKeepalive(c io.Closer, keepalive Connection, interval, maxDelay time.Duration) {
	go func() {
		for {
			time.Sleep(interval)
			if keepalive.Send("keepalive!") != nil {
				return
			}
		}
	}()

	keepaliveMessages := make(chan interface{})
	go func() {
		for {
			msg, err := keepalive.Receive()
			if err != nil {
				close(keepaliveMessages)
				return
			}
			keepaliveMessages <- msg
		}
	}()
	go func() {
		for {
			select {
			case _, ok := <-keepaliveMessages:
				if !ok {
					return
				}
			case <-time.After(maxDelay):
				c.Close()
				return
			}
		}
	}()
}

type parentCloser struct {
	Connection
	parent io.Closer
}

func (p *parentCloser) Close() error {
	return p.parent.Close()
}
