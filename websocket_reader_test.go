package phx

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// closeSpy records whether the client closed the underlying connection.
type closeSpy struct {
	net.Conn
	closed int32
}

func (c *closeSpy) Close() error {
	atomic.StoreInt32(&c.closed, 1)
	return c.Conn.Close()
}

func (c *closeSpy) isClosed() bool { return atomic.LoadInt32(&c.closed) == 1 }

// When the server drops the connection, the reader hits a read error. It must close the dead connection rather than
// only forgetting it, which would hold the file descriptor until the garbage collector finds it.
func TestReadErrorClosesTheConnection(t *testing.T) {
	server := newTestServer(t)
	socket := newTestSocket(t, server)
	ws := socket.Transport.(*Websocket)

	var mu sync.Mutex
	var spies []*closeSpy
	ws.Dialer = &websocket.Dialer{NetDial: func(network, addr string) (net.Conn, error) {
		conn, err := net.Dial(network, addr)
		if err != nil {
			return nil, err
		}
		spy := &closeSpy{Conn: conn}
		mu.Lock()
		spies = append(spies, spy)
		mu.Unlock()
		return spy, nil
	}}

	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "socket connected", socket.IsConnected)
	first := server.nextConn(t)

	// Drop the TCP connection without a websocket close handshake.
	_ = first.ws.UnderlyingConn().Close()

	eventually(t, 5*time.Second, "client reconnected", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(spies) >= 2 && socket.IsConnected()
	})
	mu.Lock()
	firstSpy := spies[0]
	mu.Unlock()
	eventually(t, 2*time.Second, "dead connection closed by the client", firstSpy.isClosed)
}

// A late read error of an old connection must not unset a newer connection.
func TestClearConnOnlyClearsTheSameConnection(t *testing.T) {
	w := NewWebsocket(nopHandler{})
	old := &websocket.Conn{}
	newer := &websocket.Conn{}

	w.setConn(newer)
	w.clearConn(old)
	if w.getConn() != newer {
		t.Fatal("clearConn removed a connection that is not the one given")
	}
	w.clearConn(newer)
	if w.getConn() != nil {
		t.Fatal("clearConn did not remove the given connection")
	}
}
