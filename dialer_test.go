package phx

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// A Websocket must own its Dialer: configuring it (a proxy, a timeout) or dialing must not touch the process-wide
// websocket.DefaultDialer, which other sockets and unrelated code share.
func TestWebsocketDoesNotShareDefaultDialer(t *testing.T) {
	a := NewWebsocket(nopHandler{})
	b := NewWebsocket(nopHandler{})
	if a.Dialer == websocket.DefaultDialer || b.Dialer == websocket.DefaultDialer {
		t.Fatal("Websocket uses the global websocket.DefaultDialer")
	}
	if a.Dialer == b.Dialer {
		t.Fatal("two Websockets share one Dialer")
	}
}

func TestConnectLeavesDefaultDialerUntouched(t *testing.T) {
	server := newTestServer(t)
	before := websocket.DefaultDialer.HandshakeTimeout

	socket := NewSocket(server.URL())
	socket.ConnectTimeout = 3 * time.Second
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "socket connected", socket.IsConnected)
	t.Cleanup(func() { go func() { _ = socket.Disconnect() }() })

	if got := websocket.DefaultDialer.HandshakeTimeout; got != before {
		t.Fatalf("DefaultDialer.HandshakeTimeout changed from %v to %v", before, got)
	}
}
