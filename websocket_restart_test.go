package phx

import (
	"bytes"
	"net"
	"net/url"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// goroutinesIn counts the goroutines currently running a function whose name contains fn.
func goroutinesIn(fn string) int {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	return bytes.Count(buf.Bytes(), []byte(fn))
}

// After Disconnect and a new Connect the old reader, writer and manager goroutines must be gone. They used to read
// the transport's done channel afresh on every loop, so one that was still winding down (the reader sleeps a bit
// after its connection closed) saw the NEW session's open channel and lived on next to the new goroutines: two
// readers on one connection.
func TestRestartLeavesNoGoroutinesOfTheOldSession(t *testing.T) {
	server := newTestServer(t)
	socket := newTestSocket(t, server)

	names := []string{"connectionReader", "connectionWriter", "connectionManager"}
	baseline := map[string]int{}
	for _, name := range names {
		baseline[name] = goroutinesIn(name)
	}

	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "socket connected", socket.IsConnected)
	if err := socket.Disconnect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "socket closed", func() bool {
		return socket.ConnectionState() == ConnectionClosed
	})
	// Connect again right away, while the old reader is still in its short sleep after the close.
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "socket connected again", socket.IsConnected)

	time.Sleep(4 * busyWait)
	for _, name := range names {
		if got := goroutinesIn(name) - baseline[name]; got > 1 {
			t.Errorf("%d %s goroutines running, want 1", got, name)
		}
	}
}

// A Disconnect while a dial is still hanging must end that dial. It used to run on until the handshake timeout, and
// then installed its connection into whatever session had been started in the meantime.
func TestDisconnectAbortsPendingDial(t *testing.T) {
	// A server that accepts the TCP connection and then never answers the websocket handshake.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()

	before := goroutinesIn("connectionManager")
	u := &url.URL{Scheme: "ws", Host: listener.Addr().String(), Path: "/socket"}
	socket := NewSocket(u)
	socket.ConnectTimeout = 30 * time.Second
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * busyWait) // the manager is now inside Dial
	if err := socket.Disconnect(); err != nil {
		t.Fatal(err)
	}

	eventually(t, time.Second, "pending dial aborted", func() bool {
		return goroutinesIn("connectionManager") <= before
	})
}

// The reader reports the server's close frame to a manager that waits for it. If the manager has given up waiting,
// nobody receives it, and the reader must not block on it.
func TestReaderDoesNotBlockOnUnwantedCloseFrame(t *testing.T) {
	server := newTestServer(t)
	socket := newTestSocket(t, server)
	ws := socket.Transport.(*Websocket)
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "socket connected", socket.IsConnected)
	conn := server.nextConn(t)

	// As if the manager were waiting for the close handshake, then stops waiting without ever receiving.
	ws.setWaitingForClose(true)
	data := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
	if err := conn.ws.WriteControl(websocket.CloseMessage, data, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	eventually(t, 2*time.Second, "reader handed the close frame over without blocking", func() bool {
		return len(ws.closeMsg) == 1
	})
}
