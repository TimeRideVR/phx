package phx

import (
	"testing"
	"time"
)

// A message queued while the connection is not usable (a reconnect is under way) must be sent once it is, not
// discarded. The writer used to take the message off the queue first and only then notice the connection was not
// ready.
func TestWriterKeepsMessageQueuedDuringReconnect(t *testing.T) {
	server := newTestServer(t)
	socket := newTestSocket(t, server)
	ws := socket.Transport.(*Websocket)
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "socket connected", socket.IsConnected)
	conn := server.nextConn(t)

	// Give the writer time to go from its idle poll into waiting for a message on a ready connection.
	time.Sleep(3 * busyWait)

	ws.mu.Lock()
	ws.reconnecting = true
	ws.mu.Unlock()

	if _, err := socket.Push("room:lobby", "ping", map[string]any{"n": 1}, 0); err != nil {
		t.Fatal(err)
	}
	// Let the writer pick the message up while the connection is not ready.
	time.Sleep(3 * busyWait)

	ws.mu.Lock()
	ws.reconnecting = false
	ws.mu.Unlock()

	f := conn.next(t)
	if f.Event != "ping" || f.Topic != "room:lobby" {
		t.Fatalf("got frame %+v, want the queued ping", f)
	}
}
