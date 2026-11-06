package phx

import (
	"sync/atomic"
	"testing"
	"time"
)

// Phoenix answers a phx_leave with a reply and then sends phx_close as well. OnClose must still run once per close:
// the client closes the Channel on the leave reply, and the server's phx_close must not close it a second time.
func TestLeaveRunsOnCloseOnceWhenServerAlsoSendsClose(t *testing.T) {
	server := newTestServer(t)
	server.setCloseAfterLeave(true)
	socket := newTestSocket(t, server)
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "socket connected", socket.IsConnected)

	ch := socket.Channel("room:lobby", nil)
	var closes int32
	ch.OnClose(func(any) { atomic.AddInt32(&closes, 1) })
	if _, err := ch.Join(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "channel joined", ch.IsJoined)

	if _, err := ch.Leave(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "OnClose ran", func() bool { return atomic.LoadInt32(&closes) >= 1 })
	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt32(&closes); n != 1 {
		t.Fatalf("OnClose ran %d times, want 1", n)
	}
	if !ch.IsClosed() {
		t.Fatalf("channel is %s, want closed", ch.State())
	}
}

// Leaving while the socket is down closes the Channel at once, and runs OnClose once.
func TestLeaveWhileDisconnectedClosesOnce(t *testing.T) {
	server := newTestServer(t)
	socket := newTestSocket(t, server)
	ch := socket.Channel("room:lobby", nil)
	ch.setState(ChannelErrored)

	var closes int32
	ch.OnClose(func(any) { atomic.AddInt32(&closes, 1) })
	if _, err := ch.Leave(); err != nil {
		t.Fatal(err)
	}
	if !ch.IsClosed() {
		t.Fatalf("channel is %s, want closed", ch.State())
	}
	eventually(t, 2*time.Second, "OnClose ran", func() bool { return atomic.LoadInt32(&closes) >= 1 })
	time.Sleep(200 * time.Millisecond)
	if n := atomic.LoadInt32(&closes); n != 1 {
		t.Fatalf("OnClose ran %d times, want 1", n)
	}
}
