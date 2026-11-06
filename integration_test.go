//go:build integration && !windows

// Integration tests against a real Phoenix server (integration/server.exs), started as a subprocess.
//
//	go test -tags integration -race ./...
//
// They need `elixir` on the PATH. The first run fetches and compiles Phoenix, which takes a while. Set
// PHOENIX_VERSION (a Hex requirement such as "~> 1.7.0") to test against another Phoenix version.

package phx

import (
	"bufio"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// phoenixServer is a running integration/server.exs.
type phoenixServer struct {
	cmd  *exec.Cmd
	port int

	mu   sync.Mutex
	tail []string
}

var shared *phoenixServer

func TestMain(m *testing.M) {
	srv, err := startPhoenixServer(0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration tests: could not start the Phoenix server:", err)
		os.Exit(1)
	}
	shared = srv
	code := m.Run()
	srv.stop()
	os.Exit(code)
}

// startPhoenixServer starts the server on port (0 picks a free one) and waits until it accepts connections.
func startPhoenixServer(port int) (*phoenixServer, error) {
	if port == 0 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		port = listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
	}

	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command("elixir", "integration/server.exs")
	cmd.Env = append(os.Environ(), fmt.Sprintf("PORT=%d", port))
	cmd.Stdout = writer
	cmd.Stderr = writer
	// Its own process group, so that stop kills the Erlang VM behind the elixir script as well.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	_ = writer.Close()

	srv := &phoenixServer{cmd: cmd, port: port}
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(reader)
		var once sync.Once
		for scanner.Scan() {
			line := scanner.Text()
			srv.mu.Lock()
			srv.tail = append(srv.tail, line)
			if len(srv.tail) > 30 {
				srv.tail = srv.tail[1:]
			}
			srv.mu.Unlock()
			if strings.HasPrefix(line, "READY") {
				once.Do(func() { close(ready) })
			}
		}
	}()

	select {
	case <-ready:
		return srv, nil
	case <-time.After(5 * time.Minute):
		srv.stop()
		return nil, fmt.Errorf("server not ready in time, last output:\n%s", srv.output())
	}
}

func (s *phoenixServer) output() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.tail, "\n")
}

func (s *phoenixServer) stop() {
	_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
	_ = s.cmd.Wait()
}

func (s *phoenixServer) endpoint(token, clientID string) *url.URL {
	u, _ := url.Parse(fmt.Sprintf("ws://127.0.0.1:%d/socket?token=%s&id=%s", s.port, token, clientID))
	return u
}

var nonWord = regexp.MustCompile(`\W+`)

// testName is the name of the test usable as a topic suffix or client id.
func testName(t *testing.T) string { return nonWord.ReplaceAllString(t.Name(), "_") }

// newRealClient returns a connected Socket for the server, with fast reconnects.
func newRealClient(t *testing.T, srv *phoenixServer, clientID string) *Socket {
	t.Helper()
	socket := newDisconnectedClient(t, srv.endpoint("secret", clientID))
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "socket connected", socket.IsConnected)
	return socket
}

func newDisconnectedClient(t *testing.T, endPoint *url.URL) *Socket {
	t.Helper()
	socket := NewSocket(endPoint)
	socket.ConnectTimeout = 5 * time.Second
	socket.ReconnectAfterFunc = func(int) time.Duration { return 50 * time.Millisecond }
	t.Cleanup(func() { go func() { _ = socket.Disconnect() }() })
	return socket
}

func joinChannel(t *testing.T, ch *Channel) {
	t.Helper()
	if _, err := ch.Join(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "channel joined", ch.IsJoined)
}

// realChannel returns a Channel for a topic of this test, configured for quick rejoins.
func realChannel(t *testing.T, socket *Socket, params map[string]string) *Channel {
	t.Helper()
	ch := socket.Channel("echo:"+testName(t), params)
	ch.RejoinAfterFunc = func(int) time.Duration { return 100 * time.Millisecond }
	return ch
}

type reply struct {
	status  string
	payload any
}

// pushAndWait pushes an event and waits for its reply (ok, error or timeout).
func pushAndWait(t *testing.T, ch *Channel, event string, payload any) reply {
	t.Helper()
	push, err := ch.Push(event, payload)
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan reply, 3)
	for _, status := range []string{"ok", "error", "timeout"} {
		status := status
		push.Receive(status, func(response any) { out <- reply{status, response} })
	}
	select {
	case r := <-out:
		return r
	case <-time.After(15 * time.Second):
		t.Fatalf("no reply to %q", event)
		return reply{}
	}
}

func counter() (func(), func() int) {
	var n int32
	return func() { atomic.AddInt32(&n, 1) }, func() int { return int(atomic.LoadInt32(&n)) }
}

func TestRealJoinReplyCarriesParams(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, map[string]string{"hello": "world"})

	push, err := ch.Join()
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan any, 1)
	push.Receive("ok", func(response any) { got <- response })

	select {
	case response := <-got:
		joined := response.(map[string]any)["joined"].(map[string]any)
		if joined["hello"] != "world" {
			t.Fatalf("join reply %v does not carry the join params", response)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no join reply")
	}
	eventually(t, 5*time.Second, "channel joined", ch.IsJoined)
}

func TestRealJoinRejected(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := socket.Channel("echo:reject", nil)

	push, err := ch.Join()
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan any, 1)
	push.Receive("error", func(response any) { got <- response })

	select {
	case response := <-got:
		if response.(map[string]any)["reason"] != "rejected" {
			t.Fatalf("join error %v does not carry the server's reason", response)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no join error")
	}
	if ch.IsJoined() {
		t.Fatal("channel reports joined after a rejected join")
	}
	if !socket.IsConnected() {
		t.Fatal("a rejected join must not close the socket")
	}
}

func TestRealPushEchoesJSON(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	joinChannel(t, ch)

	payload := map[string]any{
		"text":   "héllo wörld 🌍",
		"int":    float64(42),
		"float":  1.5,
		"bool":   true,
		"null":   nil,
		"list":   []any{"a", float64(1), nil},
		"nested": map[string]any{"k": map[string]any{"deep": "v"}},
	}
	r := pushAndWait(t, ch, "ping", payload)
	if r.status != "ok" || !reflect.DeepEqual(r.payload, payload) {
		t.Fatalf("got %s %v, want ok with the payload echoed", r.status, r.payload)
	}
}

func TestRealLargePayload(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	joinChannel(t, ch)

	payload := map[string]any{"data": strings.Repeat("x", 512*1024)}
	r := pushAndWait(t, ch, "ping", payload)
	if r.status != "ok" || !reflect.DeepEqual(r.payload, payload) {
		t.Fatalf("a 512 KiB payload did not come back intact (status %s)", r.status)
	}
}

func TestRealErrorReply(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	joinChannel(t, ch)

	r := pushAndWait(t, ch, "fail", map[string]any{"why": "because"})
	if r.status != "error" || !reflect.DeepEqual(r.payload, map[string]any{"why": "because"}) {
		t.Fatalf("got %s %v, want an error reply with the payload", r.status, r.payload)
	}
}

func TestRealPushWithoutReplyTimesOut(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	ch.PushTimeout = 300 * time.Millisecond
	joinChannel(t, ch)

	if r := pushAndWait(t, ch, "noreply", map[string]any{}); r.status != "timeout" {
		t.Fatalf("got %s, want timeout", r.status)
	}
}

func TestRealConcurrentPushesGetTheirOwnReplies(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	joinChannel(t, ch)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := map[string]any{"i": float64(i)}
			if r := pushAndWait(t, ch, "ping", payload); r.status != "ok" || !reflect.DeepEqual(r.payload, payload) {
				t.Errorf("push %d got %s %v", i, r.status, r.payload)
			}
		}(i)
	}
	wg.Wait()
}

func TestRealBroadcastReachesEveryClientOnTheTopic(t *testing.T) {
	var channels []*Channel
	received := make(chan string, 4)
	for _, id := range []string{"a", "b"} {
		id := id
		socket := newRealClient(t, shared, testName(t)+"_"+id)
		ch := realChannel(t, socket, nil)
		ch.On("shout", func(payload any) { received <- id + ":" + payload.(map[string]any)["text"].(string) })
		joinChannel(t, ch)
		channels = append(channels, ch)
	}

	// The server does not reply to a shout; it broadcasts it.
	if _, err := channels[0].Push("shout", map[string]any{"text": "hi"}); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case m := <-received:
			got[m] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only received %v, want a broadcast on both clients", got)
		}
	}
	if !got["a:hi"] || !got["b:hi"] {
		t.Fatalf("received %v, want a:hi and b:hi", got)
	}
}

func TestRealOrderedCallbacksSeeServerPushesInOrder(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	ch.OrderedCallbacks = true

	var mu sync.Mutex
	var seen []int
	ch.On("seq", func(payload any) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, int(payload.(map[string]any)["n"].(float64)))
	})
	joinChannel(t, ch)

	const count = 500
	if r := pushAndWait(t, ch, "sequence", map[string]any{"count": count}); r.status != "ok" {
		t.Fatalf("got %s", r.status)
	}
	eventually(t, 10*time.Second, "all messages received", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == count
	})
	mu.Lock()
	defer mu.Unlock()
	for i, n := range seen {
		if n != i+1 {
			t.Fatalf("message %d arrived as number %d: callbacks ran out of order", i+1, n)
		}
	}
}

func TestRealLeave(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	closed, closedCount := counter()
	ch.OnClose(func(any) { closed() })
	joinChannel(t, ch)

	push, err := ch.Leave()
	if err != nil {
		t.Fatal(err)
	}
	ok := make(chan struct{}, 1)
	push.Receive("ok", func(any) { ok <- struct{}{} })
	select {
	case <-ok:
	case <-time.After(10 * time.Second):
		t.Fatal("no reply to the leave")
	}
	eventually(t, 5*time.Second, "channel closed", ch.IsClosed)
	// Callbacks run on their own goroutines, so the state can change before OnClose has run.
	eventually(t, 5*time.Second, "OnClose callback", func() bool { return closedCount() >= 1 })
	time.Sleep(200 * time.Millisecond)
	if closedCount() != 1 {
		t.Fatalf("OnClose ran %d times, want 1", closedCount())
	}

	// A left channel can be joined again.
	joinChannel(t, ch)
}

func TestRealServerClosingTheChannelDoesNotRejoin(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	closed, closedCount := counter()
	ch.OnClose(func(any) { closed() })
	joinChannel(t, ch)

	if r := pushAndWait(t, ch, "stop", map[string]any{}); r.status != "ok" {
		t.Fatalf("got %s", r.status)
	}
	eventually(t, 5*time.Second, "channel closed", ch.IsClosed)
	eventually(t, 5*time.Second, "OnClose callback", func() bool { return closedCount() >= 1 })
	time.Sleep(500 * time.Millisecond)
	if !ch.IsClosed() {
		t.Fatalf("channel is %s, but a phx_close must not trigger a rejoin", ch.State())
	}
	if closedCount() != 1 {
		t.Fatalf("OnClose ran %d times, want 1", closedCount())
	}
	joinChannel(t, ch)
}

func TestRealChannelCrashIsRejoined(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	ch.PushTimeout = 500 * time.Millisecond
	failed, failedCount := counter()
	joined, joinedCount := counter()
	ch.OnError(func(any) { failed() })
	ch.OnJoin(func(any) { joined() })
	joinChannel(t, ch)

	_ = pushAndWait(t, ch, "crash", map[string]any{}) // never answered: the channel process dies

	eventually(t, 15*time.Second, "phx_error seen and channel rejoined", func() bool {
		return failedCount() >= 1 && joinedCount() >= 2 && ch.IsJoined()
	})
	payload := map[string]any{"after": "rejoin"}
	if r := pushAndWait(t, ch, "ping", payload); r.status != "ok" || !reflect.DeepEqual(r.payload, payload) {
		t.Fatalf("got %s %v after the rejoin", r.status, r.payload)
	}
}

func TestRealServerDroppingTheSocketReconnectsAndRejoins(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	ch.PushTimeout = 500 * time.Millisecond
	closed, closedCount := counter()
	socket.OnClose(closed)
	joined, joinedCount := counter()
	ch.OnJoin(func(any) { joined() })
	joinChannel(t, ch)

	_ = pushAndWait(t, ch, "drop_socket", map[string]any{})

	eventually(t, 20*time.Second, "socket reconnected and channel rejoined", func() bool {
		return closedCount() >= 1 && joinedCount() >= 2 && socket.IsConnected() && ch.IsJoined()
	})
	payload := map[string]any{"after": "reconnect"}
	if r := pushAndWait(t, ch, "ping", payload); r.status != "ok" || !reflect.DeepEqual(r.payload, payload) {
		t.Fatalf("got %s %v after the reconnect", r.status, r.payload)
	}
}

func TestRealServerRestartIsSurvived(t *testing.T) {
	srv, err := startPhoenixServer(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.stop() })

	socket := newRealClient(t, srv, testName(t))
	ch := realChannel(t, socket, nil)
	joined, joinedCount := counter()
	ch.OnJoin(func(any) { joined() })
	joinChannel(t, ch)

	srv.stop()
	eventually(t, 10*time.Second, "client notices the server is gone", func() bool { return !socket.IsConnected() })

	restarted, err := startPhoenixServer(srv.port)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.stop() })

	eventually(t, 30*time.Second, "client reconnected and rejoined", func() bool {
		return socket.IsConnected() && ch.IsJoined() && joinedCount() >= 2
	})
	payload := map[string]any{"after": "restart"}
	if r := pushAndWait(t, ch, "ping", payload); r.status != "ok" || !reflect.DeepEqual(r.payload, payload) {
		t.Fatalf("got %s %v after the server restart", r.status, r.payload)
	}
}

func TestRealDisconnectThenConnectRejoins(t *testing.T) {
	socket := newRealClient(t, shared, testName(t))
	ch := realChannel(t, socket, nil)
	joined, joinedCount := counter()
	ch.OnJoin(func(any) { joined() })
	joinChannel(t, ch)

	if err := socket.Disconnect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "socket closed", func() bool { return socket.ConnectionState() == ConnectionClosed })
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}

	eventually(t, 20*time.Second, "socket connected and channel rejoined", func() bool {
		return socket.IsConnected() && ch.IsJoined() && joinedCount() >= 2
	})
	payload := map[string]any{"after": "restart"}
	if r := pushAndWait(t, ch, "ping", payload); r.status != "ok" || !reflect.DeepEqual(r.payload, payload) {
		t.Fatalf("got %s %v after reconnecting", r.status, r.payload)
	}
}

func TestRealSerializerV1(t *testing.T) {
	socket := newDisconnectedClient(t, shared.endpoint("secret", testName(t)))
	socket.Serializer = NewJSONSerializerV1()
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "socket connected", socket.IsConnected)

	ch := realChannel(t, socket, nil)
	joinChannel(t, ch)
	payload := map[string]any{"v": "1.0.0"}
	if r := pushAndWait(t, ch, "ping", payload); r.status != "ok" || !reflect.DeepEqual(r.payload, payload) {
		t.Fatalf("got %s %v over the V1 serializer", r.status, r.payload)
	}
}

func TestRealRejectedConnectionKeepsRetrying(t *testing.T) {
	socket := newDisconnectedClient(t, shared.endpoint("wrong", testName(t)))
	failed, failedCount := counter()
	socket.OnError(func(error) { failed() })
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}

	eventually(t, 5*time.Second, "several failed connection attempts", func() bool { return failedCount() >= 3 })
	if socket.IsConnected() {
		t.Fatal("socket connected with a wrong token")
	}
}

func TestRealHeartbeatsKeepTheConnectionOpen(t *testing.T) {
	socket := newDisconnectedClient(t, shared.endpoint("secret", testName(t)))
	socket.HeartbeatInterval = 100 * time.Millisecond
	closed, closedCount := counter()
	socket.OnClose(closed)
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "socket connected", socket.IsConnected)
	ch := realChannel(t, socket, nil)
	joinChannel(t, ch)

	// A heartbeat the server did not answer makes the client reconnect after two intervals, so a connection that
	// survives many intervals means Phoenix answers them.
	time.Sleep(2 * time.Second)
	if closedCount() != 0 || !socket.IsConnected() {
		t.Fatalf("connection was closed %d times while only heartbeats were exchanged", closedCount())
	}
	if r := pushAndWait(t, ch, "ping", map[string]any{}); r.status != "ok" {
		t.Fatalf("got %s", r.status)
	}
}

func TestRealBinaryFrames(t *testing.T) {
	t.Skip("known gap: binary frames (Phoenix's binary V2 serializer) are not supported, see the README")
}
