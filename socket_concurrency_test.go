package phx

import (
	"sync"
	"testing"
	"time"
)

// The reader goroutine walks the Socket's channels and callbacks for every message while user goroutines create and
// remove channels and register callbacks. Run with -race: unsynchronised maps also fail with a fatal
// "concurrent map iteration and map write".
func TestSocketRegistriesAreSafeWhileMessagesArrive(t *testing.T) {
	server := newTestServer(t)
	socket := newTestSocket(t, server)
	if err := socket.Connect(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "socket connected", socket.IsConnected)
	conn := server.nextConn(t)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = conn.send(testFrame{Topic: "room:other", Event: "tick", Payload: map[string]any{}})
			time.Sleep(time.Millisecond)
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				channel := socket.Channel("room:churn", nil)
				_ = channel.Remove()
				open := socket.OnOpen(func() {})
				closed := socket.OnClose(func() {})
				failed := socket.OnError(func(error) {})
				message := socket.OnMessage(func(Message) {})
				socket.Off(open)
				socket.Off(closed)
				socket.Off(failed)
				socket.Off(message)
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestSocketChannelReturnsOneChannelPerTopic(t *testing.T) {
	server := newTestServer(t)
	socket := newTestSocket(t, server)

	const n = 32
	channels := make([]*Channel, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			channels[i] = socket.Channel("room:same", nil)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, channel := range channels {
		if channel != channels[0] {
			t.Fatalf("goroutine %d got a different Channel for the same topic", i)
		}
	}
}
