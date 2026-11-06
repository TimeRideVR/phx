package phx

import (
	"net/url"
	"testing"
	"time"
)

// RejoinAfterFunc is a public field that users assign after creating the Channel. The rejoin timer used to capture
// the default function at construction, so the assignment was silently ignored.
func TestRejoinAfterFuncAssignedLaterIsUsed(t *testing.T) {
	u, _ := url.Parse("ws://127.0.0.1:1/socket")
	socket := NewSocket(u)
	ch := socket.Channel("room:lobby", nil)

	ch.RejoinAfterFunc = func(tries int) time.Duration { return time.Duration(tries) * 7 * time.Millisecond }

	if got := ch.rejoinTimer.timerCalc(3); got != 21*time.Millisecond {
		t.Fatalf("rejoin delay for try 3 is %v, want 21ms from the assigned RejoinAfterFunc", got)
	}
}
