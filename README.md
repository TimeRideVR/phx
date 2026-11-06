# Phx - A Phoenix Channels client for Go

> **TimeRide fork of [nshafer/phx](https://github.com/nshafer/phx)**, published as `github.com/TimeRideVR/phx`. It
> carries these fixes until they are merged upstream:
>
> - [fix-disconnect-deadlock](https://github.com/TimeRideVR/phx/tree/fix-disconnect-deadlock): Disconnect no longer
>   deadlocks when it races a reconnect.
> - [fix-push-binding-race](https://github.com/TimeRideVR/phx/tree/fix-push-binding-race): a push registers its reply
>   binding before sending, so fast replies are not lost; push state is synchronised.
> - [ordered-callbacks](https://github.com/TimeRideVR/phx/tree/ordered-callbacks): opt-in `Channel.OrderedCallbacks`
>   runs a channel's event callbacks one at a time in arrival order.
> - [fix-heartbeat-race](https://github.com/TimeRideVR/phx/tree/fix-heartbeat-race): no data race or panic between
>   heartbeat goroutines across reconnects.
> - Concurrency and lifecycle fixes found in a code review: the Socket's channel and callback registries are
>   synchronised; a Websocket no longer mutates the process-wide `websocket.DefaultDialer`; the writer no longer drops
>   a message it could not send yet; a connection that failed a read is closed; a `Disconnect` followed by `Connect`
>   no longer leaves goroutines of the old session running; `gorilla/websocket` is updated to v1.5.3 (GO-2026-6278).
> - [nshafer/phx#6](https://github.com/nshafer/phx/pull/6): fix dangling bindings due to timed out Push calls.
> - [nshafer/phx#8](https://github.com/nshafer/phx/pull/8): fix "send on closed channel" panic when shutdown races
>   internal signalling.

[![CI](https://github.com/TimeRideVR/phx/actions/workflows/ci.yml/badge.svg)](https://github.com/TimeRideVR/phx/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/TimeRideVR/phx.svg)](https://pkg.go.dev/github.com/TimeRideVR/phx) [![Static Badge](https://img.shields.io/badge/changelog-blue)](https://github.com/TimeRideVR/phx/releases)

## How the changes in this fork were made

**Everything this fork changed since it was forked from nshafer/phx was implemented with AI tools** (Claude Code).
That covers the fixes listed above and every commit on the `timeride` branch and its fix branches; commits made this
way carry a `Co-Authored-By: Claude` trailer. The code up to that point is nshafer's.

What that means for you as a consumer:

- Each fix comes with tests that fail without it, and the suite runs with the race detector in CI, together with
  `go vet` and `govulncheck`.
- Besides unit tests against a small in-process stand-in server, [integration tests](#testing) run the client against
  a real Phoenix server (1.7 and 1.8) in CI. They found a bug the stand-in could not: `OnClose` ran twice after a
  `Leave`.
- This is MIT-licensed software without warranty, as before. Review the diff of a release the way you would for any
  third-party dependency, and pin the version you have verified in your own setup.

This is a comprehensive client for Phoenix Channels written for Go applications.
The goal of this project is to be a reliable, resilient, full-featured client library for connecting to Phoenix
servers over websockets and being able to push and receive events from one or more channels in a performant and
concurrent manner.

This module is based off of the official JavaScript client implementation, except where deviations were needed due to
being written in Go. But if you're familiar with connecting to Phoenix Channels in JS, then this library should feel
right at home.

## Installation

This module requires Go 1.18 or later.

    go get github.com/TimeRideVR/phx

## Documentation

API documentation can be viewed via godoc at [API Documentation](https://pkg.go.dev/github.com/TimeRideVR/phx).

Examples are in [examples/](examples/).

## Changelog

Please see [Releases](https://github.com/TimeRideVR/phx/releases) on Github.

## Features

- Supports websockets as the transport method. Longpoll is not currently supported, nor are there plans to implement it.
- Supports the JSONSerializerV2 serializer. (JSONSerializerV1 also available if preferred.)
- All event handlers are simple functions that are registered with the Socket, Channels or Pushes. No complicated
  interfaces to implement.
- Completely concurrent using many goroutines in the background so that your main thread is not blocked. All callbacks
  will run in separate goroutines, unless a Channel's `OrderedCallbacks` is set: then its callbacks run one at a time,
  those for messages from the server in the order the messages arrived (lifecycle callbacks such as OnJoin and
  OnError are not ordered relative to them).
- Supports setting connection parameters, headers, proxy, etc on the main websocket connection.
- Supports passing parameters when joining a Channel
- Pluggable Transport, TransportHandler, Logger if needed.

## Simple example

For a more complete example, see [examples/simple/simple.go](examples/simple/simple.go).
This example does not include error handling.

```go
// Connect to socket
endPoint, _ := url.Parse("ws://localhost:4000/socket?foo=bar")
socket := phx.NewSocket(endPoint)
_ = socket.Connect()

// Join a channel
channel := socket.Channel("test:lobby", nil)
join, _ := channel.Join()
join.Receive("ok", func(response any) {
  log.Println("Joined channel:", channel.Topic(), response)
})

// Send an event
ping, _ := channel.Push("ping", "pong")
ping.Receive("ok", func(response any) {
  log.Println("Ping:", response)
})

// Listen for pushes or broadcasts from the server
channel.On("shout", func(payload any) {
  log.Println("received shout:", payload)
})
```

## CLI example

There is also a simple CLI example for interactively using the library to connect to any server/channel in
[examples/cli/cli.go](examples/cli/cli.go).

## Testing

    go test -race ./...                       # unit tests, no extra tooling
    go test -tags integration -race ./...     # plus integration tests against a real Phoenix server

The integration tests start `integration/server.exs`, a single-file Phoenix application (Bandit, one socket with a
test channel), as a subprocess. They need `elixir` on the `PATH`; the first run fetches and compiles Phoenix. Set
`PHOENIX_VERSION` (a Hex requirement, default `~> 1.8`) to test against another Phoenix version. CI runs them against
`~> 1.7.0` and `~> 1.8.0`.

They cover joining (accepted and rejected), replies and error replies, push timeouts, JSON round trips including
unicode and 512 KiB payloads, concurrent pushes, broadcasts between clients, callback ordering, `Leave`, a channel
closed or crashed by the server, a socket dropped by the server, a restarted server, `Disconnect` followed by
`Connect`, the V1 serializer, a rejected connection and heartbeats.

## Not implemented currently:

- Longpoll transport.
- Binary messages. A binary frame from the server is treated as a read error, so the connection is dropped and
  re-established: do not use this client against channels that push `{:binary, data}` payloads.
- Authentication through the `authToken` subprotocol of the Phoenix JavaScript client, and connection parameters that
  are re-evaluated on every reconnect (a token in the endpoint URL is fixed when `Connect` is called).
