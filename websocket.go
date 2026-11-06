package phx

import (
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"net"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"
)

// Websocket is a Transport that connects to the server via Websockets. It is based on
// [gorilla.Websocket](https://pkg.go.dev/github.com/gorilla/websocket). The Dialer defaults to websocket.DefaultDialer.
type Websocket struct {
	Dialer          *websocket.Dialer
	Handler         TransportHandler
	conn            *websocket.Conn
	endPoint        *url.URL
	requestHeader   http.Header
	connectTimeout  time.Duration
	done            chan any
	close           chan bool
	reconnect       chan bool
	closeMsg        chan bool
	send            chan []byte
	mu              sync.RWMutex
	started         bool
	closing         bool
	reconnecting    bool
	waitingForClose bool
}

// NewWebsocket creates a Websocket with its own copy of websocket.DefaultDialer, so configuring its Dialer (a proxy,
// subprotocols, TLS settings) affects only this Websocket and never the rest of the process.
func NewWebsocket(handler TransportHandler) *Websocket {
	dialer := *websocket.DefaultDialer
	return &Websocket{
		Dialer:  &dialer,
		Handler: handler,
	}
}

// implements Transport

func (w *Websocket) Connect(endPoint *url.URL, requestHeader http.Header, connectTimeout time.Duration) error {
	if w.isStarted() {
		return errors.New("connect was already called")
	}

	// Copy the passed in endpoint so we can modify it
	newEndpoint := *endPoint

	newEndpoint.Path = path.Join(newEndpoint.Path, "websocket")

	if newEndpoint.Scheme == "" {
		if newEndpoint.Port() == "443" {
			newEndpoint.Scheme = "wss"
		} else {
			newEndpoint.Scheme = "ws"
		}
	} else if newEndpoint.Scheme == "http" {
		newEndpoint.Scheme = "ws"
	} else if newEndpoint.Scheme == "https" {
		newEndpoint.Scheme = "wss"
	}

	if newEndpoint.Scheme != "ws" && newEndpoint.Scheme != "wss" {
		return errors.New("invalid scheme for websocket transport, must be 'ws://' or 'wss://'")
	}

	w.endPoint = &newEndpoint
	w.requestHeader = requestHeader
	w.connectTimeout = connectTimeout

	w.startup()
	return nil
}

func (w *Websocket) Disconnect() error {
	if !w.isStarted() {
		return errors.New("not connected")
	}

	if w.connIsSet() {
		w.sendClose()
	} else {
		w.shutdown()
	}
	return nil
}

func (w *Websocket) Reconnect() error {
	if !w.isStarted() {
		return errors.New("not connected")
	}

	w.sendReconnect()
	return nil
}

func (w *Websocket) IsConnected() bool {
	return w.connIsReady()
}

func (w *Websocket) ConnectionState() ConnectionState {
	if w.connIsReady() {
		return ConnectionOpen
	} else if !w.isStarted() {
		return ConnectionClosed
	} else if w.isClosing() {
		return ConnectionClosing
	} else {
		return ConnectionConnecting
	}
}

func (w *Websocket) Send(msg []byte) error {
	w.mu.RLock()
	if w.closing {
		w.mu.RUnlock()
		return errors.New("cannot Send when closing connection")
	}
	if !w.started {
		w.mu.RUnlock()
		return errors.New("cannot Send when not connected or connecting")
	}
	done, send := w.done, w.send
	w.mu.RUnlock()

	select {
	case send <- msg:
		return nil
	case <-done:
		return errors.New("cannot Send after connection is shut down")
	}
}

func (w *Websocket) startup() {
	// Every start gets its own channels, and each goroutine below keeps the ones it was started with. A goroutine of
	// an earlier session that is still winding down must never pick up the channels of the next session, or it
	// would carry on next to the new goroutines.
	done := make(chan any)
	// close, closeMsg and reconnect carry at most one pending request each and are never sent to blockingly, see
	// signal.
	closeReq := make(chan bool, 1)
	closeMsg := make(chan bool, 1)
	reconnectReq := make(chan bool, 1)
	send := make(chan []byte, messageQueueLength)

	w.mu.Lock()
	w.done = done
	w.close = closeReq
	w.closeMsg = closeMsg
	w.reconnect = reconnectReq
	w.send = send
	w.reconnecting = false
	w.closing = false
	w.started = true
	w.mu.Unlock()

	go w.connectionManager(done, closeReq, reconnectReq, closeMsg)
	go w.connectionWriter(done, send)
	go w.connectionReader(done, closeMsg)
}

func (w *Websocket) shutdown() {
	w.shutdownSession(nil)
}

// shutdownSession ends the current session. With a non-nil done it only does so if that is still the current
// session's channel, so a goroutine of an earlier session cannot end the session that replaced it.
func (w *Websocket) shutdownSession(done chan any) {

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.started || (done != nil && w.done != done) {
		return
	}

	// Tell the goroutines to exit. Only the done channel is closed; the
	// signal channels (close, closeMsg, reconnect, send) are left open so a
	// concurrent sender can never panic with "send on closed channel" — every
	// send on them selects against done instead.
	close(w.done)

	w.started = false
	w.reconnecting = false
	w.closing = false
}

// errSessionEnded is returned by dial when the session it was started for has ended while it was dialing.
var errSessionEnded = errors.New("websocket session ended")

// dial connects to the server. It stops waiting as soon as done is closed, so a Disconnect does not have to wait for
// the handshake timeout. gorilla/websocket cannot abort a handshake that is already waiting for the server's answer,
// so the dial itself is left to finish on its own, and closes its connection if it completes after all.
func (w *Websocket) dial(done chan any) error {
	// Dial with a copy, so the timeout never gets written to a Dialer the caller supplied.
	dialer := *w.Dialer
	dialer.HandshakeTimeout = w.connectTimeout
	endPoint := w.endPoint.String()
	requestHeader := w.requestHeader

	type result struct {
		conn *websocket.Conn
		err  error
	}
	results := make(chan result, 1)
	go func() {
		conn, _, err := dialer.Dial(endPoint, requestHeader)
		results <- result{conn, err}
	}()

	var conn *websocket.Conn
	select {
	case <-done:
		go func() {
			if r := <-results; r.conn != nil {
				_ = r.conn.Close()
			}
		}()
		return errSessionEnded
	case r := <-results:
		if r.err != nil {
			return r.err
		}
		conn = r.conn
	}

	w.mu.Lock()
	if w.done != done || !w.started {
		w.mu.Unlock()
		_ = conn.Close()
		return errSessionEnded
	}
	w.mu.Unlock()

	w.setConn(conn)
	w.setReconnecting(false)
	w.Handler.onConnOpen()

	return nil
}

func (w *Websocket) closeConn(closeMsg <-chan bool) {

	if conn := w.getConn(); conn != nil {
		// attempt to gracefully close the connection by sending a close websocket message
		data := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
		deadline := time.Now().Add(2 * time.Second)
		// Forget a close frame left over from an earlier handshake that nobody was waiting for any more.
		select {
		case <-closeMsg:
		default:
		}
		w.setWaitingForClose(true)
		err := conn.WriteControl(websocket.CloseMessage, data, deadline)
		if err == nil {
			// Wait for a close message to be received by `connectionReader`, or time out after 2 seconds
			w.setWaitingForClose(true)
			select {
			case <-closeMsg:
			case <-time.After(2 * time.Second):
			}
		}
		w.setWaitingForClose(false)
	}

	// The reader may have dropped the connection while we waited, so look again.
	if conn := w.getConn(); conn != nil {
		// The reader may have closed the connection already after a read error.
		err := conn.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			w.Handler.onConnError(err)
		}

		w.setConn(nil)
	}

	w.Handler.onConnClose()
}

// errConnNotOpen is returned when there is no connection that is ready for use.
var errConnNotOpen = errors.New("connection is not open")

func (w *Websocket) writeToConn(data []byte) error {
	conn := w.readyConn()
	if conn == nil {
		return errConnNotOpen
	}

	return conn.WriteMessage(websocket.TextMessage, data)
}

// readFromConn reads the next message. It also returns the connection it read from, so that the caller deals with
// that very connection when the read fails, even if a new one has been dialed since.
func (w *Websocket) readFromConn() (*websocket.Conn, []byte, error) {
	conn := w.readyConn()
	if conn == nil {
		return nil, nil, errConnNotOpen
	}

	messageType, data, err := conn.ReadMessage()
	if err != nil {
		return conn, nil, err
	}
	if messageType != websocket.TextMessage {
		return conn, nil, errors.New(fmt.Sprint("Got unsupported websocket message type ", messageType))
	}

	return conn, data, nil
}

func (w *Websocket) connectionManager(done chan any, closeReq, reconnectReq, closeMsg chan bool) {
	//defer fmt.Println("connectionManager stopped")

	// closedByReconnect is set when a reconnect has closed the connection and no new one has been dialed since. A
	// Disconnect that arrived during that reconnect must not close (and report closing) the same connection again.
	closedByReconnect := false
	connectionTries := 0

	for {
		// Check if we have been told to finish
		select {
		case <-done:
			return
		default:
		}

		if !w.isClosing() && !w.connIsSet() {
			err := w.dial(done)
			if err != nil {
				if errors.Is(err, errSessionEnded) {
					return
				}
				w.Handler.onConnError(err)
				w.setReconnecting(true)
				connectionTries++
				delay := w.Handler.reconnectAfter(connectionTries)
				select {
				case <-done:
				case <-time.After(delay):
				}
				continue
			}
			closedByReconnect = false
		}

		select {
		case <-done:
			return
		case <-closeReq:
			if !closedByReconnect {
				w.closeConn(closeMsg)
			}
			w.shutdownSession(done)
		case <-reconnectReq:
			w.closeConn(closeMsg)
			closedByReconnect = true
		}
	}
}

func (w *Websocket) connectionWriter(done chan any, send chan []byte) {
	//defer fmt.Println("connectionWriter stopped")

	// pending is a message already taken off the queue that could not be written yet because the connection was not
	// ready. It is kept, never dropped, and written first once the connection is ready again.
	var pending []byte

	for {
		// Check if we have been told to finish
		select {
		case <-done:
			return
		default:
		}

		if !w.connIsReady() {
			time.Sleep(busyWait)
			continue
		}

		if pending == nil {
			select {
			case <-done:
				return
			case pending = <-send:
			}
		}

		// The connection may have stopped being ready while we waited for a message.
		if !w.connIsReady() {
			time.Sleep(busyWait)
			continue
		}

		// Send the message
		data := pending
		pending = nil
		err := w.writeToConn(data)

		if errors.Is(err, errConnNotOpen) {
			// Nothing was written, so keep the message for the next connection.
			pending = data
			time.Sleep(busyWait)
			continue
		}

		// If there were any other errors sending, then tell the connectionManager to reconnect. The message may
		// have been partly written, so it is not sent again.
		if err != nil {
			w.Handler.onWriteError(err)
			w.sendReconnect()
			time.Sleep(busyWait)
			continue
		}
	}
}

func (w *Websocket) connectionReader(done chan any, closeMsg chan bool) {
	//defer fmt.Println("connectionReader stopped")

	for {
		// Check if we have been told to finish
		select {
		case <-done:
			return
		default:
		}

		// Wait until we're connected
		if !w.connIsReady() {
			time.Sleep(busyWait)
			continue
		}

		// Read the next message from the websocket. This blocks until there is a message or error
		conn, data, err := w.readFromConn()

		// If there were any errors, tell the connectionManager to reconnect
		if err != nil {
			// The error of a session that has ended is of no interest to the session that replaced it.
			select {
			case <-done:
				if conn != nil {
					_ = conn.Close()
				}
				return
			default:
			}

			if websocket.IsCloseError(err, 1000) && w.isWaitingForClose() {
				// tell the connectionManager that we got the close message
				select {
				case w.closeMsg <- true:
				case <-done:
					return
				}
			} else {
				w.Handler.onReadError(err)
				// The connection is dead: close it, so it does not hold its socket until garbage collection, and
				// forget it unless a newer connection has replaced it already.
				if conn != nil {
					_ = conn.Close()
					w.clearConn(conn)
				}
				w.sendReconnect()
			}

			time.Sleep(busyWait)
			continue
		}

		w.Handler.onConnMessage(data)
	}
}

func (w *Websocket) setStarted(started bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.started = started
}

func (w *Websocket) isStarted() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.started
}

func (w *Websocket) setClosing(closing bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.closing = closing
}

func (w *Websocket) isClosing() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.closing
}

func (w *Websocket) sendClose() {
	w.mu.Lock()

	if w.closing || !w.started {
		w.mu.Unlock()
		return
	}

	w.closing = true
	signal(w.close)
	w.mu.Unlock()
}

func (w *Websocket) setReconnecting(reconnecting bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.reconnecting = reconnecting
}

func (w *Websocket) isReconnecting() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.reconnecting
}

func (w *Websocket) sendReconnect() {
	w.mu.Lock()

	if w.reconnecting || w.closing || !w.started {
		w.mu.Unlock()
		return
	}

	w.reconnecting = true
	signal(w.reconnect)
	w.mu.Unlock()
}

// signal queues a request for the connectionManager without blocking. The callers hold w.mu, which the
// connectionManager may itself be waiting for, so a blocking send here could deadlock. If a request is already
// pending, the manager has yet to act on it and the new one is redundant.
func signal(ch chan bool) {
	select {
	case ch <- true:
	default:
	}
}

func (w *Websocket) setConn(conn *websocket.Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.conn = conn
}

// clearConn forgets conn, unless a newer connection has replaced it in the meantime.
func (w *Websocket) clearConn(conn *websocket.Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn == conn {
		w.conn = nil
	}
}

// getConn returns the current connection, or nil. The connection is read under the mutex, but used outside it:
// reads and writes on it block, and must not hold up the other goroutines. gorilla/websocket allows one reader and
// one writer at a time, plus Close and WriteControl from any goroutine.
func (w *Websocket) getConn() *websocket.Conn {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.conn
}

// readyConn returns the current connection if it is ready for use (see connIsReady), or nil.
func (w *Websocket) readyConn() *websocket.Conn {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if !w.started || w.closing || w.reconnecting {
		return nil
	}
	return w.conn
}

func (w *Websocket) connIsSet() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.conn != nil
}

func (w *Websocket) connIsReady() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.started && !w.closing && !w.reconnecting && w.conn != nil
}

func (w *Websocket) setWaitingForClose(waitingForClose bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.waitingForClose = waitingForClose
}

func (w *Websocket) isWaitingForClose() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.waitingForClose
}
