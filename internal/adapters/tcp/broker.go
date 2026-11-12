// Package tcp implements the framed JSON broker protocol over TCP.
package tcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/google/uuid"
)

const maxRequestBytes = 1 << 20
const maxResponseBytes = 2 << 20

func readFrame(reader io.Reader, limit uint32) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > limit {
		return nil, errors.New("broker frame length exceeds bounds")
	}
	data := make([]byte, length)
	_, err := io.ReadFull(reader, data)
	return data, err
}

func writeFrame(writer io.Writer, value any, limit int) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > limit {
		return errors.New("broker frame length exceeds bounds")
	}
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(data)))
	copy(frame[4:], data)
	_, err = io.Copy(writer, bytes.NewReader(frame))
	return err
}

func decode(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("expected one broker JSON document")
	}
	return nil
}

// Server serves bounded connections against the shared broker protocol.
type Server struct{ Protocol ports.BrokerProtocol }

// Serve owns all accepted connections and joins them before returning.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var workers sync.WaitGroup
	closeConnections := func() {
		mu.Lock()
		defer mu.Unlock()
		for conn := range connections {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Printf("close broker TCP connection: %v", err)
			}
		}
	}
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(stopped)
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("close broker TCP listener: %v", err)
		}
		closeConnections()
	})
	defer func() {
		cancel()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("close broker TCP listener: %v", err)
		}
		if !stop() {
			<-stopped
		}
		closeConnections()
		workers.Wait()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept broker TCP connection: %w", err)
		}
		mu.Lock()
		if len(connections) >= 64 || ctx.Err() != nil {
			mu.Unlock()
			if err := conn.Close(); err != nil {
				log.Printf("close excess broker TCP connection: %v", err)
			}
			continue
		}
		connections[conn] = struct{}{}
		mu.Unlock()
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() {
				if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					log.Printf("close broker TCP connection: %v", err)
				}
				mu.Lock()
				delete(connections, conn)
				mu.Unlock()
			}()
			s.serveConnection(ctx, conn)
		}()
	}
}

func (s *Server) serveConnection(ctx context.Context, conn net.Conn) {
	for ctx.Err() == nil {
		if err := conn.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return
		}
		data, err := readFrame(conn, maxRequestBytes)
		if err != nil {
			return
		}
		var request ports.BrokerRequest
		response := ports.BrokerResponse{Version: ports.BrokerProtocolVersion, Code: "invalid"}
		if err := decode(data, &request); err == nil {
			opCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
			response = s.Protocol.Execute(opCtx, request)
			cancel()
		}
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return
		}
		if err := writeFrame(conn, response, maxResponseBytes); err != nil {
			return
		}
	}
}

// Client reuses one connection and serializes calls. Close releases that connection.
type Client struct {
	Addr  string
	Token string
	mu    sync.Mutex
	conn  net.Conn
}

// Call executes one bounded request. A failed exchange discards the connection.
func (c *Client) Call(ctx context.Context, request ports.BrokerRequest) (ports.BrokerResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	request.Version = ports.BrokerProtocolVersion
	request.Token = c.Token
	if request.ID == "" {
		request.ID = uuid.NewString()
	}
	callCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	if c.conn == nil {
		var err error
		c.conn, err = (&net.Dialer{Timeout: 5 * time.Second}).DialContext(callCtx, "tcp", c.Addr)
		if err != nil {
			return ports.BrokerResponse{}, fmt.Errorf("connect broker TCP peer: %w", err)
		}
	}
	conn := c.conn
	deadline, _ := callCtx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return ports.BrokerResponse{}, c.discard(errors.Join(err, callCtx.Err()))
	}
	closed := make(chan struct{})
	stop := context.AfterFunc(callCtx, func() {
		defer close(closed)
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("cancel broker TCP call: %v", err)
		}
	})
	defer func() {
		if !stop() {
			<-closed
			if c.conn == conn {
				c.conn = nil
			}
		}
	}()
	if err := writeFrame(conn, request, maxRequestBytes); err != nil {
		return ports.BrokerResponse{}, c.discard(err)
	}
	data, err := readFrame(conn, maxResponseBytes)
	if err != nil {
		return ports.BrokerResponse{}, c.discard(errors.Join(err, callCtx.Err()))
	}
	var response ports.BrokerResponse
	if err := decode(data, &response); err != nil {
		return response, c.discard(err)
	}
	if response.Version != ports.BrokerProtocolVersion || response.ID != request.ID {
		return response, c.discard(errors.New("broker TCP response does not match request"))
	}
	return response, nil
}

func (c *Client) discard(err error) error {
	closeErr := c.conn.Close()
	c.conn = nil
	if errors.Is(closeErr, net.ErrClosed) {
		closeErr = nil
	}
	return errors.Join(err, closeErr)
}

// Close closes an established peer connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
