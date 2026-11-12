package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/google/uuid"
)

// Client sends shared broker RPCs to a configured HTTP peer.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

// NewClient validates the peer URL and creates a bounded reusable HTTP client.
func NewClient(baseURL, token string) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("broker HTTP output requires an absolute HTTP(S) URL without credentials, query or fragment")
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), token: token, http: &http.Client{Timeout: 25 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Call executes one broker request. Acceptance is verified from the response body.
func (c *Client) Call(ctx context.Context, request ports.BrokerRequest) (result ports.BrokerResponse, callErr error) {
	request.Version = ports.BrokerProtocolVersion
	request.Token = c.token
	if request.ID == "" {
		request.ID = uuid.NewString()
	}
	data, err := json.Marshal(request)
	if err != nil {
		return ports.BrokerResponse{}, err
	}
	if len(data) > 1<<20 {
		return ports.BrokerResponse{}, errors.New("broker HTTP request exceeds 1 MiB")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/broker/rpc", bytes.NewReader(data))
	if err != nil {
		return ports.BrokerResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return ports.BrokerResponse{}, err
	}
	defer func() { callErr = errors.Join(callErr, resp.Body.Close()) }()
	if resp.StatusCode != http.StatusOK {
		return ports.BrokerResponse{}, fmt.Errorf("broker HTTP status %d", resp.StatusCode)
	}
	data, err = io.ReadAll(io.LimitReader(resp.Body, 2<<20+1))
	if err != nil {
		return ports.BrokerResponse{}, err
	}
	if len(data) > 2<<20 {
		return ports.BrokerResponse{}, errors.New("broker HTTP response exceeds 2 MiB")
	}
	var response ports.BrokerResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return response, err
	}
	if response.Version != ports.BrokerProtocolVersion || response.ID != request.ID {
		return response, errors.New("broker HTTP response does not match request")
	}
	return response, nil
}

// Close releases idle HTTP connections.
func (c *Client) Close() error { c.http.CloseIdleConnections(); return nil }
