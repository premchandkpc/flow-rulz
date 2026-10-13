package flowrulz

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

type httpTransport struct {
	cfg     *TransportConfig
	client  *http.Client
	baseURL string
	mu      sync.RWMutex
	closed  bool
}

func newHTTPTransport(cfg *TransportConfig) (Transport, error) {
	if cfg == nil || cfg.Config == nil {
		return nil, ErrConfig("HTTP transport config is required")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.Config.HTTPAddr), "/")
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, ErrConfig("HTTP_ADDR must be an absolute http or https URL")
	}

	return &httpTransport{
		cfg:     cfg,
		client:  &http.Client{Timeout: cfg.Config.HTTPTimeout},
		baseURL: baseURL,
	}, nil
}

func (t *httpTransport) Send(ctx context.Context, env *Envelope) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return ErrTransportClosed
	}
	return t.sendLocked(ctx, env)
}

// sendLocked performs one request while the caller holds t.mu.RLock. Keeping
// request execution separate prevents SendBatch from recursively acquiring the
// same RWMutex, which can deadlock when Close is waiting for the write lock.
func (t *httpTransport) sendLocked(ctx context.Context, env *Envelope) error {
	if env == nil {
		return ErrInvalidArgument("event envelope is required")
	}
	if strings.TrimSpace(env.Type) == "" {
		return ErrInvalidArgument("event type is required")
	}

	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	sendCtx, cancel := context.WithTimeout(ctx, t.cfg.Config.SendTimeout)
	defer cancel()

	endpoint := t.baseURL + "/v1/events/" + url.PathEscape(env.Type)
	req, err := http.NewRequestWithContext(sendCtx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if env.TenantID != "" {
		req.Header.Set("X-Tenant-ID", env.TenantID)
	}
	if env.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", env.CorrelationID)
	}
	if env.WorkflowID != "" {
		req.Header.Set("X-Workflow-ID", env.WorkflowID)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		// Do not include response bodies in errors: upstreams may echo secrets,
		// credentials, or tenant data into their error responses.
		return fmt.Errorf("HTTP %d response from FlowRulz API", resp.StatusCode)
	}
	return nil
}

func (t *httpTransport) SendBatch(ctx context.Context, envs []*Envelope) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return ErrTransportClosed
	}
	for i, env := range envs {
		if err := t.sendLocked(ctx, env); err != nil {
			return fmt.Errorf("send batch item %d: %w", i, err)
		}
	}
	return nil
}

func (t *httpTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true
	if t.client != nil {
		t.client.CloseIdleConnections()
	}
	return nil
}

func (t *httpTransport) Name() string {
	return "http"
}
