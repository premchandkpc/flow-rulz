// Package effects provides implementations for sending effects to external destinations.
package effects

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

const maxDomainServicePayloadBytes = 1 << 20

// DomainServiceSender delivers effects to external HTTP endpoints.
type DomainServiceSender struct {
	services     ports.DomainServiceRepository
	client       *http.Client
	allowPrivate bool
}

// NewDomainServiceSender creates a new DomainServiceSender.
func NewDomainServiceSender(services ports.DomainServiceRepository, allowPrivate bool) *DomainServiceSender {
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return dialDomainService(ctx, network, address, allowPrivate)
		},
	}
	return &DomainServiceSender{
		services:     services,
		allowPrivate: allowPrivate,
		client: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Send delivers an effect to the configured domain service endpoint.
func (s *DomainServiceSender) Send(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	event, err := validateAndDecodeEffect(effect)
	if err != nil {
		return err
	}
	service, err := s.loadActiveService(ctx, effect)
	if err != nil {
		return err
	}
	target, err := s.validateServiceEndpoint(service.EndpointURL)
	if err != nil {
		return err
	}
	request, err := buildServiceRequest(ctx, target, effect, event, service.ID, idempotencyKey)
	if err != nil {
		return err
	}
	return s.deliverRequest(request, service.ID)
}

func validateAndDecodeEffect(effect *domain.Effect) (*domain.EventEnvelope, error) {
	if effect == nil {
		return nil, errors.New("send domain service event: effect is nil")
	}
	if effect.TenantID == "" || effect.Destination == "" || effect.Name == "" {
		return nil, errors.New("send domain service event: tenant, service, and event type are required")
	}
	if effect.EffectType != domain.EffectTypeServiceEvent {
		return nil, fmt.Errorf("send domain service event: unsupported effect type %q", effect.EffectType)
	}
	if len(effect.Payload) == 0 || len(effect.Payload) > maxDomainServicePayloadBytes {
		return nil, fmt.Errorf("send domain service event: payload must be between 1 and %d bytes", maxDomainServicePayloadBytes)
	}
	var event domain.EventEnvelope
	if err := json.Unmarshal(effect.Payload, &event); err != nil {
		return nil, fmt.Errorf("decode tenant service event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("validate tenant service event: %w", err)
	}
	if event.TenantID != effect.TenantID || event.Type != effect.Name {
		return nil, errors.New("send domain service event: event identity does not match outbox effect")
	}
	return &event, nil
}

func (s *DomainServiceSender) loadActiveService(
	ctx context.Context,
	effect *domain.Effect,
) (*domain.DomainService, error) {
	if s.services == nil {
		return nil, errors.New("send domain service event: service repository is not configured")
	}
	service, err := s.services.GetService(ctx, effect.TenantID, effect.Destination)
	if err != nil {
		return nil, fmt.Errorf("get subscribed tenant service: %w", err)
	}
	if service == nil || !service.Active {
		return nil, fmt.Errorf("subscribed tenant service %q is unavailable", effect.Destination)
	}
	return service, nil
}

func (s *DomainServiceSender) validateServiceEndpoint(endpoint string) (*url.URL, error) {
	target, err := url.Parse(endpoint)
	if err != nil || target.Hostname() == "" || !target.IsAbs() || target.Opaque != "" ||
		target.User != nil || target.Fragment != "" || len(endpoint) > 2048 {
		if err != nil {
			return nil, fmt.Errorf("parse tenant service endpoint: %w", err)
		}
		return nil, errors.New("tenant service endpoint is not a valid absolute URL")
	}
	if target.Scheme != "https" && (!s.allowPrivate || target.Scheme != "http") {
		return nil, fmt.Errorf("tenant service endpoint must use HTTPS")
	}
	return target, nil
}

func buildServiceRequest(
	ctx context.Context,
	target *url.URL,
	effect *domain.Effect,
	event *domain.EventEnvelope,
	serviceID string,
	idempotencyKey string,
) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(effect.Payload))
	if err != nil {
		return nil, fmt.Errorf("create tenant service request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	request.Header.Set("X-FlowRulz-Effect-ID", effect.ID)
	request.Header.Set("X-FlowRulz-Event-ID", event.ID)
	request.Header.Set("X-FlowRulz-Tenant-ID", effect.TenantID)
	request.Header.Set("X-FlowRulz-Service-ID", serviceID)
	return request, nil
}

func (s *DomainServiceSender) deliverRequest(request *http.Request, serviceID string) error {
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("deliver event to tenant service %q: %w", serviceID, err)
	}
	defer func() { _ = response.Body.Close() }()
	_, drainErr := io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if drainErr != nil {
			return fmt.Errorf("tenant service %q returned status %d; drain response: %w", serviceID, response.StatusCode, drainErr)
		}
		return fmt.Errorf("tenant service %q returned non-success status %d", serviceID, response.StatusCode)
	}
	if drainErr != nil {
		return fmt.Errorf("read tenant service %q response: %w", serviceID, drainErr)
	}
	return nil
}

func dialDomainService(ctx context.Context, network, address string, allowPrivate bool) (net.Conn, error) {
	host, port, err := parseDomainServiceAddress(address)
	if err != nil {
		return nil, err
	}
	ips, err := resolveDomainServiceIPs(ctx, host, allowPrivate)
	if err != nil {
		return nil, err
	}
	return dialResolvedDomainServiceIPs(ctx, network, port, ips)
}

func parseDomainServiceAddress(address string) (string, string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", "", fmt.Errorf("split tenant service endpoint: %w", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return "", "", fmt.Errorf("invalid tenant service endpoint port: %w", err)
	}
	if portNumber < 1 || portNumber > 65535 {
		return "", "", fmt.Errorf("invalid tenant service endpoint port %q", port)
	}
	return host, port, nil
}

func resolveDomainServiceIPs(ctx context.Context, host string, allowPrivate bool) ([]net.IP, error) {
	resolved, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant service endpoint: %w", err)
	}
	if len(resolved) == 0 {
		return nil, fmt.Errorf("tenant service endpoint %q resolved to no addresses", host)
	}
	ips := make([]net.IP, 0, len(resolved))
	for _, item := range resolved {
		if item.IP == nil || item.IP.IsUnspecified() || item.IP.IsMulticast() {
			return nil, fmt.Errorf("tenant service endpoint %q resolves to an invalid address", host)
		}
		if !allowPrivate && !isPublicIP(item.IP) {
			return nil, fmt.Errorf("tenant service endpoint %q resolves to a non-public address", host)
		}
		ips = append(ips, item.IP)
	}
	return ips, nil
}

func dialResolvedDomainServiceIPs(
	ctx context.Context,
	network, port string,
	ips []net.IP,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	var failures []error
	for _, ip := range ips {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		failures = append(failures, err)
	}
	return nil, fmt.Errorf("connect to tenant service endpoint: %w", errors.Join(failures...))
}

func isPublicIP(ip net.IP) bool {
	if isNonPublicAddress(ip) {
		return false
	}
	if ip4 := ip.To4(); ip4 != nil {
		return !isInCIDRList(ip4, reservedIPv4CIDRs)
	}
	return !isInCIDRList(ip, reservedIPv6CIDRs)
}

func isNonPublicAddress(ip net.IP) bool {
	return ip == nil || ip.IsPrivate() || ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() || !ip.IsGlobalUnicast()
}

var reservedIPv4CIDRs = []string{
	"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.88.99.0/24",
	"192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24",
	"203.0.113.0/24", "240.0.0.0/4",
}

var reservedIPv6CIDRs = []string{
	"::/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64",
	"2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20", "fec0::/10",
}

func isInCIDRList(ip net.IP, cidrs []string) bool {
	for _, cidr := range cidrs {
		_, network, err := net.ParseCIDR(cidr)
		if err == nil && network.Contains(ip) {
			return true
		}
	}
	return false
}

var _ ports.EffectSender = (*DomainServiceSender)(nil)
