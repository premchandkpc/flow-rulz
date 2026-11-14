package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type deploymentRow struct {
	values []any
	err    error
}

func (r deploymentRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("unexpected scan arity")
	}
	for i, target := range dest {
		if err := assignScanValue(target, r.values[i]); err != nil {
			return err
		}
	}
	return nil
}

type domainDeploymentQuerier struct {
	args    []any
	execErr error
	row     pgx.Row
}

func (q *domainDeploymentQuerier) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	q.args = append([]any(nil), args...)
	return pgconn.CommandTag{}, q.execErr
}

func (*domainDeploymentQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("Query not expected in this test")
}

func (q *domainDeploymentQuerier) QueryRow(context.Context, string, ...any) pgx.Row {
	return q.row
}

func TestSaveDomainPersistsDeploymentAssignmentsAsJSONB(t *testing.T) {
	q := &domainDeploymentQuerier{}
	item := &domain.TenantDomain{
		ID: "domain-orders", TenantID: "tenant-a", Name: "orders",
		EventTypes: []string{"orders.created"}, Deployments: sampleDeployments(),
	}
	if err := NewDomainServiceRepository(q).SaveDomain(context.Background(), item); err != nil {
		t.Fatalf("SaveDomain() error = %v", err)
	}
	assertDeploymentJSON(t, q.args, item.Deployments)
}

func TestSaveDomainPersistsEmptyDeploymentArray(t *testing.T) {
	q := &domainDeploymentQuerier{}
	item := &domain.TenantDomain{ID: "domain-orders", TenantID: "tenant-a", Name: "orders"}
	if err := NewDomainServiceRepository(q).SaveDomain(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	encoded, ok := q.args[4].([]byte)
	if !ok || string(encoded) != "[]" {
		t.Fatalf("empty deployments = %q (%T), want [] JSON array", encoded, q.args[4])
	}
}

func TestSaveDomainWrapsDatabaseErrors(t *testing.T) {
	dbErr := errors.New("database unavailable")
	q := &domainDeploymentQuerier{execErr: dbErr}
	item := &domain.TenantDomain{ID: "domain-orders", TenantID: "tenant-a", Name: "orders"}
	if err := NewDomainServiceRepository(q).SaveDomain(context.Background(), item); !errors.Is(err, dbErr) {
		t.Fatalf("SaveDomain() error = %v, want wrapped database error", err)
	}
}

func TestGetDomainRestoresDeploymentAssignmentsFromJSONB(t *testing.T) {
	item, err := NewDomainServiceRepository(deploymentQuery(t)).GetDomain(context.Background(), "tenant-a", "domain-orders")
	if err != nil {
		t.Fatalf("GetDomain() error = %v", err)
	}
	if item == nil {
		t.Fatal("decoded domain is nil")
	}
	assertDeploymentsEqual(t, item.Deployments, sampleDeployments())
}

func TestGetDomainRejectsMalformedDeploymentJSON(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	q := &domainDeploymentQuerier{row: deploymentRow{values: []any{
		"domain-orders", "tenant-a", "orders", []string{"orders.created"}, []byte("{"), now, now,
	}}}
	if _, err := NewDomainServiceRepository(q).GetDomain(context.Background(), "tenant-a", "domain-orders"); err == nil {
		t.Fatal("GetDomain() error = nil, want malformed JSON error")
	}
}

func assignScanValue(target, source any) error {
	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return errors.New("scan target must be a non-nil pointer")
	}
	if source == nil {
		value.Elem().Set(reflect.Zero(value.Elem().Type()))
		return nil
	}
	return assignReflectedValue(value.Elem(), reflect.ValueOf(source))
}

func assignReflectedValue(target, source reflect.Value) error {
	if !source.Type().AssignableTo(target.Type()) {
		return errors.New("scan value type mismatch: " + source.Type().String() + " -> " + target.Type().String())
	}
	target.Set(source)
	return nil
}

func sampleDeployments() []domain.ServiceDeployment {
	return []domain.ServiceDeployment{{
		ServiceID: "orders-worker", Name: "orders-worker", Revision: 3, Active: true,
		Rulesets:      []domain.RulesetAssignment{{RulesetID: "orders-created", Revision: 7}},
		WorkerProfile: domain.WorkerProfile{MinReplicas: 1, MaxReplicas: 4, Concurrency: 8},
	}}
}

func assertDeploymentJSON(t *testing.T, args []any, want []domain.ServiceDeployment) {
	t.Helper()
	if len(args) != 7 {
		t.Fatalf("query args = %d, want 7", len(args))
	}
	encoded, ok := args[4].([]byte)
	if !ok {
		t.Fatalf("deployments argument type = %T, want []byte JSONB", args[4])
	}
	var got []domain.ServiceDeployment
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("deployment JSON is invalid: %v", err)
	}
	assertDeploymentsEqual(t, got, want)
}

func assertDeploymentsEqual(t *testing.T, got, want []domain.ServiceDeployment) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted deployments = %#v, want %#v", got, want)
	}
}

func deploymentQuery(t *testing.T) *domainDeploymentQuerier {
	t.Helper()
	encoded, err := json.Marshal(sampleDeployments())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	return &domainDeploymentQuerier{row: deploymentRow{values: []any{
		"domain-orders", "tenant-a", "orders", []string{"orders.created"}, encoded, now, now,
	}}}
}
