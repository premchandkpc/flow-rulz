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
		value := reflect.ValueOf(target)
		if value.Kind() != reflect.Pointer || value.IsNil() {
			return errors.New("scan target must be a non-nil pointer")
		}
		if r.values[i] == nil {
			value.Elem().Set(reflect.Zero(value.Elem().Type()))
			continue
		}
		source := reflect.ValueOf(r.values[i])
		if !source.Type().AssignableTo(value.Elem().Type()) {
			return errors.New("scan value type mismatch: " + source.Type().String() + " -> " + value.Elem().Type().String())
		}
		value.Elem().Set(source)
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
	repo := NewDomainServiceRepository(q)
	item := &domain.TenantDomain{
		ID: "domain-orders", TenantID: "tenant-a", Name: "orders",
		EventTypes: []string{"orders.created"},
		Deployments: []domain.ServiceDeployment{{
			ServiceID: "orders-worker", Name: "orders-worker", Revision: 3, Active: true,
			Rulesets:      []domain.RulesetAssignment{{RulesetID: "orders-created", Revision: 7}},
			WorkerProfile: domain.WorkerProfile{MinReplicas: 1, MaxReplicas: 4, Concurrency: 8},
		}},
	}
	if err := repo.SaveDomain(context.Background(), item); err != nil {
		t.Fatalf("SaveDomain() error = %v", err)
	}
	if len(q.args) != 7 {
		t.Fatalf("query args = %d, want 7", len(q.args))
	}
	encoded, ok := q.args[4].([]byte)
	if !ok {
		t.Fatalf("deployments argument type = %T, want []byte JSONB", q.args[4])
	}
	var got []domain.ServiceDeployment
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("deployment JSON is invalid: %v", err)
	}
	if !reflect.DeepEqual(got, item.Deployments) {
		t.Fatalf("persisted deployments = %#v, want %#v", got, item.Deployments)
	}
}

func TestSaveDomainPersistsEmptyDeploymentArrayAndWrapsDatabaseErrors(t *testing.T) {
	q := &domainDeploymentQuerier{}
	repo := NewDomainServiceRepository(q)
	item := &domain.TenantDomain{ID: "domain-orders", TenantID: "tenant-a", Name: "orders"}
	if err := repo.SaveDomain(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	encoded, ok := q.args[4].([]byte)
	if !ok || string(encoded) != "[]" {
		t.Fatalf("empty deployments = %q (%T), want [] JSON array", encoded, q.args[4])
	}

	dbErr := errors.New("database unavailable")
	q.execErr = dbErr
	if err := repo.SaveDomain(context.Background(), item); !errors.Is(err, dbErr) {
		t.Fatalf("SaveDomain() error = %v, want wrapped database error", err)
	}
}

func TestGetDomainRestoresDeploymentAssignmentsFromJSONB(t *testing.T) {
	deployments := []domain.ServiceDeployment{{
		ServiceID: "orders-worker", Name: "orders-worker", Revision: 3, Active: true,
		Rulesets:      []domain.RulesetAssignment{{RulesetID: "orders-created", Revision: 7}},
		WorkerProfile: domain.WorkerProfile{MinReplicas: 1, MaxReplicas: 4, Concurrency: 8},
	}}
	encoded, err := json.Marshal(deployments)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	q := &domainDeploymentQuerier{row: deploymentRow{values: []any{
		"domain-orders", "tenant-a", "orders", []string{"orders.created"}, encoded, now, now,
	}}}
	item, err := NewDomainServiceRepository(q).GetDomain(context.Background(), "tenant-a", "domain-orders")
	if err != nil {
		t.Fatalf("GetDomain() error = %v", err)
	}
	if item == nil || !reflect.DeepEqual(item.Deployments, deployments) {
		t.Fatalf("decoded domain deployments = %#v, want %#v", item, deployments)
	}
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
