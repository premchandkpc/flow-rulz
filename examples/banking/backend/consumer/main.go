// Command consumer runs the banking services that sit beside FlowRulz:
// fraud screening, compliance, and a ledger/ops/notification observer.
// FlowRulz rule sets route between them; these services only answer requests.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	flowrulz "github.com/flowrulz/flow-rulz-sdk/go/flowrulz"

	amlalert "github.com/flowrulz/flow-rulz-domains/banking/builds/go/aml_alert_raised"
	compliancecheck "github.com/flowrulz/flow-rulz-domains/banking/builds/go/compliance_check"
	complianceresult "github.com/flowrulz/flow-rulz-domains/banking/builds/go/compliance_result"
	fraudrequested "github.com/flowrulz/flow-rulz-domains/banking/builds/go/fraud_screening_requested"
	fraudresult "github.com/flowrulz/flow-rulz-domains/banking/builds/go/fraud_screening_result"
	notification "github.com/flowrulz/flow-rulz-domains/banking/builds/go/notification_requested"
	reviewrequired "github.com/flowrulz/flow-rulz-domains/banking/builds/go/transaction_review_required"
	settled "github.com/flowrulz/flow-rulz-domains/banking/builds/go/transaction_settled"
)

var sanctionedCountries = map[string]bool{"IR": true, "KP": true, "SY": true}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	client, err := flowrulz.New()
	if err != nil {
		return fmt.Errorf("create FlowRulz client: %w", err)
	}
	defer client.Close()

	subscriptions := []struct {
		name string
		run  func(context.Context) error
	}{
		{"fraud-screening", func(c context.Context) error { return fraudService(c, client) }},
		{"compliance", func(c context.Context) error { return complianceService(c, client) }},
		{"ledger", func(c context.Context) error { return ledgerService(c, client) }},
		{"operations", func(c context.Context) error { return operationsService(c, client) }},
		{"customer-care", func(c context.Context) error { return customerCareService(c, client) }},
		{"aml", func(c context.Context) error { return amlService(c, client) }},
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(subscriptions))
	for _, s := range subscriptions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errs <- fmt.Errorf("%s service: %w", s.name, err)
				cancel()
			}
		}()
		time.Sleep(300 * time.Millisecond) // stagger consumer creation on the stream
	}
	log.Printf("banking services listening for tenant %q", os.Getenv("FLOWRULZ_TENANT_ID"))
	wg.Wait()
	close(errs)
	return errors.Join(drain(errs)...)
}

func drain(ch <-chan error) []error {
	var out []error
	for err := range ch {
		out = append(out, err)
	}
	return out
}

func fraudService(ctx context.Context, client *flowrulz.Client) error {
	reply := fraudresult.NewClient(client)
	return fraudrequested.NewClient(client).Subscribe(ctx, func(ctx context.Context, req *fraudrequested.ScreeningRequested, env *flowrulz.Envelope) error {
		score, decision := scoreTransaction(req)
		log.Printf("[fraud] txn=%s amount=%.2f channel=%s country=%q -> score=%.0f decision=%s", req.TransactionId, req.Amount/100, req.Channel, req.CounterpartyCountry, score, decision)
		return reply.Send(ctx, &fraudresult.ScreeningResult{
			ScreeningId: req.ScreeningId, TransactionId: req.TransactionId, AccountId: req.AccountId,
			Amount: req.Amount, Currency: req.Currency, CounterpartyCountry: req.CounterpartyCountry,
			Score: score, Decision: decision, ScreenedAt: time.Now().UTC().Format(time.RFC3339),
		}, flowrulz.WithPartitionKey(env.PartitionKey))
	})
}

func scoreTransaction(req *fraudrequested.ScreeningRequested) (float64, string) {
	score := 10.0
	if req.Amount >= 1_000_000 {
		score += 25
	}
	if req.Channel == "api" {
		score += 15
	}
	if req.CounterpartyCountry != "" && req.CounterpartyCountry != "US" {
		score += 20
	}
	if sanctionedCountries[req.CounterpartyCountry] {
		score += 50
	}
	switch {
	case score >= 80:
		return score, "deny"
	case score >= 50:
		return score, "review"
	default:
		return score, "approve"
	}
}

func complianceService(ctx context.Context, client *flowrulz.Client) error {
	reply := complianceresult.NewClient(client)
	return compliancecheck.NewClient(client).Subscribe(ctx, func(ctx context.Context, req *compliancecheck.Check, env *flowrulz.Envelope) error {
		passed, level := !sanctionedCountries[req.CounterpartyCountry], "low"
		var flags []string
		if !passed {
			level, flags = "critical", []string{"sanctioned_country"}
		} else if req.Amount >= 5_000_000 {
			level, flags = "high", []string{"large_amount"}
		} else if req.Amount >= 1_000_000 {
			level = "medium"
		}
		log.Printf("[compliance] txn=%s factors=%v -> passed=%t risk=%s", req.TransactionId, req.RiskFactors, passed, level)
		return reply.Send(ctx, &complianceresult.Result{
			CheckId: req.CheckId, TransactionId: req.TransactionId, AccountId: req.AccountId,
			Amount: req.Amount, Currency: req.Currency, Passed: passed, RiskLevel: level, Flags: flags,
			ReviewedAt: time.Now().UTC().Format(time.RFC3339),
		}, flowrulz.WithPartitionKey(env.PartitionKey))
	})
}

func ledgerService(ctx context.Context, client *flowrulz.Client) error {
	return settled.NewClient(client).Subscribe(ctx, func(_ context.Context, s *settled.Settled, _ *flowrulz.Envelope) error {
		log.Printf("[ledger] posted txn=%s account=%s amount=%.2f %s", s.TransactionId, s.AccountId, s.Amount/100, s.Currency)
		return nil
	})
}

func operationsService(ctx context.Context, client *flowrulz.Client) error {
	return reviewrequired.NewClient(client).Subscribe(ctx, func(_ context.Context, r *reviewrequired.ReviewRequired, _ *flowrulz.Envelope) error {
		log.Printf("[operations] manual review txn=%s reason=%s", r.TransactionId, r.Reason)
		return nil
	})
}

func customerCareService(ctx context.Context, client *flowrulz.Client) error {
	return notification.NewClient(client).Subscribe(ctx, func(_ context.Context, n *notification.Requested, _ *flowrulz.Envelope) error {
		log.Printf("[customer-care] send %q to account=%s txn=%s", n.Template, n.AccountId, n.TransactionId)
		return nil
	})
}

func amlService(ctx context.Context, client *flowrulz.Client) error {
	return amlalert.NewClient(client).Subscribe(ctx, func(_ context.Context, a *amlalert.AlertRaised, _ *flowrulz.Envelope) error {
		log.Printf("[aml] alert reason=%s txn=%s account=%s amount=%.2f", a.Reason, a.TransactionId, a.AccountId, a.Amount/100)
		return nil
	})
}
