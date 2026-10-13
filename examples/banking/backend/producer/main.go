// Command producer sends one banking transaction per scenario so every branch
// of the transaction rule sets can be observed.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	flowrulz "github.com/flowrulz/flow-rulz-sdk/go/flowrulz"

	transactioninitiated "github.com/flowrulz/flow-rulz-domains/banking/builds/go/transaction_initiated"
)

type scenario struct {
	name    string
	amount  float64
	kind    string
	channel string
	country string
}

var scenarios = []scenario{
	{"low-risk domestic payment", 12_500, "payment", "mobile", ""},
	{"mid-value domestic transfer", 250_000, "transfer", "web", "US"},
	{"structuring-sized transfer", 950_000, "transfer", "web", "US"},
	{"high-value transfer", 2_500_000, "transfer", "web", "US"},
	{"large wire needing review", 7_500_000, "wire_transfer", "branch", "GB"},
	{"international transfer", 400_000, "transfer", "mobile", "DE"},
	{"sanctioned destination", 600_000, "wire_transfer", "api", "IR"},
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	client, err := flowrulz.New()
	if err != nil {
		return fmt.Errorf("create FlowRulz client: %w", err)
	}
	defer client.Close()

	typed := transactioninitiated.NewClient(client)
	stamp := time.Now().UTC().Format("150405")
	for i, s := range scenarios {
		txn := transactioninitiated.Initiated{
			TransactionId:       fmt.Sprintf("txn-%s-%02d", stamp, i+1),
			AccountId:           fmt.Sprintf("account-%03d", 100+i),
			Amount:              s.amount,
			Currency:            "USD",
			TransactionType:     s.kind,
			Channel:             s.channel,
			CounterpartyCountry: s.country,
			CounterpartyAccount: fmt.Sprintf("ext-%04d", 9000+i),
			Description:         s.name,
			InitiatedAt:         time.Now().UTC().Format(time.RFC3339),
		}
		if err := typed.Send(context.Background(), &txn, flowrulz.WithPartitionKey(txn.AccountId)); err != nil {
			return fmt.Errorf("send %s: %w", s.name, err)
		}
		fmt.Printf("sent %-28s txn=%s amount=%.2f country=%q\n", s.name, txn.TransactionId, s.amount/100, s.country)
	}
	return nil
}
