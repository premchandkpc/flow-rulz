package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	flowrulz "github.com/flowrulz/flow-rulz-sdk/go/flowrulz"

	orderplaced "github.com/flowrulz/flow-rulz-domains/food-delivery/builds/go/order_placed"
)

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

	typed := orderplaced.NewClient(client)
	log.Printf("Listening for %s events for tenant %q", orderplaced.DefaultRuleSet, os.Getenv("FLOWRULE_TENANT_ID"))
	err = typed.Subscribe(ctx, func(_ context.Context, order *orderplaced.Placed, envelope *flowrulz.Envelope) error {
		log.Printf(
			"Received %s event id=%s partition_key=%s order_id=%s customer_id=%s restaurant_id=%s total=%.2f %s",
			envelope.Type,
			envelope.ID,
			envelope.PartitionKey,
			order.OrderId,
			order.CustomerId,
			order.RestaurantId,
			order.TotalAmount/100,
			order.Currency,
		)
		return nil
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("subscribe to order.placed: %w", err)
	}
	return nil
}
