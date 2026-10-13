package main

import (
	"context"
	"fmt"
	"log"
	"time"

	flowrulz "github.com/flowrulz/flow-rulz-sdk/go/flowrulz"

	ordercreated "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/order_created"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
	}
}

func run() error {
	client, err := flowrulz.New()
	if err != nil {
		return fmt.Errorf("create FlowRulz client: %w", err)
	}
	defer client.Close()

	order := ordercreated.Created{
		OrderId:     "order-001",
		CustomerId:  "customer-123",
		TotalAmount: 25000,
		Currency:    "USD",
		Items: []ordercreated.Item{
			{ProductId: "prod-001", Quantity: 2, UnitPrice: 10000},
			{ProductId: "prod-002", Quantity: 1, UnitPrice: 5000},
		},
		ShippingAddress: ordercreated.ShippingAddress{
			Street:     "123 Main St",
			City:       "San Francisco",
			State:      "CA",
			PostalCode: "94102",
			Country:    "US",
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	typed := ordercreated.NewClient(client)
	if err := typed.Send(context.Background(), &order, flowrulz.WithPartitionKey(order.CustomerId)); err != nil {
		return fmt.Errorf("send order.created: %w", err)
	}

	fmt.Println("Order created event published successfully!")
	fmt.Printf("Type: %s\n", ordercreated.DefaultRuleSet)
	fmt.Printf("Partition key: %s\n", order.CustomerId)
	return nil
}
