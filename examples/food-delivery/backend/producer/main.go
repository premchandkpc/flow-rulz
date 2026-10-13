package main

import (
	"context"
	"fmt"
	"log"
	"time"

	flowrulz "github.com/flowrulz/flow-rulz-sdk/go/flowrulz"

	orderplaced "github.com/flowrulz/flow-rulz-domains/food-delivery/builds/go/order_placed"
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

	order := orderplaced.Placed{
		OrderId:      "order-001",
		CustomerId:   "customer-789",
		RestaurantId: "restaurant-001",
		TotalAmount:  2800,
		Currency:     "USD",
		Items: []orderplaced.Item{
			{MenuItemId: "pizza-margherita", Quantity: 1, UnitPrice: 1800, Name: "Pizza Margherita"},
			{MenuItemId: "garlic-bread", Quantity: 2, UnitPrice: 500, Name: "Garlic Bread"},
		},
		DeliveryAddress: orderplaced.DeliveryAddress{
			Street:     "456 Oak Ave",
			City:       "San Francisco",
			State:      "CA",
			PostalCode: "94102",
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	typed := orderplaced.NewClient(client)
	if err := typed.Send(context.Background(), &order, flowrulz.WithPartitionKey(order.DeliveryAddress.PostalCode)); err != nil {
		return fmt.Errorf("send order.placed: %w", err)
	}

	fmt.Println("Order placed event published successfully!")
	fmt.Printf("Type: %s\n", orderplaced.DefaultRuleSet)
	fmt.Printf("Partition key: %s\n", order.DeliveryAddress.PostalCode)
	return nil
}
