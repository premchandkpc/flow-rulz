package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	flowrulz "github.com/flowrulz/flow-rulz-sdk/go/flowrulz"

	ordercreated "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/order_created"
	paymentcompleted "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/payment_completed"
	fraudresult "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/fraud_result"
	fraudreview "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/fraud_review"
	orderconfirmed "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/order_confirmed"
	shipmentrequested "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/shipment_requested"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	client, err := flowrulz.New()
	if err != nil {
		return fmt.Errorf("create FlowRulz client: %w", err)
	}
	defer client.Close()

	fmt.Println("=== E-Commerce Consumer Starting ===")
	fmt.Println("Listening for events...")

	// Subscribe to order.created events
	orderClient := ordercreated.NewClient(client)
	go func() {
		if err := orderClient.Subscribe(ctx, handleOrderCreated); err != nil {
			log.Printf("order.created subscription error: %v", err)
		}
	}()
	fmt.Println("✓ Subscribed to order.created")

	// Subscribe to payment.completed events
	paymentClient := paymentcompleted.NewClient(client)
	go func() {
		if err := paymentClient.Subscribe(ctx, handlePaymentCompleted); err != nil {
			log.Printf("payment.completed subscription error: %v", err)
		}
	}()
	fmt.Println("✓ Subscribed to payment.completed")

	// Subscribe to fraud.result events
	fraudClient := fraudresult.NewClient(client)
	go func() {
		if err := fraudClient.Subscribe(ctx, handleFraudResult); err != nil {
			log.Printf("fraud.result subscription error: %v", err)
		}
	}()
	fmt.Println("✓ Subscribed to fraud.result")

	// Subscribe to order.confirmed events
	confirmedClient := orderconfirmed.NewClient(client)
	go func() {
		if err := confirmedClient.Subscribe(ctx, handleOrderConfirmed); err != nil {
			log.Printf("order.confirmed subscription error: %v", err)
		}
	}()
	fmt.Println("✓ Subscribed to order.confirmed")

	// Subscribe to shipment.requested events
	shipmentClient := shipmentrequested.NewClient(client)
	go func() {
		if err := shipmentClient.Subscribe(ctx, handleShipmentRequested); err != nil {
			log.Printf("shipment.requested subscription error: %v", err)
		}
	}()
	fmt.Println("✓ Subscribed to shipment.requested")

	// Subscribe to fraud.review events
	reviewClient := fraudreview.NewClient(client)
	go func() {
		if err := reviewClient.Subscribe(ctx, handleFraudReview); err != nil {
			log.Printf("fraud.review subscription error: %v", err)
		}
	}()
	fmt.Println("✓ Subscribed to fraud.review")

	// Wait for shutdown signal
	<-ctx.Done()
	fmt.Println("\n=== Shutting down consumer ===")
	return nil
}

func handleOrderCreated(ctx context.Context, order *ordercreated.Created, envelope *flowrulz.Envelope) error {
	fmt.Printf("\n📦 [ORDER CREATED] %s\n", envelope.ID)
	fmt.Printf("   Order ID: %s\n", order.OrderId)
	fmt.Printf("   Customer: %s\n", order.CustomerId)
	fmt.Printf("   Amount: $%.2f %s\n", float64(order.TotalAmount)/100, order.Currency)
	fmt.Printf("   Items: %d\n", len(order.Items))
	fmt.Printf("   Partition Key: %s\n", envelope.PartitionKey)
	fmt.Printf("   Timestamp: %s\n", envelope.OccurredAt.Format(time.RFC3339))

	// Business logic: Check if high value
	if order.TotalAmount >= 50000 {
		fmt.Printf("   ⚠️  HIGH VALUE ORDER - Will trigger fraud check after payment\n")
	}

	return nil
}

func handlePaymentCompleted(ctx context.Context, payment *paymentcompleted.Completed, envelope *flowrulz.Envelope) error {
	fmt.Printf("\n💳 [PAYMENT COMPLETED] %s\n", envelope.ID)
	fmt.Printf("   Payment ID: %s\n", payment.PaymentId)
	fmt.Printf("   Order ID: %s\n", payment.OrderId)
	fmt.Printf("   Customer: %s\n", payment.CustomerId)
	fmt.Printf("   Amount: $%.2f %s\n", float64(payment.Amount)/100, payment.Currency)
	fmt.Printf("   Transaction ID: %s\n", payment.TransactionId)
	fmt.Printf("   Completed At: %s\n", payment.CompletedAt)
	fmt.Printf("   Partition Key: %s\n", envelope.PartitionKey)

	// Business logic: High value triggers fraud check
	if payment.Amount >= 50000 {
		fmt.Printf("   ⚠️  HIGH VALUE PAYMENT - Fraud check will be triggered\n")
	}

	return nil
}

func handleFraudResult(ctx context.Context, result *fraudresult.Result, envelope *flowrulz.Envelope) error {
	fmt.Printf("\n🔍 [FRAUD RESULT] %s\n", envelope.ID)
	fmt.Printf("   Check ID: %s\n", result.CheckId)
	fmt.Printf("   Order ID: %s\n", result.OrderId)
	fmt.Printf("   Passed: %v\n", result.Passed)
	fmt.Printf("   Risk Score: %.2f%%\n", result.RiskScore*100)
	if len(result.Flags) > 0 {
		fmt.Printf("   Flags: %v\n", result.Flags)
	}
	fmt.Printf("   Partition Key: %s\n", envelope.PartitionKey)

	if !result.Passed {
		fmt.Printf("   ❌ FRAUD CHECK FAILED - Manual review required\n")
	} else {
		fmt.Printf("   ✅ FRAUD CHECK PASSED - Order can proceed to shipment\n")
	}

	return nil
}

func handleOrderConfirmed(ctx context.Context, confirmed *orderconfirmed.Confirmed, envelope *flowrulz.Envelope) error {
	fmt.Printf("\n✅ [ORDER CONFIRMED] %s\n", envelope.ID)
	fmt.Printf("   Order ID: %s\n", confirmed.OrderId)
	fmt.Printf("   Customer: %s\n", confirmed.CustomerId)
	fmt.Printf("   Confirmed At: %s\n", confirmed.ConfirmedAt)
	fmt.Printf("   Partition Key: %s\n", envelope.PartitionKey)
	return nil
}

func handleShipmentRequested(ctx context.Context, shipment *shipmentrequested.Requested, envelope *flowrulz.Envelope) error {
	fmt.Printf("\n📦 [SHIPMENT REQUESTED] %s\n", envelope.ID)
	fmt.Printf("   Shipment ID: %s\n", shipment.ShipmentId)
	fmt.Printf("   Order ID: %s\n", shipment.OrderId)
	fmt.Printf("   Customer: %s\n", shipment.CustomerId)
	fmt.Printf("   Warehouse: %s\n", shipment.WarehouseId)
	fmt.Printf("   Method: %s\n", shipment.ShippingMethod)
	fmt.Printf("   Partition Key: %s\n", envelope.PartitionKey)
	fmt.Printf("   ✅ Order ready for fulfillment\n")
	return nil
}

func handleFraudReview(ctx context.Context, review *fraudreview.Review, envelope *flowrulz.Envelope) error {
	fmt.Printf("\n👤 [FRAUD REVIEW] %s\n", envelope.ID)
	fmt.Printf("   Order ID: %s\n", review.OrderId)
	fmt.Printf("   Check ID: %s\n", review.CheckId)
	fmt.Printf("   Risk Score: %.2f%%\n", review.RiskScore*100)
	if len(review.Flags) > 0 {
		fmt.Printf("   Flags: %v\n", review.Flags)
	}
	fmt.Printf("   Customer ID: %s\n", review.CustomerId)
	fmt.Printf("   Created At: %s\n", review.CreatedAt)
	fmt.Printf("   Partition Key: %s\n", envelope.PartitionKey)
	fmt.Printf("   ⚖️  Manual review received - will trigger approve/reject logic\n")
	return nil
}