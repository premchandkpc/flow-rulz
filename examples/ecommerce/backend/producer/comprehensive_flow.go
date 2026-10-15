package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	flowrulz "github.com/flowrulz/flow-rulz-sdk/go/flowrulz"

	fraudresult "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/fraud_result"
	fraudreview "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/fraud_review"
	ordercreated "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/order_created"
	paymentcompleted "github.com/flowrulz/flow-rulz-domains/ecommerce/builds/go/payment_completed"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	client, err := flowrulz.New()
	if err != nil {
		return fmt.Errorf("create FlowRulz client: %w", err)
	}
	defer client.Close()

	ctx := context.Background()

	// =====================================================================
	// SCENARIO 1: Standard Order Flow (with payment)
	// =====================================================================
	fmt.Println("=== SCENARIO 1: Standard Order Flow ===")

	// 1. Create order
	order := ordercreated.Created{
		OrderId:     "order-001",
		CustomerId:  "customer-123",
		TotalAmount: 25000, // $250.00
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

	orderClient := ordercreated.NewClient(client)
	if err := orderClient.Send(ctx, &order, flowrulz.WithPartitionKey(order.CustomerId)); err != nil {
		return fmt.Errorf("send order.created: %w", err)
	}
	fmt.Printf("✓ Order created: %s (customer: %s, amount: $%.2f)\n", order.OrderId, order.CustomerId, float64(order.TotalAmount)/100)

	// Wait for rule processing
	time.Sleep(500 * time.Millisecond)

	// 2. Simulate payment completion (this would normally come from payment provider)
	payment := paymentcompleted.Completed{
		PaymentId:     "pay-001",
		OrderId:       "order-001",
		CustomerId:    "customer-123",
		Amount:        25000,
		Currency:      "USD",
		TransactionId: "txn-001",
		CompletedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	paymentClient := paymentcompleted.NewClient(client)
	if err := paymentClient.Send(ctx, &payment, flowrulz.WithPartitionKey(payment.CustomerId)); err != nil {
		return fmt.Errorf("send payment.completed: %w", err)
	}
	fmt.Printf("✓ Payment completed: %s for order %s\n", payment.PaymentId, payment.OrderId)

	time.Sleep(500 * time.Millisecond)

	// 3. Simulate fraud check result (passed)
	fraud := fraudresult.Result{
		CheckId:    "check-001",
		OrderId:    "order-001",
		Passed:     true,
		RiskScore:  0.15,
		Flags:      []string{},
		ReviewedAt: time.Now().UTC().Format(time.RFC3339),
	}

	fraudClient := fraudresult.NewClient(client)
	if err := fraudClient.Send(ctx, &fraud, flowrulz.WithPartitionKey("customer-123")); err != nil {
		return fmt.Errorf("send fraud.result: %w", err)
	}
	fmt.Printf("✓ Fraud check passed for order %s\n", fraud.OrderId)

	time.Sleep(500 * time.Millisecond)

	// =====================================================================
	// SCENARIO 2: Free Order (zero amount) - Auto Confirm
	// =====================================================================
	fmt.Println("\n=== SCENARIO 2: Free Order (Auto Confirm) ===")

	freeOrder := ordercreated.Created{
		OrderId:     "order-free-001",
		CustomerId:  "customer-456",
		TotalAmount: 0,
		Currency:    "USD",
		Items: []ordercreated.Item{
			{ProductId: "promo-item", Quantity: 1, UnitPrice: 0},
		},
		ShippingAddress: ordercreated.ShippingAddress{
			Street:     "456 Oak Ave",
			City:       "Los Angeles",
			State:      "CA",
			PostalCode: "90210",
			Country:    "US",
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	if err := orderClient.Send(ctx, &freeOrder, flowrulz.WithPartitionKey(freeOrder.CustomerId)); err != nil {
		return fmt.Errorf("send free order: %w", err)
	}
	fmt.Printf("✓ Free order created: %s (auto-confirmed)\n", freeOrder.OrderId)

	time.Sleep(500 * time.Millisecond)

	// =====================================================================
	// SCENARIO 3: High Value Order - Fraud Check Required
	// =====================================================================
	fmt.Println("\n=== SCENARIO 3: High Value Order (Fraud Check) ===")

	highValueOrder := ordercreated.Created{
		OrderId:     "order-hv-001",
		CustomerId:  "customer-vip",
		TotalAmount: 100000, // $1000.00
		Currency:    "USD",
		Items: []ordercreated.Item{
			{ProductId: "prod-luxury", Quantity: 1, UnitPrice: 100000},
		},
		ShippingAddress: ordercreated.ShippingAddress{
			Street:     "1 Luxury Blvd",
			City:       "Miami",
			State:      "FL",
			PostalCode: "33101",
			Country:    "US",
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	if err := orderClient.Send(ctx, &highValueOrder, flowrulz.WithPartitionKey(highValueOrder.CustomerId)); err != nil {
		return fmt.Errorf("send high value order: %w", err)
	}
	fmt.Printf("✓ High value order created: %s ($%.2f)\n", highValueOrder.OrderId, float64(highValueOrder.TotalAmount)/100)

	time.Sleep(500 * time.Millisecond)

	// Simulate payment completion for high value
	hvPayment := paymentcompleted.Completed{
		PaymentId:     "pay-hv-001",
		OrderId:       "order-hv-001",
		CustomerId:    "customer-vip",
		Amount:        100000,
		Currency:      "USD",
		TransactionId: "txn-hv-001",
		CompletedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	if err := paymentClient.Send(ctx, &hvPayment, flowrulz.WithPartitionKey(hvPayment.CustomerId)); err != nil {
		return fmt.Errorf("send high value payment: %w", err)
	}
	fmt.Printf("✓ High value payment completed: %s\n", hvPayment.PaymentId)

	time.Sleep(500 * time.Millisecond)

	// Simulate fraud check result - FAILED (triggers review)
	hvFraud := fraudresult.Result{
		CheckId:    "check-hv-001",
		OrderId:    "order-hv-001",
		Passed:     false,
		RiskScore:  0.92,
		Flags:      []string{"high_value", "new_customer", "unusual_location"},
		ReviewedAt: time.Now().UTC().Format(time.RFC3339),
	}

	if err := fraudClient.Send(ctx, &hvFraud, flowrulz.WithPartitionKey("customer-vip")); err != nil {
		return fmt.Errorf("send high value fraud result: %w", err)
	}
	fmt.Printf("✓ Fraud check FAILED for order %s (triggers review)\n", hvFraud.OrderId)

	// =====================================================================
	// SCENARIO 4: Manual Fraud Review
	// =====================================================================
	fmt.Println("\n=== SCENARIO 4: Manual Fraud Review ===")

	review := fraudreview.Review{
		CheckId:   "check-hv-001",
		OrderId:   "order-hv-001",
		RiskScore: 0.92,
		Flags:     []string{"high_value", "new_customer", "unusual_location"},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	reviewClient := fraudreview.NewClient(client)
	if err := reviewClient.Send(ctx, &review, flowrulz.WithPartitionKey("customer-vip")); err != nil {
		return fmt.Errorf("send fraud review: %w", err)
	}
	fmt.Printf("✓ Fraud review submitted for order %s\n", review.OrderId)

	time.Sleep(500 * time.Millisecond)

	// =====================================================================
	// SCENARIO 5: New Events via SDK Direct Send
	// =====================================================================
	fmt.Println("\n=== SCENARIO 5: New Event Types (SDK Direct) ===")

	// Send order.payment_failed (demonstrates retry logic)
	paymentFailed := map[string]interface{}{
		"order_id":       "order-fail-001",
		"customer_id":    "customer-789",
		"payment_id":     "pay-fail-001",
		"amount":         50000,
		"currency":       "USD",
		"failure_reason": "insufficient_funds",
		"failed_at":      time.Now().UTC().Format(time.RFC3339),
		"retry_count":    0,
	}
	sendEvent(ctx, client, "order.payment_failed", "customer-789", paymentFailed)
	fmt.Printf("✓ Sent order.payment_failed for order-fail-001 (retry_count=0)\n")

	time.Sleep(500 * time.Millisecond)

	// Send order.refund_initiated
	refund := map[string]interface{}{
		"order_id":     "order-refund-001",
		"customer_id":  "customer-789",
		"refund_id":    "refund-001",
		"amount":       50000,
		"currency":     "USD",
		"reason":       "Payment failed after maximum retries",
		"payment_id":   "pay-fail-001",
		"initiated_at": time.Now().UTC().Format(time.RFC3339),
	}
	sendEvent(ctx, client, "order.refund_initiated", "customer-789", refund)
	fmt.Printf("✓ Sent order.refund_initiated for order-refund-001\n")

	time.Sleep(500 * time.Millisecond)

	// Send order.delivered
	delivered := map[string]interface{}{
		"order_id":          "order-001",
		"customer_id":       "customer-123",
		"shipment_id":       "ship-001",
		"tracking_number":   "TRK123456789",
		"carrier":           "fedex",
		"delivered_at":      time.Now().UTC().Format(time.RFC3339),
		"proof_of_delivery": "signature_captured",
	}
	sendEvent(ctx, client, "order.delivered", "customer-123", delivered)
	fmt.Printf("✓ Sent order.delivered for order-001\n")

	time.Sleep(500 * time.Millisecond)

	// =====================================================================
	// SCENARIO 6: Batch Send Example
	// =====================================================================
	fmt.Println("\n=== SCENARIO 6: Batch Send Multiple Orders ===")

	orders := []flowrulz.SendOptions{
		{
			RuleSet:      "order.created",
			Payload:      createOrder("batch-001", "customer-batch-1", 10000),
			PartitionKey: "customer-batch-1",
		},
		{
			RuleSet:      "order.created",
			Payload:      createOrder("batch-002", "customer-batch-2", 20000),
			PartitionKey: "customer-batch-2",
		},
		{
			RuleSet:      "order.created",
			Payload:      createOrder("batch-003", "customer-batch-3", 30000),
			PartitionKey: "customer-batch-3",
		},
	}

	if err := client.SendBatch(ctx, orders); err != nil {
		return fmt.Errorf("batch send: %w", err)
	}
	fmt.Printf("✓ Batch sent %d orders\n", len(orders))

	fmt.Println("\n=== All scenarios completed successfully! ===")
	return nil
}

// sendEvent sends an event directly via SDK with raw JSON payload
func sendEvent(ctx context.Context, client *flowrulz.Client, ruleSet, partitionKey string, payload map[string]interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	// Use the low-level transport to send
	transport := client.Transport()
	if transport == nil {
		return fmt.Errorf("no transport configured")
	}

	// Create envelope manually (simplified)
	env := &flowrulz.Envelope{
		ID:           fmt.Sprintf("evt-%s-%d", ruleSet, time.Now().UnixNano()),
		Type:         ruleSet,
		TenantID:     client.Config().TenantID,
		PartitionKey: partitionKey,
		OccurredAt:   time.Now().UTC(),
		Data:         data,
	}

	return transport.Send(ctx, env)
}

// createOrder creates a sample order payload
func createOrder(orderId, customerId string, amount int) map[string]interface{} {
	return map[string]interface{}{
		"order_id":     orderId,
		"customer_id":  customerId,
		"total_amount": amount,
		"currency":     "USD",
		"items": []map[string]interface{}{
			{"product_id": "prod-" + orderId, "quantity": 1, "unit_price": amount},
		},
		"shipping_address": map[string]interface{}{
			"street":      "123 Batch St",
			"city":        "Batch City",
			"state":       "CA",
			"postal_code": "94102",
			"country":     "US",
		},
		"created_at": time.Now().UTC().Format(time.RFC3339),
	}
}
