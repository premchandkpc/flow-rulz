undefined
- Durable worker retries now have an application-side exhaustion threshold (default 24, `EVENT_MAX_DELIVERY_ATTEMPTS`). An exhausted event is quarantined before ACK. Broker MaxDeliver remains unlimited so broker-side redelivery limits cannot silently strand a message. Missing broker attempt metadata remains retryable rather than guessing.
