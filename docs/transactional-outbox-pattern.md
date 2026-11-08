# Transactional Outbox Pattern — Spring Boot, Oracle and Kafka

> Independent learning/demo guide. The examples illustrate an architecture; they are not copied from an employer system and are not a claim of production deployment experience.

## 1. The problem: a database write and a Kafka publish are two writes

Imagine an order API that must save an order in Oracle and publish an `OrderCreated` event.

A naive implementation does this:

```java
@Transactional
public Order createOrder(CreateOrderRequest request) {
    Order order = orderRepository.save(toOrder(request));
    kafkaTemplate.send("orders", order.getId().toString(),
            toOrderCreatedEvent(order));
    return order;
}
```

The database transaction and Kafka publish do not share one ordinary local transaction. Two failure windows appear:

| Sequence | Failure | Result |
|---|---|---|
| 1. Commit Oracle, then publish Kafka | Kafka is unavailable after the database commit | The order exists, but downstream services never hear about it |
| 2. Publish Kafka, then commit Oracle | The Oracle transaction rolls back | Consumers see an event for an order that does not exist |

This is the **dual-write problem**. A normal `@Transactional` boundary controls the database transaction; it does not automatically make a Kafka send atomic with that database commit.

## 2. The core idea

Write the business change and an event record to the **same database transaction**. A separate publisher reads committed outbox rows and sends them to Kafka.

```text
HTTP request
    |
    v
@Transactional service
    |
    +---- INSERT / UPDATE business row
    |
    +---- INSERT outbox_event row
    |
    v
Oracle COMMIT  <---- both rows become visible together
    |
    v
Outbox publisher
    |
    +---- claim a small batch
    |
    +---- publish event to Kafka
    |
    +---- wait for broker acknowledgement
    |
    +---- mark row PUBLISHED
    |
    v
Kafka consumer -> idempotent business processing
```

The important guarantee is narrow and useful:

- If the business transaction rolls back, the outbox row rolls back too.
- If it commits, the outbox row is durably available for later publication.
- Kafka publication can be retried independently of the original HTTP request.

The outbox does **not** make Oracle and Kafka one distributed transaction. It changes the failure mode from a potentially lost event to a durable event that can be retried.

## 3. Oracle table design

A compact starting schema:

```sql
CREATE TABLE OUTBOX_EVENT (
    EVENT_ID          VARCHAR2(36) NOT NULL,
    AGGREGATE_TYPE    VARCHAR2(100) NOT NULL,
    AGGREGATE_ID      VARCHAR2(100) NOT NULL,
    AGGREGATE_VERSION NUMBER(19),
    EVENT_TYPE        VARCHAR2(150) NOT NULL,
    EVENT_VERSION     NUMBER(5) DEFAULT 1 NOT NULL,
    PAYLOAD           CLOB NOT NULL,
    STATUS            VARCHAR2(20) DEFAULT 'PENDING' NOT NULL,
    ATTEMPT_COUNT     NUMBER(10) DEFAULT 0 NOT NULL,
    NEXT_ATTEMPT_AT   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    LOCKED_UNTIL      TIMESTAMP WITH TIME ZONE,
    LOCK_TOKEN        VARCHAR2(36),
    CREATED_AT        TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    PUBLISHED_AT      TIMESTAMP WITH TIME ZONE,
    LAST_ERROR        VARCHAR2(1000),
    CONSTRAINT PK_OUTBOX_EVENT PRIMARY KEY (EVENT_ID),
    CONSTRAINT CK_OUTBOX_STATUS
        CHECK (STATUS IN ('PENDING', 'IN_PROGRESS', 'PUBLISHED', 'DEAD'))
);

CREATE INDEX IX_OUTBOX_READY
    ON OUTBOX_EVENT (STATUS, NEXT_ATTEMPT_AT, CREATED_AT);
```

Column responsibilities:

- `EVENT_ID`: stable unique identity used by consumers for deduplication.
- `AGGREGATE_TYPE` / `AGGREGATE_ID`: the business object that changed, such as `ORDER / 501`.
- `AGGREGATE_VERSION`: optional sequence/version for detecting stale or out-of-order events.
- `EVENT_TYPE` / `EVENT_VERSION`: explicit event contract and schema version.
- `PAYLOAD`: immutable event data; do not serialize a live Hibernate entity.
- `STATUS`, `ATTEMPT_COUNT`, `NEXT_ATTEMPT_AT`: retry lifecycle.
- `LOCKED_UNTIL` / `LOCK_TOKEN`: lease information for competing publisher workers.
- `PUBLISHED_AT` / `LAST_ERROR`: operations and troubleshooting.

The index is a starting point, not a universal tuning answer. Validate it against the actual claim query and Oracle execution plan. Avoid indexing every status or timestamp column without measuring the write overhead.

## 4. Save business state and outbox event in one transaction

The order and outbox repositories must participate in the same Oracle transaction manager and datasource.

```java
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxEventRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        Order order = orderRepository.save(Order.from(request));

        OrderCreated payload = new OrderCreated(
                order.getId(),
                order.getCustomerId(),
                order.getTotalAmount()
        );

        outboxRepository.save(OutboxEvent.pending(
                UUID.randomUUID().toString(),
                "ORDER",
                order.getId().toString(),
                "OrderCreated",
                1,
                toJson(payload)
        ));

        return OrderResponse.from(order);
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Could not serialize outbox event", ex);
        }
    }
}
```

This is an illustrative Spring/JPA service. The same transaction principle applies when using Hibernate `SessionFactory` or JDBC: insert the business row and outbox row using the same connection/transaction.

**Do not publish to Kafka inside this method.** A Kafka call can block, time out, or succeed while the database later rolls back. Keep the request transaction focused on durable database work.

Also avoid treating `@TransactionalEventListener(phase = AFTER_COMMIT)` as a durable outbox. It runs after commit in the application process; a process crash between commit and listener execution can still lose the notification.

## 5. Publisher lifecycle and safe concurrent claiming

A publisher should use short database transactions:

1. Claim a bounded batch of eligible rows.
2. Set `STATUS = 'IN_PROGRESS'`, a lease expiry, a fresh lock token and increment the attempt count.
3. Commit the claim transaction.
4. Publish outside the database transaction and wait for Kafka acknowledgement.
5. In a new short transaction, mark the row `PUBLISHED` or schedule a retry.

For multiple workers, Oracle's `FOR UPDATE SKIP LOCKED` lets a worker skip rows currently locked by another worker:

```sql
SELECT EVENT_ID
FROM OUTBOX_EVENT
WHERE (STATUS = 'PENDING' AND NEXT_ATTEMPT_AT <= SYSTIMESTAMP)
   OR (STATUS = 'IN_PROGRESS' AND LOCKED_UNTIL < SYSTIMESTAMP)
FOR UPDATE SKIP LOCKED;
```

Use a JDBC cursor or a database-side claim routine to stop after a configured batch size; do not lock the whole backlog. In the same claim transaction, update the selected rows with a lease and a unique `LOCK_TOKEN`, then commit before calling Kafka.

A simplified publisher outline:

```java
public void publish(ClaimedOutboxEvent event) {
    try {
        kafkaTemplate.send(
                "orders",
                event.aggregateId(),       // Kafka key
                event.payload()
        ).get(10, TimeUnit.SECONDS);        // wait for broker acknowledgement

        outboxService.markPublished(event.eventId(), event.lockToken());
    } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        outboxService.scheduleRetry(
                event.eventId(),
                event.lockToken(),
                nextAttemptTime(event.attemptCount()),
                "Publisher thread interrupted"
        );
    } catch (Exception ex) {
        outboxService.scheduleRetry(
                event.eventId(),
                event.lockToken(),
                nextAttemptTime(event.attemptCount()),
                safeErrorMessage(ex)
        );
    }
}
```

A baseline producer configuration is:

```properties
spring.kafka.producer.acks=all
spring.kafka.producer.properties.enable.idempotence=true
spring.kafka.producer.properties.max.in.flight.requests.per.connection=5
spring.kafka.producer.properties.delivery.timeout.ms=120000
spring.kafka.producer.properties.request.timeout.ms=30000
```

Producer idempotence helps with retries handled by that Kafka producer. It does **not** remove the crash window between Kafka acknowledgement and the Oracle `PUBLISHED` update; the outbox and idempotent consumer are still required.

The `markPublished` and `scheduleRetry` updates should include the event ID, `STATUS = 'IN_PROGRESS'` and the same lock token in their `WHERE` clause. If the update affects zero rows, the worker no longer owns that lease and must not overwrite the newer worker's state.

A lease must be longer than the normal publish timeout, or be renewed for long sends. An expired lease makes a crashed worker's event eligible for another worker. It does not prove that the first worker failed to publish.

## 6. The unavoidable duplicate window

Consider this sequence:

1. Publisher sends event `E-123`.
2. Kafka acknowledges the message.
3. The publisher process crashes before updating Oracle to `PUBLISHED`.
4. The lease expires and another worker sends `E-123` again.

Therefore the usual delivery contract is **at-least-once**, not exactly-once end-to-end. Kafka producer idempotence can help with producer retries, but it does not atomically combine the Oracle commit with the Kafka publish.

Consumers must be idempotent. A common inbox table uses a unique key such as:

```sql
CREATE TABLE PROCESSED_EVENT (
    CONSUMER_NAME VARCHAR2(100) NOT NULL,
    EVENT_ID      VARCHAR2(36) NOT NULL,
    PROCESSED_AT  TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT PK_PROCESSED_EVENT
        PRIMARY KEY (CONSUMER_NAME, EVENT_ID)
);
```

In one consumer-side database transaction:

- attempt to record `(consumer_name, event_id)` using a database-safe insert-if-absent operation backed by the unique constraint;
- if it was already recorded, do not repeat the business effect;
- if it is new, apply the business update in that same transaction;
- acknowledge the Kafka message only after the transaction commits.

For Oracle, implement the insert-if-absent operation with an appropriate SQL strategy and test concurrent duplicate delivery. Do not simply catch a JPA constraint exception and continue in the same transaction: the persistence transaction may already be marked rollback-only. Handle the duplicate at a transaction boundary or use a conflict-safe repository operation.

## 7. Ordering, retries and poison events

- **Kafka key:** use `AGGREGATE_ID` as the Kafka key when events for one order/account must go to the same partition.
- **Publisher ordering:** a Kafka key preserves order in the order messages are sent to that partition. Multiple outbox workers can still send version 12 before version 11. If strict per-aggregate order matters, store an aggregate version and claim only the next unpublished version for each aggregate, or partition dispatch by aggregate.
- **Backoff:** use bounded exponential backoff with jitter. Immediate retries can turn a Kafka outage into a retry storm.
- **Dead events:** after a configured number of attempts, move the row to `DEAD`, alert, and provide an audited replay procedure. Do not silently discard it.
- **Payloads:** version event schemas and keep payloads immutable. Do not put passwords, access tokens or unnecessary personal data in events.
- **Retention:** retain published rows long enough for audit and replay requirements, then archive or purge them with a controlled cleanup job.

## 8. Polling publisher vs change-data capture

| Approach | Advantages | Costs / trade-offs |
|---|---|---|
| Polling outbox table | Straightforward to understand; application controls retries and state | Repeated queries, index/cleanup work, polling latency |
| CDC (for example, Debezium reading the database log) | Avoids frequent polling; can stream committed outbox inserts | More infrastructure and operational knowledge; connector offsets and schema evolution need care |

Start with polling for a small learning implementation. Consider CDC when throughput, latency or database polling load justifies the additional operational complexity.

## 9. Tests that prove the important guarantees

Use integration tests with the same database engine and Kafka behavior as the intended deployment whenever possible. Test at least:

- [ ] When order creation rolls back, no outbox row remains.
- [ ] When order creation commits, exactly one outbox row is committed with it.
- [ ] A Kafka failure leaves the event retryable; the original order is not recreated.
- [ ] A crash after Kafka acknowledgement but before the published update causes a duplicate, and the consumer inbox prevents a second business effect.
- [ ] Two publisher workers do not claim the same active lease.
- [ ] An expired lease can be reclaimed after a worker crash.
- [ ] A stale lock token cannot mark a row published after another worker reclaimed it.
- [ ] Repeated failures reach `DEAD` and trigger an alert/replay path.
- [ ] Event ordering behavior is tested if the business requires per-aggregate order.

Useful operational signals include pending row count, age of the oldest pending row, publish latency, retry count, dead-row count, lease expirations and consumer duplicate count.

## 10. Common mistakes

1. Calling Kafka from the original business transaction and assuming `@Transactional` covers both systems.
2. Marking an event published before Kafka acknowledges it (can lose events).
3. Assuming a successful Kafka send followed by a database update is duplicate-free (the crash window remains).
4. Holding Oracle row locks while waiting on network I/O.
5. Running multiple workers without `SKIP LOCKED`, leases or ownership tokens.
6. Retrying forever without backoff, alerting or a dead-event workflow.
7. Claiming strict ordering merely because the Kafka key is the aggregate ID.
8. Storing a serialized JPA entity instead of a deliberate, versioned event contract.

## Interview questions

1. What is the dual-write problem, and why does a normal Spring `@Transactional` method not solve it?
2. Why must the business row and outbox row use the same database transaction?
3. Why is outbox delivery generally at-least-once?
4. What failure occurs after Kafka acknowledges an event but before Oracle marks it published?
5. How does an inbox/processed-event table make a consumer idempotent?
6. What does Oracle `FOR UPDATE SKIP LOCKED` solve when several publisher workers run?
7. Why are a lease and lock token useful? What if a lease expires while a send is still in progress?
8. Does using the aggregate ID as a Kafka key guarantee that concurrent outbox publishers preserve business-event order?
9. When would you choose CDC/Debezium instead of polling?
10. How would you monitor and safely replay a dead outbox event?

## Practical progression

- [ ] Draw the two dual-write failure timelines without notes.
- [ ] Implement the Oracle outbox table and same-transaction insert.
- [ ] Add a polling publisher with leases, bounded batches and backoff.
- [ ] Add a consumer inbox with a unique event identity.
- [ ] Write the crash-window and concurrent-worker integration tests.
- [ ] Measure backlog age and publish latency before tuning batch size or indexes.


## Further reading: tracing the asynchronous outbox

The publisher may run after the original HTTP request has completed and on a different thread or process. The request's thread-local trace context is therefore not automatically available to the publisher. If end-to-end causal diagnosis is required, persist the required W3C trace context (typically traceparent and, when used, tracestate) with the outbox row, then extract it in the publisher before creating the Kafka send observation. Do not persist arbitrary baggage or sensitive values.

See the [Distributed Tracing and Observability guide](distributed-tracing-observability.md) for Oracle column examples, Micrometer Propagator injection/extraction, Spring Kafka producer/consumer observation settings, sampling, and verification exercises. This improves diagnosis but does not change the outbox's at-least-once delivery guarantee or the need for an idempotent consumer.
