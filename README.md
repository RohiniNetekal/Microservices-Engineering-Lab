# Microservices Engineering Lab

A hands-on repository for Java + Spring Boot microservices and distributed-system engineering.

> Learning/demo work only. No employer or client implementation is reproduced here.

## Target architecture

~~~text
Client
  |
API Gateway
  |
  +------------+-------------+
  |                          |
Order Service            Merchant Service
  |                          |
Order DB                 Merchant DB
  |
OrderCreated
  |
Kafka
  +----------+-----------+
  |                      |
Notification          Reporting
Consumer              Consumer
~~~

## Why microservices?

The goal is not simply to create multiple Spring Boot applications. A service should represent a meaningful business capability with clear ownership.

Each time ask --->>
- What business capability does this service own?
- Which data does it own?
- Which operations must be atomic?
- What API does it expose?
- What events should it publish?
- What happens when a dependency is unavailable?

## Synchronous communication

HTTP between services introduces network failures.

Study:
- Connection timeout
- Read timeout
- Retry policy
- Backoff
- Error mapping
- Correlation IDs
- Metrics

A service should not wait forever for another service.

## Database ownership

Preferred learning model:

~~~text
Order Service -> Order DB
Merchant Service -> Merchant DB
~~~

Directly reading another service's database creates tight coupling and bypasses the owning service's rules.

## Distributed transactions

A transaction inside one database is simpler than a business operation spanning services.

Important concepts:
- Eventual consistency
- Saga pattern
- Compensating actions
- Outbox pattern
- Idempotent consumers

## Kafka

Core model:

~~~text
Producer -> Topic -> Partition -> Consumer Group
                         |
                       Offset
~~~

Understand:
- Topics
- Partitions
- Keys
- Ordering
- Consumer groups
- Offsets
- Retention
- Rebalancing
- At-least-once delivery
- Duplicate processing

## Idempotent consumer

~~~text
Message
  |
eventId already processed?
  | yes              | no
  v                  v
ignore          process + record eventId
~~~

This is important when duplicate delivery can occur.

## Resilience patterns

### Timeout
Limits how long a request waits.

### Retry
Useful for transient failures when the operation is safe to repeat.

### Exponential backoff
Prevents immediate repeated pressure on a failing dependency.

### Circuit breaker
Temporarily stops calls to a failing dependency.

### Bulkhead
Prevents one workload from consuming all application resources.

### Rate limiting
Controls traffic and protects service capacity.

## Observability

Distributed systems require visibility across services.

- Structured logs
- Metrics
- Distributed traces
- OpenTelemetry
- Health/readiness endpoints
- Correlation/trace IDs
- Alerting concepts

## Security

Learn:
- Authentication vs authorization
- JWT
- OAuth2/OIDC concepts
- Service-to-service authentication
- TLS
- Least privilege
- Secret management

## Containerization

The eventual local environment:

~~~text
Docker Compose
  +-- order-service
  +-- merchant-service
  +-- kafka
  +-- database
~~~

## Build roadmap

1. Two Spring Boot services
2. REST communication
3. Separate databases
4. Kafka event flow
5. Idempotent consumer
6. Timeouts/retry/backoff
7. Circuit breaker
8. Docker Compose
9. Integration tests
10. OpenTelemetry
11. CI/CD
12. Security

