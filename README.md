# Microservices Engineering Lab

A hands-on repository for Java + Spring Boot microservices and distributed-system engineering.

> Learning/demo work only. No employer or client implementation is reproduced here.

## Target architecture

~~~text
Client
  |
API Gateway
  |
  +------------+-------------
  |                          |
Order Service            Merchant Service
  |                          |
Order DB                 Merchant DB
  |
OrderCreated
  |
Kafka
  +----------+-----------
  |                      |
Notification          Reporting
Consumer              Consumer
~~~

## Why microservices?

The goal is not simply to create multiple Spring Boot applications. A service should represent a meaningful business capability with clear ownership.

Each time ask:
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

The outbox pattern is the next practical step after understanding Kafka delivery and duplicate handling. It makes a database change and its event record commit together, then publishes the event asynchronously.

**Implementation walkthrough:** [Transactional Outbox — Spring Boot, Oracle and Kafka](docs/transactional-outbox-pattern.md). It covers the dual-write failure, Oracle schema, transaction boundaries, concurrent publisher leases, retries, consumer deduplication, ordering, testing and operations.

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

This is important when duplicate delivery can occur. The consumer's deduplication record and its business update should commit in the same local transaction.

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

**Implementation walkthrough:** [Distributed Tracing and Observability for Spring Boot Microservices](docs/distributed-tracing-observability.md). It follows a request across HTTP, business spans, Kafka producer/consumer observations and the asynchronous outbox publisher, with practical Micrometer/OpenTelemetry configuration and operational trade-offs.

## Security

Learn:
- Authentication vs authorization
- JWT
- OAuth2/OIDC concepts
- Service-to-service authentication
- TLS
- Least privilege
- Secret management

### Service-to-service authentication

For service-to-service calls, use an explicit workload identity and a least-privilege authorization model. A common OAuth2 approach is the client-credentials grant:

1. The calling service authenticates as itself to the authorization server.
2. It obtains a short-lived access token with only the required scope.
3. The token audience is restricted to the target API.
4. The receiving service validates signature, issuer, time claims and audience before authorizing the scope.

Do not blindly forward an incoming user's access token to every downstream service. Forward it only when delegated user context is intentionally part of the design and the token's audience/scope contract permits it. Otherwise use a service identity; for stronger workload identity, evaluate mTLS or the platform's workload-identity mechanism.

A service still needs object-level authorization and must not trust a caller-provided tenant ID. Keep client secrets in a secret manager, rotate credentials, use HTTPS, and never log bearer tokens.

For the Spring Security resource-server side, see the [OAuth2 JWT implementation guide](https://github.com/RohiniNetekal/JAVA-backend-developer/blob/main/spring-security/jwt/OAUTH2_RESOURCE_SERVER_IMPLEMENTATION.md).

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
13. Transactional outbox with a durable publisher and consumer inbox


## CI/CD for distributed services

Treat each service as an independently buildable and testable artifact. A practical pipeline should:

1. Compile and run fast unit tests for each service.
2. Run integration tests against the database and Kafka behavior that the service actually depends on, using Testcontainers where appropriate.
3. Verify outbox publisher and consumer idempotency failure scenarios.
4. Build the container image only after required tests pass.
5. Publish an immutable image tagged by commit and record its digest.
6. Promote that same digest through staging and production, with health checks and protected-environment approval.

Do not let a notification-service test require a production Kafka cluster, and do not deploy every service because an unrelated documentation file changed. The [Java backend CI/CD guide](https://github.com/RohiniNetekal/Git-Engineering-Workflow-Lab/blob/main/docs/java-backend-ci-cd.md) provides a Maven workflow template and explains Testcontainers, image promotion, OIDC and least-privilege permissions.
