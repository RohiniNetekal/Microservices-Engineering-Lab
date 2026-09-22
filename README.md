# Microservices Engineering Lab

A dedicated hands-on space for learning **Java/Spring Boot microservices** and the engineering problems that appear when a monolith is split into independently deployable services.

> Learning/demo work only. No employer or client implementation is reproduced here.

## Target architecture

```text
                    +----------------+
Client ------------>| API Gateway    |
                    +-------+--------+
                            |
              +-------------+-------------+
              |                           |
              v                           v
      +---------------+           +---------------+
      | Order Service |           | Merchant Svc  |
      +-------+-------+           +-------+-------+
              |                           |
              v                           v
          Order DB                   Merchant DB

                    +----------------+
                    |     Kafka      |
                    +-------+--------+
                            |
                    +-------+--------+
                    | Event Consumer |
                    +----------------+
```

## Concepts to build

### Service fundamentals
- Service boundaries
- Database-per-service
- REST communication
- DTOs and contracts
- Configuration management
- Health checks

### Reliability
- Connection/read timeouts
- Retry with exponential backoff
- Idempotency
- Circuit breaker concept
- Bulkhead concept
- Rate limiting
- Graceful degradation

### Messaging
- Kafka producers and consumers
- Topics and partitions
- Consumer groups
- Offsets
- At-least-once delivery
- Duplicate message handling
- Dead-letter handling

### Production engineering
- Correlation IDs
- Centralized logging
- Metrics
- Distributed tracing
- OpenTelemetry
- API documentation
- Containerization
- CI/CD
- Security between services

## Important interview distinction

Microservices are not simply "many Spring Boot applications".

The difficult part is distributed-system behavior: network failures, data consistency, retries, observability, deployment, security and independent ownership.

## Planned implementation

1. Order Service
2. Merchant Service
3. API Gateway
4. Kafka event flow
5. Idempotent consumer
6. Resilience patterns
7. Docker Compose environment
8. Integration tests
9. Observability

