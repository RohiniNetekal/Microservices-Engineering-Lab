# Distributed Tracing and Observability for Spring Boot Microservices

> Independent learning/demo material. This guide explains a practical implementation path; it is not a claim that this stack has been deployed in the user's employer environment.

## 1. Why observability matters

A service returning HTTP 200 does not prove that the whole business flow is healthy. A request can be slow because of JVM pauses, a saturated connection pool, an Oracle query, a downstream HTTP call, Kafka publication, or consumer lag.

Observability combines three complementary signals:

| Signal | Best question | Example |
|---|---|---|
| Logs | What happened? | A merchant lookup failed because Oracle timed out |
| Metrics | How often / how much? | p95 latency rose from 120 ms to 2.4 s |
| Traces | Where did this request spend time? | 2.1 s was spent in the merchant database call |

A useful investigation uses all three. Metrics identify the time window and affected operation; a trace decomposes one slow request; logs provide the detailed error and business context.

## 2. Trace, span and context

A **trace** represents one end-to-end operation. A **span** represents one timed unit of work inside that trace. Spans have parent-child relationships, timestamps, attributes, events and status.

~~~text
Client: POST /orders
  |
  +-- order-api: HTTP server span
        |
        +-- order-service: create-order span
        |     |
        |     +-- Oracle transaction / SQL spans*
        |
        +-- Kafka producer span
              |
              +-- notification-service: Kafka listener span
                    |
                    +-- notification database span*

*Database spans require compatible JDBC/database instrumentation; they should not be assumed to appear merely because Actuator is present.
~~~

A trace ID identifies the overall trace. A span ID identifies one operation. The parent span ID establishes the causal relationship.

Context propagation moves trace context between execution and process boundaries. For HTTP, W3C Trace Context commonly uses the traceparent header (and optionally tracestate). A downstream service extracts that context and creates a child span instead of starting an unrelated trace.

Do not generate a new random correlation ID at every service boundary. Let supported instrumentation inject and extract the configured trace context.

## 3. Spring Boot setup

Spring Boot uses Micrometer Observation as a common instrumentation facade. With a tracing implementation configured, an observation can produce both a metric and a span. This keeps business instrumentation independent of a particular tracing vendor.

### Dependency guidance

Use the Spring Boot dependency-management BOM and the documentation for the exact Boot line in the application:

- Spring Boot 3.5.x: include spring-boot-starter-actuator, micrometer-tracing-bridge-otel and the OpenTelemetry exporter that matches the selected backend (for example, opentelemetry-exporter-otlp).
- Spring Boot 4.1.x: the current documented OTLP path uses spring-boot-starter-opentelemetry.

Do not copy dependency coordinates or property prefixes from a different Spring Boot major/minor line. Let the Boot BOM manage compatible Micrometer and OpenTelemetry versions; avoid independently pinning transitive tracing libraries unless there is a tested reason.

### Example configuration

The following is a local-learning profile for a Spring Boot 4.1.x OTLP exporter. The collector must be running at the configured endpoint.

~~~yaml
spring:
  application:
    name: order-service

management:
  tracing:
    sampling:
      probability: 1.0  # local learning only; not a production default
  opentelemetry:
    tracing:
      export:
        otlp:
          endpoint: \${OTEL_EXPORTER_OTLP_TRACES_ENDPOINT:http://localhost:4318/v1/traces}
~~~

For Boot 3.5.x, use its versioned tracing reference and the management.otlp.tracing.* property namespace. The property namespace changed in Boot 4.0; do not mix the two examples.

The default sampling probability is 10% in the documented Spring Boot tracing setup. Sampling every request is useful for a small local exercise, but can create unnecessary cost and volume in production. Choose a deliberate sampling strategy that preserves parent-based decisions and meets the team's debugging and retention requirements.

## 4. Correlate logs with traces

With Micrometer Tracing configured, Spring Boot adds traceId and spanId to the logging MDC, allowing log lines to be searched alongside a trace. Keep structured logs and include useful, non-sensitive fields such as operation name, result and dependency.

Example correlation pattern (optional):

~~~yaml
logging:
  pattern:
    correlation: "[\${spring.application.name:},%X{traceId:-},%X{spanId:-}] "
  include-application-name: false
~~~

Never log access tokens, passwords, card data, full authorization headers, private keys or unnecessary personal data. A trace ID is a diagnostic identifier, not an authorization credential.

## 5. Instrument business operations with Observation

HTTP server requests are instrumented by Spring Boot. For important business operations, create a small custom observation rather than adding ad-hoc start/end timestamps throughout the code.

~~~java
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.stereotype.Service;

@Service
public class MerchantLookupService {

    private final ObservationRegistry observationRegistry;
    private final MerchantRepository repository;

    public MerchantLookupService(
            ObservationRegistry observationRegistry,
            MerchantRepository repository) {
        this.observationRegistry = observationRegistry;
        this.repository = repository;
    }

    public Merchant findMerchant(Long merchantId) {
        return Observation.createNotStarted(
                        "merchant.lookup",
                        observationRegistry)
                .lowCardinalityKeyValue("operation", "merchant.lookup")
                .observe(() -> repository.findById(merchantId)
                        .orElseThrow(() ->
                                new MerchantNotFoundException(merchantId)));
    }
}
~~~

Keep metric dimensions low-cardinality: operation, outcome, region or dependency name are usually bounded. Do not add merchantId, orderId, email, raw URL, exception message or request ID as metric tags; they can create millions of time series. If a per-request identifier is needed for diagnosis, use a trace/span attribute only when safe and useful, not a metric label.

For latency, inspect distributions such as p50, p95 and p99, not only averages. Averages can hide a small but important set of very slow requests.

## 6. Propagate context across HTTP calls

Use Spring Boot's auto-configured RestClient.Builder, WebClient.Builder or RestTemplateBuilder to create outbound HTTP clients. Those builders participate in Micrometer context propagation.

~~~java
@Configuration
class DownstreamClientConfiguration {

    @Bean
    RestClient merchantRestClient(RestClient.Builder builder) {
        return builder
                .baseUrl("http://merchant-service")
                .build();
    }
}
~~~

Avoid constructing a raw client with new RestTemplate() or an independently created client when you expect Boot's tracing customizations to be applied. If a custom HTTP client or proxy is involved, verify that traceparent and tracestate are preserved across that boundary.

## 7. Propagate context through Kafka

Kafka is an asynchronous boundary. Enable observations on both the producer template and consumer listener:

~~~yaml
spring:
  kafka:
    template:
      observation-enabled: true
    listener:
      observation-enabled: true
~~~

With compatible Spring Kafka and Micrometer tracing instrumentation, the producer observation injects trace context into message headers and the listener observation extracts it. Verify this with the actual Spring Boot/Spring Kafka versions and the trace backend; do not assume custom KafkaTemplate or listener-container beans inherit every auto-configuration detail.

A trace can then show the producer operation and the consumer processing operation in the same causal flow. Consumer processing may occur much later, so monitor message age and consumer lag separately from the original HTTP latency.

## 8. Special case: the Transactional Outbox is asynchronous

The outbox publisher usually runs after the HTTP request transaction has committed. It may execute on another thread or process minutes later. The request's thread-local tracing context is not automatically available to that scheduled worker.

If the team needs the original request-to-event causal relationship, persist only the required W3C trace context with the outbox row. For an Oracle learning schema, optional columns can be:

~~~sql
ALTER TABLE OUTBOX_EVENT ADD (
    TRACEPARENT VARCHAR2(55 CHAR),
    TRACESTATE  VARCHAR2(512 CHAR)
);
~~~

Capture the active context when the business row and outbox row are written:

~~~java
Map<String, String> carrier = new HashMap<>();
Span current = tracer.currentSpan();

if (current != null) {
    propagator.inject(current.context(), carrier, Map::put);
}

outboxRepository.save(OutboxEvent.pending(
        eventId,
        aggregateId,
        payload,
        carrier.get("traceparent"),
        carrier.get("tracestate")
));
~~~

The publisher can extract the stored context and make a named span current while it sends the event. The KafkaTemplate observation then creates the producer operation beneath that context:

~~~java
Map<String, String> carrier = new HashMap<>();
if (event.traceparent() != null) {
    carrier.put("traceparent", event.traceparent());
}
if (event.tracestate() != null) {
    carrier.put("tracestate", event.tracestate());
}

Span publishSpan = propagator.extract(carrier, Map::get)
        .name("outbox.publish")
        .start();

try (Tracer.SpanInScope scope = tracer.withSpan(publishSpan)) {
    kafkaTemplate.send(topic, event.aggregateId(), event.payload())
            .get(10, TimeUnit.SECONDS);
} finally {
    publishSpan.end();
}
~~~

This is an illustrative Micrometer Tracing pattern; adapt constructor types, timeout handling and transaction boundaries to the selected versions. The outbox publisher must still claim rows in a short database transaction, publish outside row locks, and update the lease/status safely.

Do not persist arbitrary baggage by default. Baggage is propagated to downstream systems and may be logged. Store only the trace fields needed for the diagnostic requirement, protect the outbox table, and define retention. If long delays make a parent-child trace misleading or the parent trace may have expired from the backend, consider a span link or a separate workflow correlation strategy instead of forcing a very long trace.

This does not change the outbox delivery guarantee: publication is still normally at-least-once, and consumers still need idempotency. See the [Transactional Outbox guide](transactional-outbox-pattern.md).

## 9. Database and JVM visibility

Do not assume all database operations become spans automatically. HTTP instrumentation and Kafka observations are distinct from JDBC instrumentation. For Oracle/JDBC spans, evaluate a compatible datasource instrumentation library or OpenTelemetry Java agent, then verify that SQL text, bind values and connection details are not exposing sensitive information.

Useful signals for a Java backend include:

- JVM heap and non-heap usage
- Garbage collection count and pause duration
- Live threads and thread-pool saturation
- HikariCP active, idle and pending connections
- HTTP request count, error rate and latency distribution
- Kafka producer send failures and consumer lag
- Outbox pending count, oldest pending age, retries and dead events
- Database query latency and connection acquisition time

A slow endpoint with low CPU can still be blocked on a database connection, remote service or lock. Use traces and pool metrics together before changing thread-pool sizes.

## 10. A practical incident workflow

When an API becomes slow:

1. Use the latency/error dashboard to identify the affected endpoint and time window.
2. Compare request rate, error rate and p95/p99 latency.
3. Open representative slow traces and locate the longest span.
4. Check whether time is spent in application code, Oracle, an HTTP dependency, Kafka or a queue wait.
5. Search logs using traceId/spanId and inspect the exception and safe business context.
6. Compare JVM, connection-pool, Kafka lag and database metrics for the same period.
7. Make one targeted change and compare the same measurements after deployment.

Do not jump straight to adding more threads or indexes. First identify the saturated resource or slow operation.

## 11. Common mistakes and trade-offs

- **100% sampling in production:** can create excessive telemetry volume and cost. Use a deliberate sampling policy.
- **High-cardinality metric tags:** can overwhelm a metrics backend. Keep metric dimensions bounded.
- **Sensitive span attributes or baggage:** trace data is operational data and must be protected.
- **Raw HTTP clients:** may bypass auto-configured propagation.
- **Kafka observation enabled on only one side:** producer and consumer may appear as separate traces.
- **Thread-local assumptions:** context does not automatically cross every executor, callback or custom async boundary. Use supported context-propagation mechanisms and test them.
- **Assuming Oracle spans exist:** add and verify JDBC/database instrumentation separately.
- **Treating correlation IDs as security controls:** trace identifiers are not authentication or authorization.
- **Ignoring telemetry failure:** exporting telemetry should not take down the business request; use bounded timeouts, queues and exporter configuration.

## 12. Verification exercises

- [ ] Start an OTLP-compatible local collector/backend and configure one Spring Boot service.
- [ ] Call an instrumented REST endpoint and find its server span.
- [ ] Add a custom Observation around one business operation.
- [ ] Call a second service using an auto-configured HTTP client and verify the shared trace ID.
- [ ] Enable Kafka template and listener observations; verify producer-to-consumer propagation.
- [ ] Persist traceparent/tracestate with an outbox event and verify the publisher trace relationship.
- [ ] Generate a failed request and confirm logs can be found by trace ID.
- [ ] Compare sampled traces with request metrics; confirm unsampled requests still contribute metrics.
- [ ] Simulate an unavailable telemetry collector and confirm the business API remains available.
- [ ] Check that no secrets, tokens or personal data appear in logs, span attributes or baggage.

## Interview questions

1. Logs vs metrics vs distributed traces: when would you use each?
2. What is the difference between a trace ID and a span ID?
3. How does W3C trace context propagate across HTTP?
4. What does Micrometer Observation provide on top of metrics and tracing?
5. Why should metric tags be low-cardinality?
6. Why can a raw RestTemplate fail to propagate trace context?
7. What must be enabled to observe Spring Kafka producer and consumer operations?
8. Why is an outbox publisher a special tracing boundary?
9. How can trace context be carried through a delayed outbox event without keeping the HTTP request open?
10. Why might a slow API have low CPU utilization?
11. Why should JDBC spans be verified separately from HTTP spans?
12. How would you choose a sampling strategy for a high-volume service?

## Official references

- [Spring Boot 3.5 tracing reference](https://docs.spring.io/spring-boot/3.5/reference/actuator/tracing.html)
- [Spring Boot current tracing reference](https://docs.spring.io/spring-boot/reference/actuator/tracing.html)
- [Spring Kafka Micrometer observation reference](https://docs.spring.io/spring-kafka/reference/appendix/micrometer.html)
- [OpenTelemetry context propagation concepts](https://opentelemetry.io/docs/concepts/context-propagation/)
- [Micrometer Tracing API reference](https://docs.micrometer.io/tracing/reference/api.html)
