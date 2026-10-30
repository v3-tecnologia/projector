# Projector

A small, extensible Go library for building database-backed read models from events.

Projector is designed to work with applications that already have their own message ingestion and consumption mechanisms. It does not implement a message broker client, create consumers, or impose delivery semantics.

The application is responsible for consuming messages, converting them into `projection.Event`, and calling `Projector.Project`. The application retains full control over acknowledgments, retries, dead-letter queues, and error handling.

## Features

- Extensible handler registry, with one handler per read model.
- MariaDB/InnoDB persistence.
- Automatic creation of the library's internal idempotency table.
- Automatic creation of projection read-model tables through `EnsureSchema`.
- Atomic idempotency record and read-model update in one transaction.
- Safe redelivery after a successful database commit.
- Context propagation into SQL operations.
- No dependency on a particular message broker, consumer framework, serialization format, or event transport.
- No dependency on NATS or protobuf in the library core.

## Requirements

- Go 1.22+
- MariaDB with InnoDB tables
- A configured `*sql.DB` using `github.com/go-sql-driver/mysql`

## Quick start

### 1. Configure the database and projector

```go
package main

import (
    "context"
    "database/sql"
    "log"

    _ "github.com/go-sql-driver/mysql"

    "github.com/your-org/projector/mariadb"
    "github.com/your-org/projector/projection"
    "github.com/your-org/projector/examples/stock"
)

func main() {
    ctx := context.Background()

    db, err := sql.Open("mysql", "user:password@tcp(localhost:3306)/app")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    if err := db.PingContext(ctx); err != nil {
        log.Fatal(err)
    }

    registry := projection.NewRegistry()
    if err := registry.Register(stock.Projection{}); err != nil {
        log.Fatal(err)
    }

    store := mariadb.New(db)

    projector, err := projection.NewProjector(store, registry)
    if err != nil {
        log.Fatal(err)
    }

    // Initialize the internal table and each registered read model.
    // Complete initialization before starting message consumption.
    if err := projector.Initialize(ctx); err != nil {
        log.Fatal(err)
    }

    // Start your application's own ingestion and consumption mechanism here.
    // Pass the initialized projector to the component that processes messages.
}
```

Replace `github.com/your-org/projector` with the module path chosen for your project. The database credentials above are illustrative; use your application's secret-management mechanism in production.

### 2. Convert and process a message

The integration with the source system belongs to the application, not the library.

```go
func handleMessage(
    ctx context.Context,
    rawMessage []byte,
    projector *projection.Projector,
) error {
    event, err := convertMessage(rawMessage)
    if err != nil {
        return fmt.Errorf("convert source message: %w", err)
    }

    if err := projector.Project(ctx, event); err != nil {
        return fmt.Errorf("project event: %w", err)
    }

    return nil
}
```

`convertMessage` is application-specific. It may decode JSON, protobuf, Avro, a broker-specific envelope, or any other source format.

The transport adapter decides what to do with the returned error:

- On success, acknowledge or complete the message according to the source system's API.
- On a transient failure, retry or negatively acknowledge the message if supported.
- On a permanent failure, apply the application's dead-letter or rejection policy.

These operations are intentionally absent from Projector because their semantics differ between ingestion systems.

## Event contract

The library receives events through a transport-independent structure:

```go
type Event struct {
    ID        string
    Category  string
    EventTime time.Time
    Payload   []byte
}
```

- **`ID`** must be stable across redeliveries and unique within the relevant event source. It is used for idempotent processing.
- **`Category`** identifies the event type and is used to route it to matching handlers.
- **`EventTime`** represents the source event's timestamp. Preserve the original timestamp instead of assigning `time.Now()` when consuming the message.
- **`Payload`** contains the serialized data expected by the projection handler. The library does not assume JSON, protobuf, or any other format.

The initial implementation intentionally has no `Offset` or `Tenant` field. Add these only when there is a concrete source or domain requirement.

If multiple independent event sources can generate the same event ID, the application must ensure that the IDs are globally unique for the library's idempotency scope, or extend the event identity contract accordingly.

## Implementing a projection

Implement `projection.Handler` for each read model.

A handler is responsible for:

- `Name()` — returning a stable, unique projection name.
- `Handles(category)` — declaring which event categories it processes.
- `EnsureSchema(ctx, db)` — creating the required read-model tables.
- `Apply(ctx, tx, event)` — applying an event to the read model using the supplied transaction.

See `examples/stock/projection.go` for a concrete example.

### Transaction requirements

All tables modified by `Apply` must use InnoDB and participate in the same MariaDB transaction as the idempotency record.

Handlers must not call `Commit`, roll back the transaction themselves, or open another transaction for changes that need to be atomic with event processing.

Avoid external side effects inside `Apply`, including HTTP requests and publishing messages. If processing must produce another event, consider a transactional outbox written in the same database transaction.

## Delivery, retries, and idempotency

`Projector.Project(ctx, event)` returns `nil` when every matching handler succeeds or has already processed the event.

It returns an error when conversion, persistence, or handler execution fails. Conversion occurs in the application, so conversion errors must be handled by the application's ingestion adapter.

The library does not acknowledge, reject, retry, or reschedule messages. The caller determines how a returned error affects message delivery.

### How idempotency works

For each matching handler, the library writes an idempotency record and applies the read-model update in the same database transaction.

This provides the following behavior:

1. If the transaction fails, the idempotency record and read-model changes are rolled back together.
2. If the transaction commits but message acknowledgment fails, redelivery is safe: the completed handler recognizes the event as already processed.
3. If a handler fails, the caller receives an error and can apply its own retry policy.
4. If an event matches multiple handlers, they are processed serially, each in its own transaction. A handler that already committed remains complete if a subsequent handler fails.

This is database-level idempotency, not a guarantee of exactly-once delivery across the entire distributed system. Delivery guarantees still depend on the source system and the application's acknowledgment and retry configuration.

### Unmatched events

If an event has no matching handlers, `Project` returns `nil`. Applications that require strict routing should validate event categories or enforce a strict-routing policy at the application layer.

Permanent failures can be retried indefinitely if the source system is configured that way. Configure retry limits, dead-letter queues, or equivalent mechanisms in the application's existing ingestion layer.

## Automatic schema creation

Projector does not require an external migration command for initial table creation.

During startup, `projector.Initialize(ctx)`:

1. Creates the internal idempotency table if it does not exist.
2. Calls `EnsureSchema(ctx, db)` for each registered handler.

Handlers can use `CREATE TABLE IF NOT EXISTS` to create their read-model tables automatically.

### Schema evolution

Automatic table creation is not a replacement for a schema migration strategy. `CREATE TABLE IF NOT EXISTS` does not update an existing table when a handler's schema changes.

For breaking changes, use versioned read models or introduce an explicit, reviewed schema-upgrade mechanism. Additive changes may also require deliberate upgrade logic depending on the database change.

## Operational notes

- Use a dedicated MariaDB user with only the required DDL and DML permissions.
- Call `Initialize` before starting message consumption.
- Avoid concurrent schema initialization across many replicas when startup DDL contention is a concern. Prefer controlled initialization.
- The current engine processes matching handlers serially for an individual event. Different application instances may process events concurrently.
- The unique idempotency key protects against duplicate processing, but event ordering remains the responsibility of the source system and application.
- Propagate the real processing context into `Project`; avoid replacing it with an unrelated background context.
- Keep broker-specific code, serialization, acknowledgments, retries, and dead-letter handling outside the library.

## Design principle

**Projector owns projection execution and database consistency. The application owns message ingestion and delivery semantics.**

This separation allows the library to be reused with different brokers, transport protocols, serialization formats, and application architectures without coupling its core to a specific consumption framework.
