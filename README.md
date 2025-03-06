# Outbox

Outbox is a Go package that implements an outbox mechanism using GORM and PostgreSQL. This package is designed to reliably handle events stored in an outbox table and deliver them to a message queue (MQ). It helps ensure eventual consistency between business data updates and event publishing by providing an atomic integration of event writing within external transactions.

## Features

- **Atomic Integration:**  
  Provides the `WriteEvent` method, which can be used inside an external transaction to atomically insert an outbox event alongside your business logic updates.

- **Background Event Processing:**  
  The `ProcessEvents` method continuously polls the database for pending events using PostgreSQL’s `FOR UPDATE SKIP LOCKED` clause, ensuring that concurrent processors do not duplicate work.

- **Configurable Behavior:**  
  Easily configurable using functional options to set parameters such as batch size, polling interval, and whether to auto-delete events after successful delivery.

- **Graceful Shutdown:**  
  Includes a `Stop` method to terminate the background processing of outbox events gracefully using an internal cancellable context.

---

## Installation

Ensure you have the following dependencies installed:

- [Go](https://golang.org/)
- [GORM](https://gorm.io/)
- [PostgreSQL Driver for GORM](https://gorm.io/docs/connecting_to_the_database.html#PostgreSQL)

Use Go modules to manage dependencies:

```bash
go mod init your_module_name
go get -u gorm.io/gorm
go get -u gorm.io/driver/postgres
```

## Usage

### Initializing OutboxProcessor

First, set up your PostgreSQL connection and auto-migrate the outbox_events table. Then, create an instance of OutboxProcessor with your desired configuration:

```go
dsn := "host=localhost user=postgres password=yourpassword dbname=yourdb port=5432 sslmode=disable TimeZone=Asia/Taipei"
db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
if err != nil {
    log.Fatalf("Failed to connect to database: %v", err)
}

// Auto-migrate the OutboxEvent table
if err := db.AutoMigrate(&OutboxEvent{}); err != nil {
    log.Fatalf("Failed to auto-migrate table: %v", err)
}

processor := NewOutboxProcessor(db, WithAutoDelete(true), WithBatchSize(10), WithInterval(3*time.Second))
```

### Writing an Outbox Event Within a Transaction

In your external business logic, wrap your business update and outbox event insertion in a single transaction to ensure atomicity. Use the WriteEvent method to write the event:

```go
err = db.Transaction(func(tx *gorm.DB) error {
    // Your external business logic update (e.g., updating a user's balance)

    // Write an outbox event atomically within the same transaction.
    if err := processor.WriteEvent(tx, "UserTransaction", map[string]interface{}{
        "userId":          12345,
        "amount":          100.0,
        "transactionTime": time.Now(),
    }); err != nil {
        return err
    }

    return nil
})
if err != nil {
    log.Printf("Transaction failed: %v", err)
} else {
    log.Println("Transaction processed successfully")
}
```


### Starting and Stopping Background Event Processing

Start the background processing by calling ProcessEvents. When you need to stop processing (e.g., before shutting down your application), call the Stop method:

```go
// Start processing outbox events in the background.
go processor.ProcessEvents()

// Run your application...

// When ready to stop background processing:
processor.Stop()
```

## API

`WriteEventTx(tx *gorm.DB, eventType string, payload interface{}) error`
Writes an outbox event within the provided transaction. This ensures that the event is inserted atomically along with your business updates.

`ProcessEvents()`
Continuously polls for pending outbox events and processes them. It uses a cancellable context internally and listens for shutdown signals to stop processing gracefully.

`Stop()`
Terminates the background processing of outbox events by cancelling the internal context.

## License

This project is licensed under the Apache Public License (APL). See the LICENSE file for details.
