package outbox

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// OutboxEvent defines the structure for the outbox events table.
type OutboxEvent struct {
	ID        uint            `gorm:"primaryKey"`
	EventType string          `gorm:"type:varchar(255);not null"`
	Payload   json.RawMessage `gorm:"type:jsonb;not null"`
	Status    string          `gorm:"type:varchar(50);default:'pending'"`
	CreatedAt time.Time
	SentAt    *time.Time
}

// OutboxProcessor handles the outbox mechanism: writing and processing outbox events.
type OutboxProcessor struct {
	db         *gorm.DB
	autoDelete bool          // if true, events will be deleted after successful handling; otherwise, status is updated to "sent"
	batchSize  int           // number of events to process per batch
	interval   time.Duration // polling interval between processing batches

	// eventHandler is a customizable function to process events.
	eventHandler func(eventType string, payload []byte) error

	// Internal context and cancel function to control background processing.
	ctx    context.Context
	cancel context.CancelFunc
}

// Option is a functional option type for configuring OutboxProcessor.
type Option func(*OutboxProcessor)

// WithAutoDelete configures whether to auto-delete events after successful handling.
func WithAutoDelete(autoDelete bool) Option {
	return func(op *OutboxProcessor) {
		op.autoDelete = autoDelete
	}
}

// WithBatchSize configures the number of events to process in each batch.
func WithBatchSize(size int) Option {
	return func(op *OutboxProcessor) {
		op.batchSize = size
	}
}

// WithInterval configures the polling interval.
func WithInterval(d time.Duration) Option {
	return func(op *OutboxProcessor) {
		op.interval = d
	}
}

// WithEventHandler allows you to provide a custom function to handle events.
func WithEventHandler(handler func(eventType string, payload []byte) error) Option {
	return func(op *OutboxProcessor) {
		op.eventHandler = handler
	}
}

// NewOutboxProcessor creates a new OutboxProcessor instance with the provided options.
// It also initializes an internal context and cancel function.
func NewOutboxProcessor(db *gorm.DB, opts ...Option) *OutboxProcessor {
	// Create a cancellable context for controlling the background processing.
	ctx, cancel := context.WithCancel(context.Background())
	processor := &OutboxProcessor{
		db:         db,
		autoDelete: false,           // default: update status instead of deletion
		batchSize:  10,              // default batch size is 10
		interval:   3 * time.Second, // default polling interval is 3 seconds
		ctx:        ctx,
		cancel:     cancel,
		// Default eventHandler function.
		eventHandler: func(eventType string, payload []byte) error {
			log.Printf("Handling event: type=%s, payload=%s", eventType, payload)
			// Simulate successful event handling.
			return nil
		},
	}
	for _, opt := range opts {
		opt(processor)
	}
	return processor
}

// AutoMigrate runs auto migration for the OutboxEvent table.
func (op *OutboxProcessor) AutoMigrate() error {
	return op.db.AutoMigrate(&OutboxEvent{})
}

// WriteEvent writes an outbox event using the provided transaction.
// This ensures that the outbox event insertion is atomic with your business updates.
func (op *OutboxProcessor) WriteEvent(tx *gorm.DB, eventType string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	event := OutboxEvent{
		EventType: eventType,
		Payload:   data,
		Status:    "pending",
		CreatedAt: time.Now(),
	}

	return tx.Create(&event).Error
}

// ProcessEvents continuously polls and processes pending outbox events in a single transaction.
// The transaction wraps both the event fetching (with SKIP LOCKED), the event handling via the custom eventHandler,
// and the finalization (update or deletion) of the events. This ensures that once the transaction commits,
// the events have been finalized and cannot be picked up by another worker.
func (op *OutboxProcessor) ProcessEvents() {
	for {
		select {
		case <-op.ctx.Done():
			return
		default:
			// Continue processing.
		}

		// Wrap fetching, processing, and finalization in one transaction.
		err := op.db.Transaction(func(tx *gorm.DB) error {
			var events []OutboxEvent
			// Fetch events with status "pending" using SKIP LOCKED.
			if err := tx.
				Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
				Where("status = ?", "pending").
				Limit(op.batchSize).
				Find(&events).Error; err != nil {
				return err
			}

			// If no events are found, simply return nil.
			if len(events) == 0 {
				return nil
			}

			handledEvents := make([]uint, 0, len(events))

			// Process each event and finalize it within the same transaction.
			for _, event := range events {
				// Handle the event using the custom event handler.
				if err := op.eventHandler(event.EventType, event.Payload); err != nil {
					continue
				}

				// Keep track of the handled events to finalize them later.
				handledEvents = append(handledEvents, event.ID)
			}

			// Finalize the event by either deleting it or updating its status to "sent".
			if op.autoDelete {
				if err := tx.Unscoped().Where("id IN ?", handledEvents).Delete(&OutboxEvent{}).Error; err != nil {
					return err
				}
			} else {

				if err := tx.Model(&OutboxEvent{}).
					Where("id IN ?", handledEvents).
					Updates(map[string]interface{}{
						"status":  "sent",
						"sent_at": time.Now(),
					}).Error; err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			log.Printf("Failed to process events: %v", err)
		}

		time.Sleep(op.interval)
	}
}

// Stop terminates the background processing of outbox events by cancelling the internal context.
func (op *OutboxProcessor) Stop() {
	op.cancel()
}
