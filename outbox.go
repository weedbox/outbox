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
	autoDelete bool          // if true, events will be deleted after successful MQ delivery; otherwise, status is updated to "sent"
	batchSize  int           // number of events to process per batch
	interval   time.Duration // polling interval between processing batches

	// Internal context and cancel function to control background processing.
	ctx    context.Context
	cancel context.CancelFunc
}

// Option is a functional option type for configuring OutboxProcessor.
type Option func(*OutboxProcessor)

// WithAutoDelete configures whether to auto-delete events after successful delivery.
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

// NewOutboxProcessor creates a new OutboxProcessor instance with the provided options.
// It also initializes an internal context and cancel function.
func NewOutboxProcessor(db *gorm.DB, opts ...Option) *OutboxProcessor {
	// Create a cancellable context for controlling the background processing.
	ctx, cancel := context.WithCancel(context.Background())
	processor := &OutboxProcessor{
		db:         db,
		autoDelete: false,           // default is to update status instead of deletion
		batchSize:  10,              // default batch size is 10
		interval:   3 * time.Second, // default polling interval is 3 seconds
		ctx:        ctx,
		cancel:     cancel,
	}
	for _, opt := range opts {
		opt(processor)
	}
	return processor
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

// sendToMQ simulates sending an event to a message queue.
// Replace this with your actual MQ client logic.
func sendToMQ(eventType string, payload []byte) error {
	log.Printf("Sending MQ event: type=%s, payload=%s", eventType, payload)
	// Simulate a successful send.
	return nil
}

// ProcessEvents continuously polls and processes pending outbox events.
// It will stop processing when the internal context is cancelled.
func (op *OutboxProcessor) ProcessEvents() {
	for {
		select {
		case <-op.ctx.Done():
			log.Println("Stopping outbox event processing.")
			return
		default:
			// Proceed with processing.
		}

		var events []OutboxEvent

		// Begin a transaction and use FOR UPDATE SKIP LOCKED to lock rows for processing.
		tx := op.db.Begin()
		err := tx.
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("status = ?", "pending").
			Limit(op.batchSize).
			Find(&events).Error
		if err != nil {
			tx.Rollback()
			log.Printf("Failed to query outbox events: %v", err)
			time.Sleep(op.interval)
			continue
		}
		tx.Commit()

		// If no events are found, wait for the next polling cycle.
		if len(events) == 0 {
			time.Sleep(op.interval)
			continue
		}

		// Process each event.
		for _, event := range events {
			if err := sendToMQ(event.EventType, event.Payload); err != nil {
				log.Printf("Failed to send event ID %d: %v", event.ID, err)
				continue
			}

			if op.autoDelete {
				// Delete the event if autoDelete is enabled.
				if err := op.db.Delete(&event).Error; err != nil {
					log.Printf("Failed to delete event ID %d: %v", event.ID, err)
				} else {
					log.Printf("Event ID %d sent successfully and deleted", event.ID)
				}
			} else {
				// Otherwise, update the event status to "sent" and record the sent time.
				if err := op.db.Model(&event).Updates(map[string]interface{}{
					"status":  "sent",
					"sent_at": time.Now(),
				}).Error; err != nil {
					log.Printf("Failed to update event ID %d: %v", event.ID, err)
				} else {
					log.Printf("Event ID %d sent successfully and status updated", event.ID)
				}
			}
		}
	}
}

// Stop terminates the background processing of outbox events by cancelling the internal context.
func (op *OutboxProcessor) Stop() {
	op.cancel()
}
