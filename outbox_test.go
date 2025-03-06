package outbox

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestWriteEvent verifies that WriteEvent inserts an event atomically within a transaction.
func TestWriteEvent(t *testing.T) {
	// Use in-memory SQLite for testing.
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	assert.NoError(t, err, "failed to open database")

	// Auto-migrate the OutboxEvent table using the processor's AutoMigrate method.
	processor := NewOutboxProcessor(db, WithAutoDelete(false))
	err = processor.AutoMigrate()
	assert.NoError(t, err, "failed to auto-migrate table")

	// Run a transaction that writes an event.
	err = db.Transaction(func(tx *gorm.DB) error {
		return processor.WriteEvent(tx, "TestEvent", map[string]interface{}{
			"key": "value",
		})
	})
	assert.NoError(t, err, "transaction failed")

	// Query the event from the DB.
	var event OutboxEvent
	err = db.First(&event).Error
	assert.NoError(t, err, "failed to query outbox event")

	// Unmarshal the payload to verify content.
	var payload map[string]interface{}
	err = json.Unmarshal(event.Payload, &payload)
	assert.NoError(t, err, "failed to unmarshal payload")

	assert.Equal(t, "value", payload["key"], "unexpected payload value")
}

// TestProcessEventsStop verifies that ProcessEvents stops gracefully and uses the custom event handler.
func TestProcessEventsStop(t *testing.T) {
	// Use in-memory SQLite for testing.
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	assert.NoError(t, err, "failed to open database")

	// Auto-migrate the OutboxEvent table.
	processor := NewOutboxProcessor(db, WithAutoDelete(true), WithBatchSize(1), WithInterval(500*time.Millisecond),
		WithEventHandler(func(eventType string, payload []byte) error {
			// In this test, we simply log the event.
			// Simulate successful event handling.
			return nil
		}))
	err = processor.AutoMigrate()
	assert.NoError(t, err, "failed to auto-migrate table")

	// Insert an event manually.
	event := OutboxEvent{
		EventType: "TestEvent",
		Payload:   json.RawMessage(`{"key": "value"}`),
		Status:    "pending",
		CreatedAt: time.Now(),
	}
	err = db.Create(&event).Error
	assert.NoError(t, err, "failed to create test event")

	// Start processing events in the background.
	go processor.ProcessEvents()

	// Wait enough time for ProcessEvents to pick up the event.
	time.Sleep(2 * time.Second)

	// Stop the processor.
	processor.Stop()

	// Allow some time for the background routine to terminate.
	time.Sleep(500 * time.Millisecond)

	// Verify that the event has been processed and deleted.
	var count int64
	err = db.Model(&OutboxEvent{}).Where("id = ?", event.ID).Count(&count).Error
	assert.NoError(t, err, "failed to count events")
	assert.Equal(t, int64(0), count, "expected event to be deleted")
}
