package domain

// ScheduledEventStatus represents a persisted scheduled-event lifecycle state.
// Keep values shared by API, scheduler, and storage adapters to prevent drift.
type ScheduledEventStatus string

const (
	// ScheduledEventStatusPending indicates the event is waiting to be claimed.
	ScheduledEventStatusPending ScheduledEventStatus = "pending"
	// ScheduledEventStatusClaimed indicates the event has been claimed by a worker.
	ScheduledEventStatusClaimed ScheduledEventStatus = "claimed"
	// ScheduledEventStatusReleased indicates the event was released back to pending.
	ScheduledEventStatusReleased ScheduledEventStatus = "released"
	// ScheduledEventStatusFailed indicates the event failed processing.
	ScheduledEventStatusFailed ScheduledEventStatus = "failed"
	// ScheduledEventStatusCancelled indicates the event was cancelled.
	ScheduledEventStatusCancelled ScheduledEventStatus = "cancelled"
)
