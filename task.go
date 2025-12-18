package uniqw

// Task represents a unit of work to be processed by a worker.
// It is serialized to JSON and stored in Redis.
type Task struct {
	// ID is the unique identifier for the task.
	ID string `json:"id"`
	// Type defines the task category, used by Mux to route to the correct handler.
	Type string `json:"type"`
	// Queue is the name of the queue this task belongs to.
	Queue string `json:"queue"`
	// Payload is the raw task data.
	Payload []byte `json:"payload"`
	// Retry is the current number of retry attempts made.
	Retry int `json:"retry"`
	// MaxRetry is the maximum number of retries allowed before moving to Dead state.
	MaxRetry int `json:"max_retry"`
	// Retention is the duration (in seconds) to keep the task after successful completion.
	Retention int64 `json:"retention"`
	// ErrRetention is the duration (in seconds) to keep the task after it has permanently failed.
	ErrRetention int64 `json:"err_retention,omitempty"`
	// CreatedAt is the timestamp (ms) when the task was enqueued.
	CreatedAt int64 `json:"created_at,omitempty"`
	// DeadlineMs is the absolute timestamp (ms) after which the task should not be processed.
	DeadlineMs int64 `json:"deadline_ms,omitempty"`
	// StartedAt is the timestamp (ms) when the worker started processing the task.
	StartedAt int64 `json:"started_at,omitempty"`
	// CompletedAt is the timestamp (ms) when the task was finished (success or final failure).
	CompletedAt int64 `json:"completed_at,omitempty"`
	// LastError is the error message from the last failed attempt.
	LastError string `json:"last_error,omitempty"`
	// LastErrorAt is the timestamp (ms) of the last failed attempt.
	LastErrorAt int64 `json:"last_error_at,omitempty"`
	// Progress is the current task progress (0..100).
	Progress int `json:"progress,omitempty"`
	// Result is the execution result stored as JSON.
	Result []byte `json:"result,omitempty"`
}
