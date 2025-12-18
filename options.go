package uniqw

import "time"

type options struct {
	id           string
	delay        time.Duration
	maxRetry     int
	retention    time.Duration
	errRetention time.Duration
	deadlineMs   int64
	keepUnique   bool

	// internal flags for testing or logic
	errRetentionSet bool
}

// Option is a function that configures task behavior during Enqueue or RetryDead.
type Option func(*options)

// TaskID sets a custom ID for the task. If not provided, a random UUID will be generated.
func TaskID(id string) Option {
	return func(o *options) {
		o.id = id
	}
}

// Delay schedules the task to be executed after the specified duration.
func Delay(d time.Duration) Option {
	return func(o *options) {
		o.delay = d
	}
}

// MaxRetry sets the maximum number of retry attempts for the task.
func MaxRetry(n int) Option {
	return func(o *options) {
		o.maxRetry = n
	}
}

// Retention sets how long (in seconds) the task is kept in the Succeeded state.
func Retention(d time.Duration) Option {
	return func(o *options) {
		o.retention = d
	}
}

// RetentionError sets how long (in seconds) the task is kept in the Dead state.
// If d is 0, the task will be dropped immediately after final failure.
// If d is negative, the task will be kept forever (default).
func RetentionError(d time.Duration) Option {
	return func(o *options) {
		o.errRetention = d
		o.errRetentionSet = true
	}
}

// ExpireIn sets a relative deadline for the task. The task will not be processed
// if the current time is past the deadline.
func ExpireIn(d time.Duration) Option {
	return func(o *options) {
		o.deadlineMs = time.Now().Add(d).UnixMilli()
	}
}

// Deadline sets an absolute deadline for the task. The task will not be processed
// if the current time is past the deadline.
func Deadline(t time.Time) Option {
	return func(o *options) {
		if !t.IsZero() {
			o.deadlineMs = t.UnixMilli()
		}
	}
}

// WithKeepUniqueLock ensures that the uniqueness lock for the task ID is NOT
// released even after the task is deleted (unless it reached Succeeded state).
func WithKeepUniqueLock() Option {
	return func(o *options) {
		o.keepUnique = true
	}
}
