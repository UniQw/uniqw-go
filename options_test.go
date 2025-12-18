package uniqw

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptions_Setters(t *testing.T) {
	var o options

	TaskID("id-1")(&o)
	require.Equal(t, "id-1", o.id, "TaskID not set")

	Delay(3 * time.Second)(&o)
	require.Equal(t, 3*time.Second, o.delay, "Delay not set")

	MaxRetry(7)(&o)
	require.Equal(t, 7, o.maxRetry, "MaxRetry not set")

	Retention(5 * time.Minute)(&o)
	require.Equal(t, 5*time.Minute, o.retention, "Retention not set")

	// Deadline relative
	ExpireIn(1 * time.Second)(&o)
	require.NotZero(t, o.deadlineMs, "ExpireIn should set deadlineMs")

	// Absolute deadline overrides
	o.deadlineMs = 0
	t0 := time.Now().Add(10 * time.Second)
	Deadline(t0)(&o)
	require.Equal(t, t0.UnixMilli(), o.deadlineMs, "Deadline not set correctly")

	// Zero values should not set deadline
	o.deadlineMs = 0
	Deadline(time.Time{})(&o)
	require.Zero(t, o.deadlineMs, "Zero Deadline should not set")
}

func TestOption_RetentionError(t *testing.T) {
	var o options
	// default: not set
	require.False(t, o.errRetentionSet)
	require.Zero(t, o.errRetention)

	RetentionError(0)(&o)
	require.True(t, o.errRetentionSet)
	require.Equal(t, time.Duration(0), o.errRetention)

	RetentionError(5 * time.Minute)(&o)
	require.True(t, o.errRetentionSet)
	require.Equal(t, 5*time.Minute, o.errRetention)
}
