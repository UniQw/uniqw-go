package keys

// Package keys centralizes Redis key construction.
// It is kept in internal to avoid leaking key formats to public API.

func Pending(q string) string   { return "uniqw:{" + q + "}:pending" }
func Active(q string) string    { return "uniqw:{" + q + "}:active" }
func Delayed(q string) string   { return "uniqw:{" + q + "}:delayed" }
func Dead(q string) string      { return "uniqw:{" + q + "}:dead" }
func Succeeded(q string) string { return "uniqw:{" + q + "}:succeeded" }

// DeadExpiry is a ZSET index that tracks when dead-list members should be purged.
// Members are the raw task JSON; scores are absolute expiration timestamps in ms.
func DeadExpiry(q string) string { return "uniqw:{" + q + "}:dead_expiry" }

// Queue holds all precomputed keys for a queue name to avoid repeated concatenations.
type Queue struct {
	Pending    string
	Active     string
	Delayed    string
	Dead       string
	Succeeded  string
	Unique     string
	Expiry     string
	DeadExpiry string
}

// For returns a set of precomputed keys for the provided queue.
func For(q string) Queue {
	prefix := "uniqw:{" + q + "}:"
	return Queue{
		Pending:    prefix + "pending",
		Active:     prefix + "active",
		Delayed:    prefix + "delayed",
		Dead:       prefix + "dead",
		Succeeded:  prefix + "succeeded",
		Unique:     prefix + "unique",
		Expiry:     prefix + "expiry",
		DeadExpiry: prefix + "dead_expiry",
	}
}

// Unique returns the per-queue Set key that tracks used task IDs for de-duplication.
func Unique(q string) string { return "uniqw:{" + q + "}:unique" }

// Expiry returns the per-queue ZSET key that indexes deadlines for jobs.
func Expiry(q string) string { return "uniqw:{" + q + "}:expiry" }
