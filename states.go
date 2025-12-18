package uniqw

// State represents a queue state used to store and inspect tasks.
// Use the exported constants (StatePending, StateActive, etc.) instead of
// raw strings to avoid typos.
type State string

const (
	// StatePending contains tasks ready for execution (LIST).
	StatePending State = "pending"
	// StateActive contains tasks currently being processed by workers (ZSET).
	StateActive State = "active"
	// StateDelayed contains scheduled tasks or tasks in backoff retry (ZSET).
	StateDelayed State = "delayed"
	// StateSucceeded contains successfully completed tasks (ZSET).
	StateSucceeded State = "succeeded"
	// StateDead contains permanently failed tasks (LIST).
	StateDead State = "dead"
)

// AllStates lists every valid queue state in a stable order.
var AllStates = []State{StatePending, StateActive, StateDelayed, StateSucceeded, StateDead}

// String returns the raw string value of the state.
func (s State) String() string { return string(s) }

// ParseState converts a string into a State, returning an error for unknown values.
func ParseState(s string) (State, error) {
	switch s {
	case string(StatePending):
		return StatePending, nil
	case string(StateActive):
		return StateActive, nil
	case string(StateDelayed):
		return StateDelayed, nil
	case string(StateSucceeded):
		return StateSucceeded, nil
	case string(StateDead):
		return StateDead, nil
	default:
		return "", ErrUnknownState
	}
}
