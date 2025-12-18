package hctx

import "context"

// State holds per-execution, handler-provided metadata that the runtime
// can capture after handler returns.
type State struct {
	Progress int
	Result   []byte
}

// New creates a fresh handler state container.
func New() *State { return &State{} }

type ctxKey struct{}

// WithState returns a child context carrying the given handler state.
func WithState(parent context.Context, s *State) context.Context {
	return context.WithValue(parent, ctxKey{}, s)
}

// From extracts the handler state from context if present.
func From(ctx context.Context) (*State, bool) {
	v := ctx.Value(ctxKey{})
	if v == nil {
		return nil, false
	}
	st, ok := v.(*State)
	return st, ok
}
