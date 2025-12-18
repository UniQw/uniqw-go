package uniqw

import (
	"context"
	"testing"
)

func TestMux_MiddlewareOrderAndOverwrite(t *testing.T) {
	m := NewMux()

	order := []int{}
	mw1 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, b []byte) error {
			order = append(order, 1)
			return next(ctx, b)
		}
	}
	mw2 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, b []byte) error {
			order = append(order, 2)
			return next(ctx, b)
		}
	}
	m.Use(mw1)
	m.Use(mw2)

	called := 0
	m.Handle("t", func(ctx context.Context, b []byte) error { called++; return nil })
	// overwrite handler
	m.Handle("t", func(ctx context.Context, b []byte) error { called += 10; return nil })

	h := m.wrapHandler(m.handlers["t"].exec)
	_ = h(context.Background(), nil)

	if called != 10 {
		t.Fatalf("expected overwritten handler to run (10), got %d", called)
	}
	// middleware applied in registration order: mw1 outer, then mw2
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("unexpected middleware order: %+v", order)
	}
}
