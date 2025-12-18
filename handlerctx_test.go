package uniqw

import (
	"context"
	"testing"

	"github.com/UniQw/uniqw-go/internal/hctx"
	"github.com/stretchr/testify/require"
)

func TestHandlerCtx_NoState_NoPanic(t *testing.T) {
	ctx := context.Background()
	// should be no-op and no panic
	SetProgress(ctx, 50)
	require.NoError(t, SetResult(ctx, map[string]int{"a": 1}))
	SetResultBytes(ctx, []byte("x"))
}

func TestHandlerCtx_WithState_ProgressAndResult(t *testing.T) {
	st := hctx.New()
	ctx := hctx.WithState(context.Background(), st)

	// progress clamps 0..100
	SetProgress(ctx, -10)
	require.Equal(t, 0, st.Progress)
	SetProgress(ctx, 150)
	require.Equal(t, 100, st.Progress)
	SetProgress(ctx, 42)
	require.Equal(t, 42, st.Progress)

	// SetResult JSON encodes
	require.NoError(t, SetResult(ctx, map[string]any{"ok": true}))
	require.NotEmpty(t, st.Result)

	// Override with raw bytes
	SetResultBytes(ctx, []byte("raw"))
	require.Equal(t, []byte("raw"), st.Result)
}
