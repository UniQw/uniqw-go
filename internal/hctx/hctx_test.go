package hctx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestState_NewAndWithFrom(t *testing.T) {
	st := New()
	require.NotNil(t, st)
	st.Progress = 10
	st.Result = []byte("x")

	ctx := WithState(context.Background(), st)
	got, ok := From(ctx)
	require.True(t, ok, "From should find state")
	require.Same(t, st, got, "should retrieve the same pointer")
}

func TestState_From_Absent(t *testing.T) {
	ctx := context.Background()
	st, ok := From(ctx)
	require.False(t, ok)
	require.Nil(t, st)
}
