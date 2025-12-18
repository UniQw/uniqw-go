package uniqw

import (
	"context"

	"github.com/UniQw/uniqw-go/internal/hctx"
)

// SetProgress allows a handler to report progress (0..100) for the current task.
// It is a no-op if the context is not provided by the UniQw runtime.
func SetProgress(ctx context.Context, p int) {
	st, ok := hctx.From(ctx)
	if !ok || st == nil {
		return
	}
	if p < 0 {
		p = 0
	} else if p > 100 {
		p = 100
	}
	st.Progress = p
}

// SetResult encodes the provided value using the default JSON encoder and
// attaches it as the handler result. It is safe to call multiple times; last wins.
// It is a no-op if the context is not provided by the UniQw runtime.
func SetResult(ctx context.Context, v any) error {
	st, ok := hctx.From(ctx)
	if !ok || st == nil {
		return nil
	}
	// Use the default JSONEncoder to stay consistent with payloads
	var enc Encoder = &JSONEncoder{}
	b, err := enc.Encode(v)
	if err != nil {
		return err
	}
	st.Result = b
	return nil
}

// SetResultBytes attaches raw bytes as the handler result without encoding.
// It is a no-op if the context is not provided by the UniQw runtime.
func SetResultBytes(ctx context.Context, b []byte) {
	st, ok := hctx.From(ctx)
	if !ok || st == nil {
		return
	}
	st.Result = b
}
