package uniqw

import "context"

// HandlerFunc is the function signature for processing a task.
type HandlerFunc func(ctx context.Context, payload []byte) error

// Middleware is a function that wraps a HandlerFunc to provide cross-cutting concerns.
type Middleware func(HandlerFunc) HandlerFunc

type handler struct {
	exec HandlerFunc
}

// Mux routes tasks to their respective handlers based on task type.
type Mux struct {
	handlers    map[string]handler
	encoder     Encoder
	middlewares []Middleware
}

// NewMux creates a new Task Mux.
func NewMux() *Mux {
	return &Mux{
		handlers:    make(map[string]handler),
		encoder:     &JSONEncoder{},
		middlewares: []Middleware{},
	}
}

// Handle registers a handler for a specific task type.
func (m *Mux) Handle(taskType string, fn func(context.Context, []byte) error) {
	m.handlers[taskType] = handler{
		exec: fn,
	}
}

// Use adds middleware(s) to the mux. Middlewares are executed in the order they are added.
func (m *Mux) Use(mw Middleware) {
	m.middlewares = append(m.middlewares, mw)
}

func (m *Mux) wrapHandler(h HandlerFunc) HandlerFunc {
	for i := len(m.middlewares) - 1; i >= 0; i-- {
		h = m.middlewares[i](h)
	}
	return h
}
