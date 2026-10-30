package projection

import (
	"fmt"
	"reflect"
)

// Registry stores handlers in registration order.
type Registry struct {
	handlers []Handler
	names    map[string]struct{}
}

func NewRegistry() *Registry {
	return &Registry{names: make(map[string]struct{})}
}

func (r *Registry) Register(handler Handler) error {
	if r == nil {
		return fmt.Errorf("registry is nil")
	}
	if handler == nil {
		return fmt.Errorf("handler is nil")
	}
	// An interface containing a typed nil pointer is not equal to nil.
	v := reflect.ValueOf(handler)
	if (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface ||
		v.Kind() == reflect.Map || v.Kind() == reflect.Func ||
		v.Kind() == reflect.Slice) && v.IsNil() {
		return fmt.Errorf("handler is nil")
	}
	if handler.Name() == "" {
		return fmt.Errorf("handler name is required")
	}
	if r.names == nil {
		r.names = make(map[string]struct{})
	}
	if _, exists := r.names[handler.Name()]; exists {
		return fmt.Errorf("handler %q already registered", handler.Name())
	}
	r.names[handler.Name()] = struct{}{}
	r.handlers = append(r.handlers, handler)
	return nil
}

func (r *Registry) Handlers() []Handler {
	if r == nil {
		return nil
	}
	return append([]Handler(nil), r.handlers...)
}
