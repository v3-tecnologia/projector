package projection

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Store is the persistence contract required by Projector.
type Store interface {
	Initialize(ctx context.Context, handlers []Handler) error
	Process(ctx context.Context, handler Handler, event Event) error
}

// ErrAlreadyProcessed is returned by a Store when this handler has committed
// the event previously. Projector treats it as successful idempotent delivery.
var ErrAlreadyProcessed = errors.New("event already processed")

type Projector interface {
	Initialize(ctx context.Context) error
	Project(ctx context.Context, event Event) error
}

type projector struct {
	store    Store
	handlers []Handler

	initMu sync.Mutex
	ready  bool
}

func NewProjector(store Store, registry *Registry) (Projector, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if registry == nil {
		return nil, fmt.Errorf("registry is required")
	}
	handlers := registry.Handlers()
	seen := make(map[string]struct{}, len(handlers))
	for _, h := range handlers {
		if h == nil {
			return nil, fmt.Errorf("registry contains nil handler")
		}
		name := strings.TrimSpace(h.Name())
		if name == "" {
			return nil, fmt.Errorf("handler name is required")
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate handler name %q", name)
		}
		seen[name] = struct{}{}
	}
	return &projector{store: store, handlers: handlers}, nil
}

// Initialize creates internal persistence structures and asks every handler to
// create its read-model schema. Call this before starting Subscribe consumers.
func (p *projector) Initialize(ctx context.Context) error {
	p.initMu.Lock()
	defer p.initMu.Unlock()

	if p.ready {
		return nil
	}
	if err := p.store.Initialize(ctx, append([]Handler(nil), p.handlers...)); err != nil {
		return fmt.Errorf("initialize projection store: %w", err)
	}
	p.ready = true
	return nil
}

func (p *projector) Project(ctx context.Context, event Event) error {
	if !p.isReady() {
		return fmt.Errorf("projector is not initialized; call Initialize before consuming")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(event.ID) == "" {
		return fmt.Errorf("event ID is required")
	}
	if strings.TrimSpace(event.Category) == "" {
		return fmt.Errorf("event category is required")
	}

	matched := false
	for _, handler := range p.handlers {
		if !handler.Handles(event.Category) {
			continue
		}
		matched = true
		err := p.store.Process(ctx, handler, event)
		if err != nil && !errors.Is(err, ErrAlreadyProcessed) {
			return fmt.Errorf("project event %q with handler %q: %w", event.ID, handler.Name(), err)
		}
	}
	_ = matched // Unmatched events are intentionally acknowledged as no-ops.
	return nil
}

func (p *projector) isReady() bool {
	p.initMu.Lock()
	defer p.initMu.Unlock()
	return p.ready
}
