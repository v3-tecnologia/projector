package projection

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

type fakeHandler struct {
	name     string
	category string
	applyErr error
	applied  int
}

func (h *fakeHandler) Name() string { return h.name }
func (h *fakeHandler) Handles(category string) bool { return category == h.category }
func (h *fakeHandler) EnsureSchema(context.Context, *sql.DB) error { return nil }
func (h *fakeHandler) Apply(context.Context, *sql.Tx, Event) error {
	h.applied++
	return h.applyErr
}

type fakeStore struct {
	initialized bool
	processErr  error
	processed   int
}

func (s *fakeStore) Initialize(context.Context, []Handler) error {
	s.initialized = true
	return nil
}
func (s *fakeStore) Process(context.Context, Handler, Event) error {
	s.processed++
	return s.processErr
}

func TestProjectRequiresInitialize(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(&fakeHandler{name: "h", category: "a"}); err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{}
	p, err := NewProjector(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	err = p.Project(context.Background(), Event{ID: "1", Category: "a"})
	if err == nil {
		t.Fatal("expected not initialized error")
	}
}

func TestProjectDispatchesMatchingHandler(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(&fakeHandler{name: "a-handler", category: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(&fakeHandler{name: "b-handler", category: "b"}); err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{}
	p, err := NewProjector(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Project(context.Background(), Event{ID: "1", Category: "a"}); err != nil {
		t.Fatal(err)
	}
	if store.processed != 1 {
		t.Fatalf("expected 1 processed handler, got %d", store.processed)
	}
}

func TestAlreadyProcessedIsSuccess(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(&fakeHandler{name: "a-handler", category: "a"}); err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{processErr: ErrAlreadyProcessed}
	p, err := NewProjector(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Project(context.Background(), Event{ID: "1", Category: "a"}); err != nil {
		t.Fatalf("duplicate should be successful: %v", err)
	}
}

func TestStoreErrorIsReturned(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(&fakeHandler{name: "a-handler", category: "a"}); err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{processErr: errors.New("db unavailable")}
	p, err := NewProjector(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Project(context.Background(), Event{ID: "1", Category: "a"}); err == nil {
		t.Fatal("expected store error")
	}
}
