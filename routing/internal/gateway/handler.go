// Package gateway exposes the Duro HTTP API.
package gateway

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/jsonvalue"
)

const (
	maxRequest      = 64 << 10
	maxPackage      = 1 << 20
	maxRegistration = 2 << 20
)

type Backend interface {
	Worker(context.Context, string) (pb.WorkerServiceClient, error)
	Candidate(context.Context, string) (string, error)
	Create(context.Context, *pb.CreateOperation) (*pb.ReserveCreateResponse, error)
}

type Assigner interface {
	Assign(context.Context, string) (*pb.OwnerLease, error)
}

type Event struct {
	Operation, Code, ObjectID, RequestID, WorkerID string
	Epoch                                          uint64
	Duration                                       time.Duration
}

type Config struct {
	Token                                             string
	Timeout                                           time.Duration
	MaxRequests, MaxLookups, CacheEntries, CacheBytes int
	CacheTTL                                          time.Duration
	OnRequest                                         func(Event)
}

type Handler struct {
	cfg       Config
	placement pb.PlacementServiceClient
	backend   Backend
	slots     chan struct{}
	routes    *resolver
}

func New(cfg Config, placement pb.PlacementServiceClient, backend Backend) *Handler {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.MaxRequests <= 0 {
		cfg.MaxRequests = 256
	}
	if cfg.MaxLookups <= 0 {
		cfg.MaxLookups = 64
	}
	if cfg.CacheEntries <= 0 {
		cfg.CacheEntries = 100_000
	}
	if cfg.CacheBytes <= 0 {
		cfg.CacheBytes = 64 << 20
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 30 * time.Second
	}
	h := &Handler{cfg: cfg, placement: placement, backend: backend, slots: make(chan struct{}, cfg.MaxRequests)}
	h.routes = newResolver(cfg, h.findOwner)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	event := Event{Operation: "unknown"}
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	var payload any
	err := h.admit(w, r, &event)
	if err == nil {
		defer func() { <-h.slots }()
		payload, err = h.handle(w, r, start, &event)
	}
	if err != nil {
		e := publicError(err)
		event.Code = e.Code
		w.WriteHeader(e.status)
		_ = encoder.Encode(e)
	} else {
		event.Code = "OK"
		_ = encoder.Encode(payload)
	}
	if h.cfg.OnRequest != nil {
		event.Duration = time.Since(start)
		h.cfg.OnRequest(event)
	}
}

func (h *Handler) admit(w http.ResponseWriter, r *http.Request, e *Event) error {
	switch r.URL.Path {
	case "/v1/create", "/v1/invoke", "/v1/registerType", "/v1/describe", "/v1/read":
		e.Operation = strings.TrimPrefix(r.URL.Path, "/v1/")
	default:
		return notFound
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		return &apiError{"INVALID_ARGUMENT", "POST required", http.StatusMethodNotAllowed}
	}
	auth := r.Header.Values("Authorization")
	if h.cfg.Token == "" || len(auth) != 1 || subtle.ConstantTimeCompare([]byte(auth[0]), []byte("Bearer "+h.cfg.Token)) != 1 {
		return unauthorized
	}
	select {
	case h.slots <- struct{}{}:
		return nil
	default:
		return overloaded
	}
}

func (h *Handler) handle(w http.ResponseWriter, r *http.Request, start time.Time, e *Event) (any, error) {
	timeout := h.cfg.Timeout
	if values := r.Header.Values("Duro-Timeout-Ms"); len(values) != 0 {
		ms, err := strconv.ParseUint(values[0], 10, 64)
		if err != nil || ms == 0 || len(values) != 1 {
			return nil, invalid
		}
		if ms <= uint64(timeout.Milliseconds()) {
			timeout = time.Duration(ms) * time.Millisecond
		}
	}
	ctx, cancel := context.WithDeadline(r.Context(), start.Add(timeout))
	defer cancel()
	deadline, _ := ctx.Deadline()
	// A slow request body also consumes the client's budget.
	controller := http.NewResponseController(w)
	if err := controller.SetReadDeadline(deadline); err == nil {
		defer controller.SetReadDeadline(time.Time{})
	}
	limit := int64(maxRequest)
	if e.Operation == "registerType" {
		limit = maxRegistration
	}
	if r.ContentLength > limit {
		return nil, limitExceeded
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, limitExceeded
		}
		if !time.Now().Before(deadline) || ctx.Err() != nil {
			return nil, unavailable
		}
		return nil, invalid
	}
	if ctx.Err() != nil {
		return nil, unavailable
	}
	if !jsonvalue.Valid(&pb.JsonValue{Utf8: body}) {
		return nil, invalid
	}
	result, err := h.dispatch(ctx, e, body)
	if ctx.Err() != nil {
		return nil, unavailable
	}
	return result, err
}

type fields map[string]json.RawMessage

func parseFields(data []byte, names ...string) (fields, error) {
	var f fields
	if err := json.Unmarshal(data, &f); err != nil || len(f) != len(names) {
		return nil, invalid
	}
	for _, name := range names {
		if _, ok := f[name]; !ok {
			return nil, invalid
		}
	}
	return f, nil
}

func (f fields) str(key string) string {
	var value string
	_ = json.Unmarshal(f[key], &value)
	return value
}

func (h *Handler) dispatch(ctx context.Context, e *Event, body []byte) (any, error) {
	switch e.Operation {
	case "registerType":
		return h.register(ctx, e, body)
	case "create":
		f, err := parseFields(body, "object_id", "request_id", "type", "version", "input")
		if err != nil {
			return nil, err
		}
		op := &pb.CreateOperation{ObjectId: f.str("object_id"), RequestId: f.str("request_id"), Type: f.str("type"), Version: f.str("version"), Input: &pb.JsonValue{Utf8: f["input"]}}
		if op.ObjectId == "" || op.RequestId == "" || op.Type == "" || op.Version == "" {
			return nil, invalid
		}
		e.ObjectID, e.RequestID = op.ObjectId, op.RequestId
		return h.create(ctx, e, op)
	case "invoke":
		f, err := parseFields(body, "object_id", "request_id", "method", "argument")
		if err != nil {
			return nil, err
		}
		op := &pb.InvokeOperation{ObjectId: f.str("object_id"), RequestId: f.str("request_id"), Method: f.str("method"), Argument: &pb.JsonValue{Utf8: f["argument"]}}
		if op.ObjectId == "" || op.RequestId == "" || op.Method == "" {
			return nil, invalid
		}
		e.ObjectID, e.RequestID = op.ObjectId, op.RequestId
		return h.invoke(ctx, e, op)
	default:
		f, err := parseFields(body, "object_id")
		if err != nil {
			return nil, err
		}
		e.ObjectID = f.str("object_id")
		if e.ObjectID == "" {
			return nil, invalid
		}
		if e.Operation == "read" {
			return h.read(ctx, e.ObjectID)
		}
		return h.describe(ctx, e.ObjectID)
	}
}

func (h *Handler) findOwner(ctx context.Context, id string) (route, error) {
	reply, err := h.placement.GetOwner(ctx, &pb.GetOwnerRequest{ObjectId: id})
	if err != nil {
		return route{}, err
	}
	lease := reply.GetActive()
	if reply.GetUnassigned() != nil {
		assigner, ok := h.backend.(Assigner)
		if !ok {
			return route{}, unavailable
		}
		lease, err = assigner.Assign(ctx, id)
		if err != nil {
			return route{}, err
		}
	}
	return ownerRoute(lease.GetOwner(), id)
}
