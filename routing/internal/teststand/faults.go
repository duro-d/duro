package teststand

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type faultKey struct{ kind, worker string }

// SetFault replaces the remaining occurrence count. Passing zero clears it.
func (w *World) SetFault(kind, worker string, count int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch kind {
	case "precommit", "postcommit", "stale", "overloaded", "execution-timeout":
	case "worker-down":
		if worker == "" {
			return fmt.Errorf("worker-down requires worker_id")
		}
	case "durability-unknown":
		if worker != "" {
			return fmt.Errorf("durability-unknown applies to the shared primary")
		}
	default:
		return fmt.Errorf("unknown fault kind")
	}
	if worker != "" && w.workers[worker] == nil {
		return fmt.Errorf("unknown worker_id")
	}
	if count < 0 || count > 100000 {
		return fmt.Errorf("count must be between 0 and 100000")
	}
	key := faultKey{kind, worker}
	if count == 0 {
		delete(w.faults, key)
	} else {
		w.faults[key] = count
	}
	return nil
}
func (w *World) take(kind, worker string) bool {
	k := faultKey{kind, worker}
	if w.faults[k] == 0 {
		k.worker = ""
	}
	n := w.faults[k]
	if n == 0 {
		return false
	}
	if n == 1 {
		delete(w.faults, k)
	} else {
		w.faults[k] = n - 1
	}
	return true
}
func (w *World) confirmed() error {
	if w.take("durability-unknown", "") {
		return unavailable("primary cannot confirm durability")
	}
	return nil
}

func (w *World) Admin() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /fault", func(rw http.ResponseWriter, r *http.Request) {
		body := http.MaxBytesReader(rw, r.Body, 2048)
		defer body.Close()
		d := json.NewDecoder(body)
		d.DisallowUnknownFields()
		var f struct {
			Kind   string `json:"kind"`
			Worker string `json:"worker_id"`
			Count  *int   `json:"count"`
		}
		if err := d.Decode(&f); err != nil || d.Decode(&struct{}{}) != io.EOF {
			http.Error(rw, "invalid fault request", http.StatusBadRequest)
			return
		}
		count := 1
		if f.Count != nil {
			count = *f.Count
		}
		if err := w.SetFault(f.Kind, f.Worker, count); err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		rw.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /owner", func(rw http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("object_id")
		if id == "" {
			http.Error(rw, "missing object_id", http.StatusBadRequest)
			return
		}
		result, err := w.GetOwner(r.Context(), &pb.GetOwnerRequest{ObjectId: id})
		if err != nil {
			code := http.StatusServiceUnavailable
			if status.Code(err) == codes.NotFound {
				code = http.StatusNotFound
			}
			http.Error(rw, status.Convert(err).Message(), code)
			return
		}
		owner := result.GetActive().GetOwner()
		if owner == nil {
			http.Error(rw, "object awaits assignment", http.StatusServiceUnavailable)
			return
		}
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(struct {
			Worker string `json:"worker_id"`
			Epoch  uint64 `json:"epoch"`
		}{owner.WorkerId, owner.Epoch})
	})
	return mux
}
