package gateway

import (
	"container/list"
	"context"
	"strings"
	"sync"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
)

type route struct {
	objectID string
	workerID string
	epoch    uint64
}

func ownerRoute(owner *pb.Owner, objectID string) (route, error) {
	if owner.GetObjectId() != objectID || owner.GetWorkerId() == "" || owner.GetEpoch() == 0 {
		return route{}, unavailable
	}
	return route{objectID, owner.WorkerId, owner.Epoch}, nil
}

func (r route) owner() *pb.Owner {
	return &pb.Owner{ObjectId: r.objectID, WorkerId: r.workerID, Epoch: r.epoch}
}

// The allowance covers the list element, map slot and entry in addition to both strings.
func (r route) size() int { return 256 + len(r.objectID) + len(r.workerID) }

type cacheEntry struct {
	route
	expires time.Time
}

type flight struct {
	done          chan struct{}
	cancel        context.CancelFunc
	waiters       int
	interrupted   bool
	rejectedEpoch uint64
	route         route
	err           error
}

type resolver struct {
	now                 func() time.Time
	mu                  sync.Mutex
	entries             map[string]*list.Element
	lru                 list.List
	bytes               int
	hits, misses, stale uint64
	pending             map[string]*flight
	config              Config
	find                func(context.Context, string) (route, error)
}

func newResolver(cfg Config, find func(context.Context, string) (route, error)) *resolver {
	return &resolver{now: time.Now, entries: make(map[string]*list.Element), pending: make(map[string]*flight), config: cfg, find: find}
}

func (s *resolver) resolve(ctx context.Context, id string) (route, error) {
	if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > s.config.Timeout {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
	}
	deadline, _ := ctx.Deadline()
	for ctx.Err() == nil {
		s.mu.Lock()
		if elem := s.entries[id]; elem != nil {
			entry := elem.Value.(cacheEntry)
			if s.now().Before(entry.expires) {
				s.hits++
				s.lru.MoveToFront(elem)
				s.mu.Unlock()
				return entry.route, nil
			}
			s.remove(elem)
		}
		s.misses++
		call := s.pending[id]
		if call == nil {
			if len(s.pending) >= s.config.MaxLookups {
				s.mu.Unlock()
				return route{}, overloaded
			}
			// The RPC inherits the starting waiter's deadline. A later waiter with
			// more time can retry when this RPC expires.
			rpc, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
			call = &flight{done: make(chan struct{}), cancel: cancel}
			s.pending[id] = call
			go s.lookup(rpc, id, call)
		}
		call.waiters++
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			s.mu.Lock()
			call.waiters--
			if call.waiters == 0 {
				call.cancel()
			}
			s.mu.Unlock()
			return route{}, ctx.Err()
		case <-call.done:
			if call.interrupted && time.Now().Before(deadline) {
				continue
			}
			return call.route, call.err
		}
	}
	return route{}, ctx.Err()
}

func (s *resolver) lookup(ctx context.Context, id string, f *flight) {
	defer f.cancel()
	r, err := s.find(ctx, id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		f.interrupted = true
		err = ctx.Err()
	} else if err == nil {
		switch {
		case r.epoch <= f.rejectedEpoch:
			err = unavailable
		default:
			s.insert(r)
		}
	}
	f.route, f.err = r, err
	delete(s.pending, id)
	close(f.done)
}

func (s *resolver) remove(elem *list.Element) {
	entry := elem.Value.(cacheEntry)
	delete(s.entries, entry.objectID)
	s.bytes -= entry.size()
	s.lru.Remove(elem)
}

// Called under the mutex. Only the single outstanding lookup can populate an ID.
func (s *resolver) insert(r route) {
	size := r.size()
	if size > s.config.CacheBytes {
		return
	}
	for s.lru.Len() >= s.config.CacheEntries || size > s.config.CacheBytes-s.bytes {
		s.remove(s.lru.Back())
	}
	r.objectID = strings.Clone(r.objectID)
	r.workerID = strings.Clone(r.workerID)
	entry := cacheEntry{r, s.now().Add(s.config.CacheTTL)}
	s.entries[r.objectID] = s.lru.PushFront(entry)
	s.bytes += size
}

func (s *resolver) invalidate(r route, stale bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stale {
		s.stale++
	}
	if elem := s.entries[r.objectID]; elem != nil && elem.Value.(cacheEntry).route == r {
		s.remove(elem)
	}
	// A miss after TTL expiry may still be reading the rejected generation.
	if call := s.pending[r.objectID]; call != nil && stale {
		call.rejectedEpoch = max(call.rejectedEpoch, r.epoch)
	}
}

type CacheStats struct {
	Hits, Misses, Stale uint64
	Entries, Bytes      int
}

func (h *Handler) Stats() CacheStats {
	s := h.routes
	s.mu.Lock()
	defer s.mu.Unlock()
	return CacheStats{Hits: s.hits, Misses: s.misses, Stale: s.stale, Entries: s.lru.Len(), Bytes: s.bytes}
}
