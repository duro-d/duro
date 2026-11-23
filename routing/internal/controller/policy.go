package controller

import (
	"context"
	"slices"
	"sort"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/placement"
)

type WorkerMetric struct {
	ID           string
	Queue        int
	BusyFraction float64
}

type workerReading struct {
	metric WorkerMetric
	at     time.Time
}

// Metrics reads the latest valid polls without performing discovery or taking
// the poll lock. The sample age includes the worker's reported age and RPC RTT.
func (c *Controller) Metrics() []WorkerMetric {
	readings := c.readings.Load()
	if readings == nil {
		return nil
	}
	now := time.Now()
	out := make([]WorkerMetric, 0, len(*readings))
	for _, r := range *readings {
		if age := now.Sub(r.at); age >= 0 && age <= 3*time.Second {
			out = append(out, r.metric)
		}
	}
	return out
}

type observation struct {
	window placement.LoadWindow
	hot    []placement.HotObject
}

func (c *Controller) snapshot(ctx context.Context) ([]observation, error) {
	// Serialize membership reads with cache updates. A delayed older snapshot
	// must not resurrect a worker incarnation removed by a newer snapshot.
	select {
	case c.pollGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.pollGate }()
	ids, err := c.dir.Workers(ctx)
	if err != nil {
		c.readings.Store(nil)
		c.polled = time.Time{}
		return nil, err
	}
	ids = slices.Clone(ids)
	sort.Strings(ids)
	ids = slices.Compact(ids)
	same := len(ids) == len(c.polls)
	for _, id := range ids {
		if _, ok := c.polls[id]; !ok {
			same = false
			break
		}
	}
	pollStarted := time.Now()
	due := c.polled.IsZero() || pollStarted.Sub(c.polled) >= 950*time.Millisecond
	if !same || due {
		next := make([]observation, len(ids))
		indexes := make([]int, len(ids))
		for i := range indexes {
			indexes[i] = i
		}
		parallel(ctx, indexes, 8, func(i int) {
			id := ids[i]
			prior, known := c.polls[id]
			if known && !due {
				next[i] = prior
				return
			}
			next[i] = c.poll(ctx, id, prior.window)
		})
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.polls = make(map[string]observation, len(next))
		readings := make([]workerReading, 0, len(next))
		for i, id := range ids {
			c.polls[id] = next[i]
			w := next[i].window.Worker
			sample := w.Sample
			if !sample.At.IsZero() && sample.QueueLimit > 0 && sample.Queue >= 0 && sample.BusyFraction >= 0 && sample.BusyFraction <= 1 {
				readings = append(readings, workerReading{metric: WorkerMetric{ID: id, Queue: sample.Queue, BusyFraction: sample.BusyFraction}, at: sample.At})
			}
		}
		c.readings.Store(&readings)
		if due {
			c.polled = pollStarted
		}
	}
	out := make([]observation, 0, len(ids))
	for _, id := range ids {
		out = append(out, c.polls[id])
	}
	return out, nil
}

func (c *Controller) poll(ctx context.Context, id string, previous placement.LoadWindow) observation {
	rpc, done := context.WithTimeout(ctx, rpcTimeout/2)
	defer done()
	p := observation{window: placement.LoadWindow{Worker: placement.Candidate{ID: id, Mode: placement.Active}, Previous: [2]placement.Sample{previous.Previous[1], previous.Worker.Sample}}}
	w, err := c.dir.Worker(rpc, id)
	if err != nil {
		return p
	}
	started := time.Now()
	r, err := w.PollWorker(rpc, &pb.PollWorkerRequest{WorkerId: id})
	if err != nil || r.GetWorkerId() != id {
		return p
	}
	node := &p.window.Worker
	node.Ready = r.Ready
	if r.HasCapacityForNewAssignments {
		node.FreeCapacity = 1
	}
	sample := r.Load
	if sample == nil || sample.WindowMs == 0 || sample.AgeMs > 3000 {
		return p
	}
	// Subtracting reported age from request start includes the entire poll RTT.
	node.Sample = placement.Sample{At: started.Add(-time.Duration(sample.AgeMs) * time.Millisecond), Queue: int(sample.QueueDepth), QueueLimit: int(sample.QueueLimit), BusyFraction: sample.BusyFraction}
	if len(sample.HotObjects) <= 16 {
		for _, h := range sample.HotObjects {
			if h.GetOwner().GetWorkerId() == id && ownerValid(h.Owner, h.Owner.GetObjectId()) {
				p.hot = append(p.hot, placement.HotObject{Assignment: assignment(h.Owner), CallCount: h.CallCount, BusyFraction: h.BusyFraction})
			}
		}
	}
	return p
}

type joinScan struct {
	target  string
	sources []string
	cursor  []byte
}

func (c *Controller) policyLoop(s *session) {
	previous := map[string]bool(nil)
	var joins []joinScan
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		ctx, done := context.WithTimeout(s.ctx, 3*rpcTimeout)
		polls, err := c.snapshot(ctx)
		if err == nil {
			live := make(map[string]bool, len(polls))
			ids := make([]string, 0, len(polls))
			for _, p := range polls {
				id := p.window.Worker.ID
				ids = append(ids, id)
				live[id] = true
			}
			if c.policy == placement.HRW && previous != nil {
				for _, id := range ids {
					if !previous[id] {
						joins = append(joins, joinScan{target: id, sources: slices.Clone(ids)})
					}
				}
			}
			previous = live
			now := time.Now()
			s.mu.Lock()
			for id, last := range s.lastMoved {
				if now.Sub(last.at) >= time.Minute {
					delete(s.lastMoved, id)
				}
			}
			limits := placement.Limits{InFlight: !s.readyForMoves || s.planned != nil || s.recovering != 0, LastStart: s.lastStart}
			if limits.LastStart.IsZero() {
				limits.LastStart = s.started
			}
			s.mu.Unlock()
			if !limits.InFlight {
				if c.policy == placement.Load {
					c.planLoad(ctx, s, polls, limits, now)
				}
				if len(joins) > 0 {
					job := joins[0]
					joins = joins[1:]
					if live[job.target] && !c.planJoin(ctx, s, &job, polls, limits, now) {
						joins = append(joins, job)
					}
				}
			}
		}
		done()
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Controller) planLoad(ctx context.Context, s *session, polls []observation, limits placement.Limits, now time.Time) {
	targets := make([]placement.LoadWindow, 0, len(polls))
	for _, p := range polls {
		targets = append(targets, p.window)
	}
	for _, p := range polls {
		hot := slices.Clone(p.hot)
		s.mu.Lock()
		for i := range hot {
			hot[i].LastMoved = s.lastMoveLocked(hot[i].Assignment)
		}
		s.mu.Unlock()
		m, ok := placement.PlanLoad(p.window, targets, hot, limits, now)
		if ok {
			_ = c.plan(ctx, s, m, "load")
			return
		}
	}
}

func (c *Controller) planJoin(ctx context.Context, s *session, j *joinScan, polls []observation, limits placement.Limits, now time.Time) bool {
	for len(j.sources) > 0 && j.sources[0] == j.target {
		j.sources = j.sources[1:]
	}
	if len(j.sources) == 0 {
		return true
	}
	if now.Sub(limits.LastStart) < time.Second {
		return false
	}
	candidates := make([]placement.Candidate, 0, len(polls))
	targetReady := false
	for _, p := range polls {
		w := p.window.Worker
		candidates = append(candidates, w)
		if w.ID == j.target {
			targetReady = w.Ready && w.FreeCapacity > 0
		}
	}
	if !targetReady {
		return false
	}
	req := workerFilter(j.sources[0])
	req.PageToken = j.cursor
	req.PageSize = pageSize
	r, err := c.store.ListPlacements(ctx, req)
	if err != nil || r == nil {
		return false
	}
	var owners []placement.Assignment
	s.mu.Lock()
	for _, e := range r.Placements {
		o := e.GetAssigned().GetOwner()
		if o.GetWorkerId() == j.sources[0] {
			a := assignment(o)
			a.LastMoved = s.lastMoveLocked(a)
			owners = append(owners, a)
		}
	}
	s.mu.Unlock()
	if m, ok := placement.PlanJoin(j.target, candidates, owners, limits, now); ok {
		_ = c.plan(ctx, s, m, "hrw")
		return false // Revisit this page for the remaining assignments.
	}
	j.cursor = r.NextPageToken
	if len(j.cursor) == 0 {
		j.sources = j.sources[1:]
	}
	return len(j.sources) == 0
}
