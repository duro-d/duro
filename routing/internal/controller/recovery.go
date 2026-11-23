package controller

import (
	"bytes"
	"context"
	"sync"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// scan consumes shortened pages as well as full ones. Each pass starts afresh
// so inserts behind a cursor are eventually observed.
func (c *Controller) scan(ctx context.Context, req *pb.ListPlacementsRequest, visit func([]*pb.PlacementEntry)) error {
	req.PageSize = pageSize
	for ctx.Err() == nil {
		rpc, cancel := context.WithTimeout(ctx, rpcTimeout)
		p, err := c.store.ListPlacements(rpc, req)
		cancel()
		if err != nil {
			return err
		}
		if p == nil {
			return unavailable("empty placement page")
		}
		visit(p.Placements)
		if len(p.NextPageToken) == 0 {
			return nil
		}
		if bytes.Equal(req.PageToken, p.NextPageToken) {
			return unavailable("placement cursor did not advance")
		}
		req.PageToken = p.NextPageToken
	}
	return ctx.Err()
}

func parallel[T any](ctx context.Context, items []T, limit int, visit func(T)) {
	jobs := make(chan T)
	var wg sync.WaitGroup
	for i := 0; i < min(limit, len(items)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				visit(item)
			}
		}()
	}
	for _, item := range items {
		if ctx.Err() != nil {
			break
		}
		select {
		case jobs <- item:
		case <-ctx.Done():
		}
	}
	close(jobs)
	wg.Wait()
}

func workerFilter(id string) *pb.ListPlacementsRequest {
	return &pb.ListPlacementsRequest{Filter: &pb.ListPlacementsRequest_WorkerId{WorkerId: id}}
}

func (c *Controller) recoveryLoop(s *session) {
	previous := map[string]bool{}
	lost := map[string]bool{}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(s.ctx, rpcTimeout)
		ids, err := c.dir.Workers(ctx)
		cancel()
		if err == nil {
			live := map[string]bool{}
			for _, id := range ids {
				live[id] = true
				delete(lost, id)
			}
			for id := range previous {
				if !live[id] {
					lost[id] = true
				}
			}
			previous = live
			visit := func(expired bool) func([]*pb.PlacementEntry) {
				return func(page []*pb.PlacementEntry) {
					parallel(s.ctx, page, recoveryWorkers, func(entry *pb.PlacementEntry) {
						ctx, done := context.WithTimeout(s.ctx, 3*rpcTimeout)
						defer done()
						c.reconcile(ctx, s, entry, live, expired)
					})
				}
			}
			err = c.scan(s.ctx, &pb.ListPlacementsRequest{Filter: &pb.ListPlacementsRequest_Pending{Pending: &emptypb.Empty{}}}, visit(false))
			if err == nil {
				s.mu.Lock()
				s.readyForMoves = true
				req := s.planned
				s.mu.Unlock()
				if req != nil {
					task, done := context.WithTimeout(s.ctx, 3*rpcTimeout)
					_ = c.runPlan(task, s, req)
					done()
				}
			}
			for id := range lost {
				if c.scan(s.ctx, workerFilter(id), visit(false)) == nil {
					delete(lost, id)
				}
			}
			_ = c.scan(s.ctx, &pb.ListPlacementsRequest{Filter: &pb.ListPlacementsRequest_Expired{Expired: &emptypb.Empty{}}}, visit(true))
		}
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Controller) reconcile(ctx context.Context, s *session, e *pb.PlacementEntry, live map[string]bool, expired bool) {
	if r := e.GetReservation(); r != nil {
		o := r.GetLease().GetOwner()
		if !ownerValid(o, r.GetOperation().GetObjectId()) || r.GetOperation().GetRequestId() == "" || len(r.ReservationToken) == 0 {
			return
		}
		if expired || !live[o.WorkerId] {
			_, err := c.store.AbandonCreate(ctx, &pb.AbandonCreateRequest{ControllerTerm: s.term, Owner: o, RequestId: r.Operation.RequestId, ReservationToken: r.ReservationToken})
			s.check(err)
		}
		return
	}
	t := e.GetTransfer()
	s.mu.Lock()
	s.recovering++
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.recovering--; s.mu.Unlock() }()
	if t != nil {
		s.observePlan(t)
		if c.resume(ctx, s, t) == nil {
			return
		}
		if t.Phase != pb.TransferPhase_TRANSFER_PHASE_ACTIVATING || (!expired && live[t.TargetWorkerId]) {
			return
		}
	}
	o := e.GetAssigned().GetOwner()
	if t != nil {
		o = t.GetTarget().GetOwner()
	}
	if !ownerValid(o, o.GetObjectId()) {
		return
	}
	polls, err := c.snapshot(ctx)
	if err != nil {
		return
	}
	// An expired generation on the only surviving worker can recover on that
	// same incarnation, but it still requires a fresh epoch and activation.
	target, err := c.choose(o.ObjectId, polls, o.WorkerId)
	if err != nil && live[o.WorkerId] {
		target, err = c.choose(o.ObjectId, polls, "")
	}
	if err != nil {
		return
	}
	req, err := proposal(s, o.ObjectId, target, "recover", o)
	if err != nil {
		return
	}
	if t != nil {
		req.ReplacesTransitionId = &t.TransitionId
	}
	_ = c.begin(ctx, s, req)
}

func (c *Controller) renewLoop(s *session) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		ctx, done := context.WithTimeout(s.ctx, rpcTimeout)
		workers, err := c.dir.Workers(ctx)
		done()
		if err == nil {
			parallel(s.ctx, workers, recoveryWorkers, func(id string) {
				_ = c.scan(s.ctx, workerFilter(id), func(entries []*pb.PlacementEntry) {
					// The RPC contract caps ConfirmAssignments at 128 even if a
					// peer sends a larger page than requested.
					for len(entries) > 0 {
						n := min(len(entries), pageSize)
						c.renew(s, id, entries[:n])
						entries = entries[n:]
					}
				})
			})
		}
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Controller) renew(s *session, id string, entries []*pb.PlacementEntry) {
	leases := make(map[string]*pb.OwnerLease, len(entries))
	req := &pb.ConfirmAssignmentsRequest{WorkerId: id}
	for _, e := range entries {
		lease := e.GetAssigned()
		o := lease.GetOwner()
		if !ownerValid(o, o.GetObjectId()) || o.WorkerId != id || lease.GetLeaseUntil() == nil || lease.LeaseUntil.CheckValid() != nil || leases[o.ObjectId] != nil {
			continue
		}
		leases[o.ObjectId] = lease
		req.Assignments = append(req.Assignments, o)
	}
	if len(req.Assignments) == 0 {
		return
	}
	ctx, done := context.WithTimeout(s.ctx, rpcTimeout)
	defer done()
	w, err := c.dir.Worker(ctx, id)
	if err != nil {
		return
	}
	reply, err := w.ConfirmAssignments(ctx, req)
	if err != nil || reply.GetWorkerId() != id {
		return
	}
	write := &pb.RenewLeasesRequest{ControllerTerm: s.term}
	for _, o := range reply.Confirmed {
		l := leases[o.GetObjectId()]
		if l == nil || !proto.Equal(o, l.Owner) {
			continue
		}
		write.Renewals = append(write.Renewals, &pb.LeaseRenewal{Owner: l.Owner, ExpectedLeaseUntil: l.LeaseUntil})
		delete(leases, o.ObjectId)
	}
	if len(write.Renewals) > 0 {
		_, err = c.store.RenewLeases(ctx, write)
		s.check(err)
		// Future attempts always read the next assignment page and request a
		// new confirmation, including for entries storage rejected this time.
	}
}
