package controller

import (
	"context"
	"log/slog"
	"strings"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/placement"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Move starts a planned transfer and attempts to finish it within this call.
// The leader resumes a recorded intent even when the caller disconnects.
func (c *Controller) Move(ctx context.Context, objectID, target string) error {
	s, ctx, done, err := c.operation(ctx)
	if err != nil {
		return err
	}
	defer done()
	reply, err := c.store.GetOwner(ctx, &pb.GetOwnerRequest{ObjectId: objectID})
	if err != nil {
		return err
	}
	o := reply.GetActive().GetOwner()
	if !ownerValid(o, objectID) {
		return unavailable("owner is not active")
	}
	if o.WorkerId == target {
		return nil
	}
	polls, err := c.snapshot(ctx)
	if err != nil {
		return err
	}
	for _, p := range polls {
		w := p.window.Worker
		if w.ID == target && w.Ready && w.FreeCapacity > 0 {
			return c.plan(ctx, s, placement.Move{Source: assignment(o), Target: target}, "manual")
		}
	}
	return unavailable("target cannot accept assignments")
}

func assignment(o *pb.Owner) placement.Assignment {
	return placement.Assignment{ObjectID: o.GetObjectId(), WorkerID: o.GetWorkerId(), Epoch: o.GetEpoch()}
}

// Storage's v0 contract does not retain a completed transfer timestamp. A new
// leader therefore observes a cooldown for previously moved generations.
func (s *session) lastMoveLocked(a placement.Assignment) time.Time {
	if t := s.lastMoved[a.ObjectID].at; !t.IsZero() {
		return t
	}
	if a.Epoch > 1 && time.Since(s.started) < time.Minute {
		return s.started
	}
	return time.Time{}
}

func (c *Controller) plan(ctx context.Context, s *session, m placement.Move, reason string) error {
	o := &pb.Owner{ObjectId: m.Source.ObjectID, WorkerId: m.Source.WorkerID, Epoch: m.Source.Epoch}
	req, err := proposal(s, o.ObjectId, m.Target, "plan-"+reason, o)
	if err != nil {
		return err
	}
	s.mu.Lock()
	m.Source.LastMoved = s.lastMoveLocked(m.Source)
	limits := placement.Limits{InFlight: s.planned != nil, LastStart: s.lastStart}
	// A previous leader might have started a move just before this election.
	if limits.LastStart.IsZero() {
		limits.LastStart = s.started
	}
	now := time.Now()
	if !s.readyForMoves || s.recovering != 0 || !limits.Allows(m.Source, now) {
		s.mu.Unlock()
		return unavailable("planned transfer admission is busy or cooling down")
	}
	s.lastStart = now
	s.planned = req
	s.mu.Unlock()
	return c.runPlan(ctx, s, req)
}

func (c *Controller) runPlan(ctx context.Context, s *session, req *pb.BeginTransferRequest) error {
	r, err := c.store.BeginTransfer(ctx, req)
	if err != nil {
		// A definitive generation conflict means this proposal can no longer
		// start. For ambiguous transport failures retain and retry the same ID.
		if status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.NotFound {
			s.mu.Lock()
			if s.planned.GetTransitionId() == req.TransitionId {
				s.planned = nil
			}
			s.mu.Unlock()
		}
		return s.check(err)
	}
	return c.afterBegin(ctx, s, req, r.GetTransfer())
}

func (c *Controller) begin(ctx context.Context, s *session, req *pb.BeginTransferRequest) error {
	r, err := c.store.BeginTransfer(ctx, req)
	if err != nil {
		return s.check(err)
	}
	return c.afterBegin(ctx, s, req, r.GetTransfer())
}

func (c *Controller) afterBegin(ctx context.Context, s *session, req *pb.BeginTransferRequest, t *pb.PlacementTransfer) error {
	if t.GetTransitionId() != req.TransitionId || t.GetObjectId() != req.ObjectId || t.GetTargetWorkerId() != req.TargetWorkerId || !proto.Equal(t.GetSource().GetOwner(), req.Source) {
		return unavailable("storage returned a different transfer")
	}
	return c.resume(ctx, s, t)
}

func (c *Controller) resume(ctx context.Context, s *session, t *pb.PlacementTransfer) (result error) {
	started := time.Now()
	defer func() {
		level := slog.LevelInfo
		if result != nil {
			level = slog.LevelDebug
		}
		reason := t.GetTransitionId()
		if i := strings.LastIndex(reason, "-"); i >= 0 {
			reason = reason[:i]
		}
		slog.Log(ctx, level, "placement transfer attempt", "phase", t.GetPhase().String(), "object_id", t.GetObjectId(), "transition_id", t.GetTransitionId(), "source", t.GetSource().GetOwner().GetWorkerId(), "target", t.GetTargetWorkerId(), "epoch", t.GetTarget().GetOwner().GetEpoch(), "term", s.term, "reason", reason, "duration_ms", time.Since(started).Milliseconds(), "status", status.Code(result).String())
	}()
	if t.GetObjectId() == "" || t.GetTransitionId() == "" || t.GetTargetWorkerId() == "" {
		return unavailable("incomplete persisted transfer")
	}
	if t.Phase == pb.TransferPhase_TRANSFER_PHASE_DRAINING {
		source := t.GetSource().GetOwner()
		if !ownerValid(source, t.ObjectId) {
			return unavailable("missing drain source")
		}
		fence := &pb.FenceTransferRequest{ControllerTerm: s.term, TransitionId: t.TransitionId, Source: source, Authority: &pb.FenceTransferRequest_RequireExpiredLease{RequireExpiredLease: &emptypb.Empty{}}}
		// Only an acknowledgment for this generation and intent authorizes an
		// early fence. On any other outcome storage must verify its own clock.
		rpc, cancel := context.WithTimeout(ctx, rpcTimeout/2)
		w, err := c.dir.Worker(rpc, source.WorkerId)
		if err == nil {
			ack, e := w.DrainObject(rpc, &pb.DrainObjectRequest{Source: source, TransitionId: t.TransitionId})
			if e == nil && ack.GetPhase() == pb.DrainPhase_DRAIN_PHASE_DRAINED && ack.GetTransitionId() == t.TransitionId && proto.Equal(ack.GetSource(), source) {
				fence.Authority = &pb.FenceTransferRequest_DrainAcknowledged{DrainAcknowledged: &emptypb.Empty{}}
			}
		}
		cancel()
		r, err := c.store.FenceTransfer(ctx, fence)
		if err != nil {
			return s.check(err)
		}
		next := r.GetTransfer()
		if next.GetObjectId() != t.ObjectId || next.GetTransitionId() != t.TransitionId || next.GetTargetWorkerId() != t.TargetWorkerId || !proto.Equal(next.GetSource().GetOwner(), source) {
			return unavailable("storage returned a different fenced transfer")
		}
		t = next
	}
	if t.Phase == pb.TransferPhase_TRANSFER_PHASE_COMPLETE {
		c.finished(s, t)
		return nil
	}
	o := t.GetTarget().GetOwner()
	if t.Phase != pb.TransferPhase_TRANSFER_PHASE_ACTIVATING || !ownerValid(o, t.ObjectId) || o.WorkerId != t.TargetWorkerId || o.Epoch <= t.GetSource().GetOwner().GetEpoch() {
		return unavailable("missing fenced target grant")
	}
	w, err := c.dir.Worker(ctx, o.WorkerId)
	if err != nil {
		return err
	}
	ack, err := w.ActivateObject(ctx, &pb.ActivateObjectRequest{Owner: o, TransitionId: &t.TransitionId})
	if err != nil {
		return err
	}
	if !proto.Equal(ack.GetOwner(), o) || ack.GetTransitionId() != t.TransitionId {
		return unavailable("worker returned a different activation acknowledgment")
	}
	_, err = c.store.FinishTransfer(ctx, &pb.FinishTransferRequest{ControllerTerm: s.term, TransitionId: t.TransitionId, ActivatedOwner: o})
	if err != nil {
		return s.check(err)
	}
	c.finished(s, t)
	return nil
}

func (c *Controller) finished(s *session, t *pb.PlacementTransfer) {
	now := time.Now()
	s.mu.Lock()
	epoch := t.GetTarget().GetOwner().GetEpoch()
	if prev := s.lastMoved[t.ObjectId]; epoch > prev.epoch {
		s.lastMoved[t.ObjectId] = completedMove{at: now, epoch: epoch}
		c.moves.Add(1)
		if strings.HasPrefix(t.TransitionId, "recover-") {
			c.recoveries.Add(1)
		}
	}
	if s.planned.GetTransitionId() == t.TransitionId {
		s.planned = nil
	}
	s.mu.Unlock()

}

func (s *session) observePlan(t *pb.PlacementTransfer) {
	if strings.HasPrefix(t.GetTransitionId(), "recover-") || strings.HasPrefix(t.GetTransitionId(), "assign-") {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.planned == nil {
		s.planned = &pb.BeginTransferRequest{ControllerTerm: s.term, ObjectId: t.ObjectId, Source: t.GetSource().GetOwner(), TargetWorkerId: t.TargetWorkerId, TransitionId: t.TransitionId}
	}
}
