package teststand

import (
	"bytes"
	"context"
	"slices"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/protobuf/proto"
)

type measured struct {
	owner *pb.Owner
	calls uint64
	busy  time.Duration
}
type Worker struct {
	pb.UnimplementedWorkerServiceServer
	world    *World
	id       string
	start    time.Time
	calls    map[string]*measured
	sample   *pb.WorkerLoadSample
	sampleAt time.Time
}

func (w *World) Worker(id string) *Worker {
	w.mu.Lock()
	defer w.mu.Unlock()
	if current := w.workers[id]; current != nil {
		return current
	}
	server := &Worker{world: w, id: id, start: w.now(), calls: make(map[string]*measured)}
	w.workers[id] = server
	return server
}
func (s *Worker) ready(id string) error {
	if id != s.id {
		return stale("wrong worker incarnation")
	}
	if s.world.take("worker-down", s.id) {
		return unavailable("worker down")
	}
	return nil
}
func (s *Worker) owner(id *pb.Owner) (*object, error) {
	if err := s.ready(id.GetWorkerId()); err != nil {
		return nil, err
	}
	o := s.world.objects[id.GetObjectId()]
	if o == nil || !s.world.live(o.lease) || !proto.Equal(o.lease.Owner, id) {
		return nil, stale("owner or lease changed")
	}
	return o, nil
}
func (s *Worker) ValidateCode(_ context.Context, r *pb.ValidateCodeRequest) (*pb.ValidateCodeResponse, error) {
	s.world.mu.Lock()
	defer s.world.mu.Unlock()
	if err := s.ready(s.id); err != nil {
		return nil, err
	}
	if err := checkFixture(r.CodePackage); err != nil {
		return nil, err
	}
	return &pb.ValidateCodeResponse{Validated: r.CodePackage.Manifest.Code}, nil
}
func (s *Worker) ActivateObject(_ context.Context, r *pb.ActivateObjectRequest) (*pb.ActivateObjectResponse, error) {
	w := s.world
	w.mu.Lock()
	defer w.mu.Unlock()
	o, err := s.owner(r.Owner)
	if err != nil {
		return nil, err
	}
	if err = w.confirmed(); err != nil {
		return nil, err
	}
	if o.drained || (o.pending() && o.transfer.Phase == pb.TransferPhase_TRANSFER_PHASE_DRAINING) {
		return nil, stale("admission closed")
	}
	if r.TransitionId != nil || o.pending() {
		if r.GetTransitionId() == "" || o.transfer == nil || o.transfer.TransitionId != r.GetTransitionId() || !proto.Equal(o.transfer.Target.GetOwner(), r.Owner) {
			return nil, stale("activation does not match transfer")
		}
	}
	o.activated = true
	return &pb.ActivateObjectResponse{Owner: r.Owner, TransitionId: r.TransitionId}, nil
}
func (s *Worker) DrainObject(_ context.Context, r *pb.DrainObjectRequest) (*pb.DrainObjectResponse, error) {
	w := s.world
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := s.ready(r.Source.GetWorkerId()); err != nil {
		return nil, err
	}
	o := w.objects[r.Source.GetObjectId()]
	if o == nil || o.transfer == nil || o.transfer.TransitionId != r.TransitionId || !proto.Equal(o.transfer.Source.GetOwner(), r.Source) {
		return nil, stale("source or transition changed")
	}
	if err := w.confirmed(); err != nil {
		return nil, err
	}
	// Every admitted mutation has left its critical section before this point.
	if o.transfer.Phase == pb.TransferPhase_TRANSFER_PHASE_DRAINING {
		o.drained = true
	}
	return &pb.DrainObjectResponse{Source: r.Source, TransitionId: r.TransitionId, Phase: pb.DrainPhase_DRAIN_PHASE_DRAINED}, nil
}
func (s *Worker) admission(o *object) error {
	if s.world.take("stale", s.id) || !o.activated || o.drained || (o.pending() && o.transfer.Phase == pb.TransferPhase_TRANSFER_PHASE_ACTIVATING) {
		return stale("worker rejects admission for this generation")
	}
	return s.world.confirmed()
}
func (s *Worker) beforeCommit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.world.take("precommit", s.id) {
		return unavailable("injected failure before commit")
	}
	return nil
}
func (s *Worker) afterCommit() error {
	if s.world.take("postcommit", s.id) {
		return unavailable("injected lost commit reply")
	}
	return nil
}
func (s *Worker) CreateObject(ctx context.Context, r *pb.CreateObjectRequest) (*pb.CreateObjectResponse, error) {
	w := s.world
	w.mu.Lock()
	defer w.mu.Unlock()
	op := operation{create: r.Operation}
	if !op.valid() || r.Owner.GetObjectId() != r.Operation.ObjectId {
		return nil, invalid("invalid create")
	}
	o, err := s.owner(r.Owner)
	if err != nil {
		return nil, err
	}
	if err = s.admission(o); err != nil {
		return nil, err
	}
	token, exists := w.tokens[string(r.ReservationToken)]
	if !exists || token.abandoned || !proto.Equal(token.reservation.Lease.Owner, r.Owner) || !op.same(operation{create: token.reservation.Operation}) {
		return nil, stale("reservation token or operation changed")
	}
	res, err := o.lookup(op)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return &pb.CreateObjectResponse{Owner: r.Owner, Outcome: res}, nil
	}
	if o.reservation == nil || !bytes.Equal(r.ReservationToken, o.reservation.ReservationToken) {
		return nil, stale("reservation ended")
	}
	start := time.Now()
	defer s.measure(r.Owner, start)
	kv, result := initFixture(r.Operation)
	if err = s.beforeCommit(ctx); err != nil {
		return nil, err
	}
	if !w.live(o.lease) {
		return nil, stale("lease expired before init commit")
	}
	if result.GetResult() != nil {
		o.code = w.packages[packageKey{r.Operation.Type, r.Operation.Version}]
		o.kv = kv
	} else {
		o.lease = nil
		o.activated = false
	}
	o.receipts[r.Operation.RequestId] = receipt{op, result}
	o.reservation = nil
	if err = s.afterCommit(); err != nil {
		return nil, err
	}
	return &pb.CreateObjectResponse{Owner: r.Owner, Outcome: result}, nil
}
func (s *Worker) InvokeObject(ctx context.Context, r *pb.InvokeObjectRequest) (*pb.InvokeObjectResponse, error) {
	w := s.world
	w.mu.Lock()
	defer w.mu.Unlock()
	op := operation{invoke: r.Operation}
	if !op.valid() || r.Owner.GetObjectId() != r.Operation.ObjectId {
		return nil, invalid("invalid invoke")
	}
	o, err := s.owner(r.Owner)
	if err != nil {
		return nil, err
	}
	if err = s.admission(o); err != nil {
		return nil, err
	}
	if o.code == nil {
		return nil, missing("object is absent")
	}
	res, err := o.lookup(op)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return &pb.InvokeObjectResponse{Owner: r.Owner, Outcome: res}, nil
	}
	start := time.Now()
	defer s.measure(r.Owner, start)
	if w.take("overloaded", s.id) {
		return nil, duroError(pb.ErrorCode_ERROR_CODE_OVERLOADED, "fixture admission full")
	}
	if w.take("execution-timeout", s.id) {
		return nil, duroError(pb.ErrorCode_ERROR_CODE_EXECUTION_TIMEOUT, "fixture handler timed out")
	}
	kv, result, err := runFixture(o, r.Operation)
	if err != nil {
		return nil, err
	}
	if len(result.GetResult().GetUtf8()) > 64<<10 {
		return nil, duroError(pb.ErrorCode_ERROR_CODE_LIMIT_EXCEEDED, "fixture result exceeds 64 KiB")
	}
	if err = s.beforeCommit(ctx); err != nil {
		return nil, err
	}
	if !w.live(o.lease) {
		return nil, stale("lease expired before commit")
	}
	if kv != nil {
		o.kv = kv
	}
	o.receipts[r.Operation.RequestId] = receipt{op, result}
	if err = s.afterCommit(); err != nil {
		return nil, err
	}
	return &pb.InvokeObjectResponse{Owner: r.Owner, Outcome: result}, nil
}

func (s *Worker) measure(owner *pb.Owner, start time.Time) {
	s.rollWindow()
	m := s.calls[owner.ObjectId]
	if m == nil || !proto.Equal(m.owner, owner) {
		m = &measured{owner: owner}
		s.calls[owner.ObjectId] = m
	}
	m.calls++
	m.busy += time.Since(start)
}
func (s *Worker) rollWindow() {
	now := s.world.now()
	elapsed := now.Sub(s.start)
	if elapsed < time.Second {
		return
	}
	sample := &pb.WorkerLoadSample{WindowMs: uint32(min(elapsed.Milliseconds(), int64(^uint32(0)))), QueueLimit: 1000}
	var total time.Duration
	for _, m := range s.calls {
		total += m.busy
		sample.HotObjects = append(sample.HotObjects, &pb.HotObject{Owner: m.owner, CallCount: m.calls, BusyFraction: min(1, float64(m.busy)/float64(elapsed))})
	}
	slices.SortFunc(sample.HotObjects, func(a, b *pb.HotObject) int {
		if a.CallCount > b.CallCount {
			return -1
		}
		if a.CallCount < b.CallCount {
			return 1
		}
		if a.Owner.ObjectId < b.Owner.ObjectId {
			return -1
		}
		if a.Owner.ObjectId > b.Owner.ObjectId {
			return 1
		}
		return 0
	})
	if len(sample.HotObjects) > 16 {
		sample.HotObjects = sample.HotObjects[:16]
	}
	sample.BusyFraction = min(1, float64(total)/float64(elapsed))
	s.sample = sample
	s.sampleAt = now
	s.start = now
	clear(s.calls)
}
func (s *Worker) PollWorker(_ context.Context, r *pb.PollWorkerRequest) (*pb.PollWorkerResponse, error) {
	w := s.world
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := s.ready(r.WorkerId); err != nil {
		return nil, err
	}
	s.rollWindow()
	sample := proto.CloneOf(s.sample)
	if sample != nil {
		sample.AgeMs = uint64(max(0, w.now().Sub(s.sampleAt).Milliseconds()))
	}
	return &pb.PollWorkerResponse{WorkerId: s.id, Ready: true, HasCapacityForNewAssignments: true, Load: sample}, nil
}
func (s *Worker) ConfirmAssignments(_ context.Context, r *pb.ConfirmAssignmentsRequest) (*pb.ConfirmAssignmentsResponse, error) {
	w := s.world
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := s.ready(r.WorkerId); err != nil {
		return nil, err
	}
	if len(r.Assignments) == 0 || len(r.Assignments) > 128 {
		return nil, invalid("confirm requires 1 to 128 assignments")
	}
	resp := &pb.ConfirmAssignmentsResponse{WorkerId: s.id}
	seen := make(map[string]bool)
	for _, id := range r.Assignments {
		if id.GetWorkerId() != s.id || id.GetObjectId() == "" || id.Epoch == 0 || seen[id.ObjectId] {
			return nil, invalid("invalid or duplicate assignment")
		}
		seen[id.ObjectId] = true
		o := w.objects[id.ObjectId]
		if o != nil && o.code != nil && o.activated && !o.pending() && w.live(o.lease) && proto.Equal(o.lease.Owner, id) {
			resp.Confirmed = append(resp.Confirmed, id)
		}
	}
	return resp, nil
}
