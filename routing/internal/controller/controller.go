// Package controller coordinates placement using the storage service's election
// term, generation comparisons, and durable transfer records.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/jsonvalue"
	"github.com/duro-d/duro/routing/internal/placement"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	pageSize        = 128
	rpcTimeout      = 2 * time.Second
	recoveryWorkers = 4
)

type Directory interface {
	Workers(context.Context) ([]string, error)
	Worker(context.Context, string) (pb.WorkerServiceClient, error)
}

type Controller struct {
	store          pb.PlacementServiceClient
	dir            Directory
	policy         placement.Policy
	mu             sync.Mutex
	lastTerm       uint64
	cancelCampaign context.CancelFunc
	leader         *session

	pollGate chan struct{}
	polls    map[string]observation
	polled   time.Time

	fallbacks  atomic.Uint64
	moves      atomic.Uint64
	recoveries atomic.Uint64
	readings   atomic.Pointer[[]workerReading]
}

type completedMove struct {
	at    time.Time
	epoch uint64
}

type session struct {
	ctx     context.Context
	cancel  context.CancelFunc
	term    uint64
	started time.Time

	mu            sync.Mutex
	readyForMoves bool
	planned       *pb.BeginTransferRequest
	lastStart     time.Time
	lastMoved     map[string]completedMove
	recovering    int
}

func New(store pb.PlacementServiceClient, dir Directory, policy placement.Policy) *Controller {
	return &Controller{store: store, dir: dir, policy: policy, pollGate: make(chan struct{}, 1)}
}

func (c *Controller) IsLeader() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader != nil && c.leader.ctx.Err() == nil
}

// Counts returns the HRW fallbacks and acknowledged transfer completions seen by this process.
func (c *Controller) Counts() (fallbacks, moves uint64) {
	return c.fallbacks.Load(), c.moves.Load()
}

// Recoveries counts completed transfers initiated by the recovery sweep.
func (c *Controller) Recoveries() uint64 { return c.recoveries.Load() }

func unavailable(msg string) error { return status.Error(codes.Unavailable, msg) }

func (c *Controller) operation(ctx context.Context) (*session, context.Context, func(), error) {
	c.mu.Lock()
	s := c.leader
	c.mu.Unlock()
	if s == nil || s.ctx.Err() != nil {
		return nil, nil, nil, unavailable("placement controller has no active leadership")
	}
	rpc, cancel := context.WithTimeout(ctx, 4*rpcTimeout)
	stop := context.AfterFunc(s.ctx, cancel)
	if s.ctx.Err() != nil {
		cancel()
	}
	return s, rpc, func() { stop(); cancel() }, nil
}

// ServeLeader registers the election revision before it admits writes. The
// caller cancels ctx on session loss, including when the watch is interrupted.
func (c *Controller) ServeLeader(ctx context.Context, term uint64) error {
	if term == 0 {
		return status.Error(codes.InvalidArgument, "zero election term")
	}
	c.mu.Lock()
	if term <= c.lastTerm {
		c.mu.Unlock()
		return status.Error(codes.FailedPrecondition, "superseded election term")
	}
	c.lastTerm = term
	if c.cancelCampaign != nil {
		c.cancelCampaign()
	}
	ctx, cancel := context.WithCancel(ctx)
	c.cancelCampaign = cancel
	if c.leader != nil {
		c.leader.cancel()
		c.leader = nil
	}
	c.mu.Unlock()

	defer cancel()
	s := &session{ctx: ctx, cancel: cancel, term: term, lastMoved: make(map[string]completedMove)}
	for {
		rpc, done := context.WithTimeout(ctx, rpcTimeout)
		_, err := c.store.RegisterControllerTerm(rpc, &pb.RegisterControllerTermRequest{ControllerTerm: term})
		done()
		if err == nil {
			break
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	c.mu.Lock()
	if c.lastTerm != term || ctx.Err() != nil {
		c.mu.Unlock()
		return unavailable("election session ended")
	}
	s.started = time.Now()
	c.leader = s
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.leader == s {
			c.leader = nil
		}
		c.mu.Unlock()
	}()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); c.recoveryLoop(s) }()
	go func() { defer wg.Done(); c.renewLoop(s) }()
	go func() { defer wg.Done(); c.policyLoop(s) }()
	wg.Wait()
	return ctx.Err()
}

func (s *session) check(err error) error {
	if err != nil {
		for _, d := range status.Convert(err).Details() {
			if detail, ok := d.(*pb.ErrorDetail); ok && detail.Code == pb.ErrorCode_ERROR_CODE_STALE_CONTROLLER {
				s.cancel()
				break
			}
		}
	}
	return err
}

func token() ([]byte, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	return b, err
}

func proposal(s *session, objectID, target, kind string, source *pb.Owner) (*pb.BeginTransferRequest, error) {
	id, err := token()
	if err != nil {
		return nil, err
	}
	return &pb.BeginTransferRequest{ControllerTerm: s.term, ObjectId: objectID, TargetWorkerId: target, Source: source, TransitionId: kind + "-" + hex.EncodeToString(id)}, nil
}

func (c *Controller) Create(ctx context.Context, op *pb.CreateOperation) (*pb.ReserveCreateResponse, error) {
	if op.GetObjectId() == "" || op.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "create requires object and request IDs")
	}
	s, ctx, done, err := c.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	seen, err := c.store.LookupOperation(ctx, &pb.LookupOperationRequest{Operation: &pb.LookupOperationRequest_Create{Create: op}})
	if err != nil {
		return nil, err
	}
	if receipt := seen.GetCompleted(); receipt != nil {
		return &pb.ReserveCreateResponse{Outcome: &pb.ReserveCreateResponse_Completed{Completed: receipt}}, nil
	}
	if seen.GetAbsent() == nil {
		return nil, unavailable("missing operation observation")
	}
	id, err := c.Candidate(ctx, op.ObjectId)
	if err != nil {
		return nil, err
	}
	b, err := token()
	if err != nil {
		return nil, err
	}
	req := &pb.ReserveCreateRequest{ControllerTerm: s.term, Operation: op, TargetWorkerId: id, ReservationToken: b}
	var reply *pb.ReserveCreateResponse
	// A lost first reply may have left a reservation. Retry its exact proposal.
	for i := 0; i < 2; i++ {
		rpc, cancel := context.WithTimeout(ctx, rpcTimeout)
		reply, err = c.store.ReserveCreate(rpc, req)
		cancel()
		if err == nil || ctx.Err() != nil || (status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded) {
			break
		}
	}
	if err != nil {
		return nil, s.check(err)
	}
	if reply.GetCompleted() != nil {
		return reply, nil
	}
	r := reply.GetReservation()
	pinned := r.GetOperation()
	if pinned.GetObjectId() != op.ObjectId || pinned.GetRequestId() != op.RequestId || pinned.GetType() != op.Type || pinned.GetVersion() != op.Version || !jsonvalue.Equal(pinned.GetInput(), op.Input) || len(r.GetReservationToken()) == 0 || !ownerValid(r.GetLease().GetOwner(), op.ObjectId) {
		return nil, unavailable("invalid create reservation")
	}
	return reply, nil
}

func ownerValid(o *pb.Owner, object string) bool {
	return o.GetObjectId() == object && object != "" && o.GetWorkerId() != "" && o.GetEpoch() > 0
}

func (c *Controller) Assign(ctx context.Context, objectID string) (*pb.OwnerLease, error) {
	s, ctx, done, err := c.operation(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	reply, err := c.store.GetOwner(ctx, &pb.GetOwnerRequest{ObjectId: objectID})
	if err != nil {
		return nil, err
	}
	if live := reply.GetActive(); live != nil {
		if !ownerValid(live.GetOwner(), objectID) {
			return nil, unavailable("storage returned a different active owner")
		}
		return live, nil
	}
	if reply.GetUnassigned() == nil {
		return nil, unavailable("storage returned no assignment status")
	}
	id, err := c.Candidate(ctx, objectID)
	if err != nil {
		return nil, err
	}
	req, err := proposal(s, objectID, id, "assign", nil)
	if err != nil {
		return nil, err
	}
	err = c.begin(ctx, s, req)
	// A concurrent assignment or a lost FinishTransfer reply may have won.
	reply, readErr := c.store.GetOwner(ctx, &pb.GetOwnerRequest{ObjectId: objectID})
	if readErr == nil && ownerValid(reply.GetActive().GetOwner(), objectID) {
		return reply.GetActive(), nil
	}
	if err != nil {
		return nil, err
	}
	if readErr != nil {
		return nil, readErr
	}
	return nil, unavailable("assignment is pending")
}

func (c *Controller) Candidate(ctx context.Context, objectID string) (string, error) {
	polls, err := c.snapshot(ctx)
	if err != nil {
		return "", err
	}
	return c.choose(objectID, polls, "")
}

func (c *Controller) choose(objectID string, polls []observation, exclude string) (string, error) {
	candidates := make([]placement.Candidate, 0, len(polls))
	for _, p := range polls {
		if p.window.Worker.ID != exclude {
			candidates = append(candidates, p.window.Worker)
		}
	}
	d, err := placement.Select(c.policy, objectID, candidates, time.Now())
	if err != nil {
		if errors.Is(err, placement.ErrInvalidObjectID) {
			return "", status.Error(codes.InvalidArgument, err.Error())
		}
		return "", unavailable(err.Error())
	}
	if d.Fallback {
		c.fallbacks.Add(1)
	}
	return d.WorkerID, nil
}
