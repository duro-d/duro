package controller

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/placement"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Tests specify individual outcomes at the two RPC boundaries. Inheriting the
// unused client methods makes unexpected calls fail the test immediately.
type directory map[string]pb.WorkerServiceClient

func (d directory) Workers(context.Context) ([]string, error) {
	var ids []string
	for id := range d {
		ids = append(ids, id)
	}
	return ids, nil
}
func (d directory) Worker(_ context.Context, id string) (pb.WorkerServiceClient, error) {
	if w, ok := d[id]; ok {
		return w, nil
	}
	return nil, unavailable("worker offline")
}

type worker struct {
	pb.WorkerServiceClient
	poll     func(*pb.PollWorkerRequest) (*pb.PollWorkerResponse, error)
	drain    func(*pb.DrainObjectRequest) (*pb.DrainObjectResponse, error)
	activate func(*pb.ActivateObjectRequest) (*pb.ActivateObjectResponse, error)
	confirm  func(*pb.ConfirmAssignmentsRequest) (*pb.ConfirmAssignmentsResponse, error)
}

func (w worker) PollWorker(_ context.Context, r *pb.PollWorkerRequest, _ ...grpc.CallOption) (*pb.PollWorkerResponse, error) {
	if w.poll != nil {
		return w.poll(r)
	}
	return &pb.PollWorkerResponse{WorkerId: r.WorkerId, Ready: true, HasCapacityForNewAssignments: true}, nil
}
func (w worker) DrainObject(_ context.Context, r *pb.DrainObjectRequest, _ ...grpc.CallOption) (*pb.DrainObjectResponse, error) {
	return w.drain(r)
}
func (w worker) ActivateObject(_ context.Context, r *pb.ActivateObjectRequest, _ ...grpc.CallOption) (*pb.ActivateObjectResponse, error) {
	return w.activate(r)
}
func (w worker) ConfirmAssignments(_ context.Context, r *pb.ConfirmAssignmentsRequest, _ ...grpc.CallOption) (*pb.ConfirmAssignmentsResponse, error) {
	return w.confirm(r)
}

func activeController(t *testing.T, store pb.PlacementServiceClient, dir directory) (*Controller, *session) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	c := New(store, dir, placement.HRW)
	s := &session{ctx: ctx, cancel: cancel, term: 12, started: time.Now().Add(-2 * time.Minute), readyForMoves: true, lastMoved: make(map[string]completedMove)}
	c.leader = s
	return c, s
}

type createStore struct {
	pb.PlacementServiceClient
	receipt   *pb.OperationResult
	proposals []*pb.ReserveCreateRequest
}

func (s *createStore) LookupOperation(_ context.Context, _ *pb.LookupOperationRequest, _ ...grpc.CallOption) (*pb.LookupOperationResponse, error) {
	if s.receipt != nil {
		return &pb.LookupOperationResponse{Observation: &pb.LookupOperationResponse_Completed{Completed: s.receipt}}, nil
	}
	return &pb.LookupOperationResponse{Observation: &pb.LookupOperationResponse_Absent{Absent: &emptypb.Empty{}}}, nil
}
func (s *createStore) ReserveCreate(_ context.Context, r *pb.ReserveCreateRequest, _ ...grpc.CallOption) (*pb.ReserveCreateResponse, error) {
	s.proposals = append(s.proposals, proto.CloneOf(r))
	if len(s.proposals) == 1 {
		return nil, unavailable("reply lost")
	}
	return &pb.ReserveCreateResponse{Outcome: &pb.ReserveCreateResponse_Reservation{Reservation: &pb.CreateReservation{
		Operation: r.Operation, ReservationToken: r.ReservationToken, Lease: &pb.OwnerLease{Owner: &pb.Owner{ObjectId: r.Operation.ObjectId, WorkerId: r.TargetWorkerId, Epoch: 1}},
	}}}, nil
}

func TestCreateRetriesLostReplyWithSameTokenAndReadsReceiptWithoutWorkers(t *testing.T) {
	store := new(createStore)
	dir := directory{"w": worker{}}
	c, s := activeController(t, store, dir)
	op := &pb.CreateOperation{ObjectId: "lot", RequestId: "r"}
	first, err := c.Create(t.Context(), op)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.proposals) != 2 || !proto.Equal(store.proposals[0], store.proposals[1]) || len(first.GetReservation().GetReservationToken()) < 16 || store.proposals[0].ControllerTerm != s.term {
		t.Fatalf("retries: %v", store.proposals)
	}
	again, err := c.Create(t.Context(), op)
	if err != nil || bytes.Equal(first.GetReservation().GetReservationToken(), again.GetReservation().GetReservationToken()) {
		t.Fatalf("fresh attempt: %v %v", again, err)
	}
	delete(dir, "w")
	store.receipt = &pb.OperationResult{Outcome: &pb.OperationResult_DomainRejection{DomainRejection: "declined"}}
	replay, err := c.Create(t.Context(), op)
	if err != nil || !proto.Equal(replay.GetCompleted(), store.receipt) || len(store.proposals) != 3 {
		t.Fatalf("replay: %v, %v", replay, err)
	}
}

type moveStore struct {
	pb.PlacementServiceClient
	fence   func(*pb.FenceTransferRequest) (*pb.FenceTransferResponse, error)
	finish  func(*pb.FinishTransferRequest) (*pb.FinishTransferResponse, error)
	begin   func(*pb.BeginTransferRequest) (*pb.BeginTransferResponse, error)
	abandon func(*pb.AbandonCreateRequest) (*pb.AbandonCreateResponse, error)
}

func (s moveStore) FenceTransfer(_ context.Context, r *pb.FenceTransferRequest, _ ...grpc.CallOption) (*pb.FenceTransferResponse, error) {
	return s.fence(r)
}
func (s moveStore) FinishTransfer(_ context.Context, r *pb.FinishTransferRequest, _ ...grpc.CallOption) (*pb.FinishTransferResponse, error) {
	return s.finish(r)
}
func (s moveStore) BeginTransfer(_ context.Context, r *pb.BeginTransferRequest, _ ...grpc.CallOption) (*pb.BeginTransferResponse, error) {
	return s.begin(r)
}
func (s moveStore) AbandonCreate(_ context.Context, r *pb.AbandonCreateRequest, _ ...grpc.CallOption) (*pb.AbandonCreateResponse, error) {
	return s.abandon(r)
}

func TestDrainFenceAndResumeAtGrantedEpochAfterLeaderLoss(t *testing.T) {
	source := &pb.Owner{ObjectId: "lot", WorkerId: "a", Epoch: 4}
	target := &pb.Owner{ObjectId: "lot", WorkerId: "b", Epoch: 5}
	tr := &pb.PlacementTransfer{Source: &pb.OwnerLease{Owner: source}, ObjectId: "lot", TransitionId: "plan-manual-T", TargetWorkerId: "b", Phase: pb.TransferPhase_TRANSFER_PHASE_DRAINING}
	granted := proto.CloneOf(tr)
	granted.Phase = pb.TransferPhase_TRANSFER_PHASE_ACTIVATING
	granted.Target = &pb.OwnerLease{Owner: target}
	calls := []string{}
	wrongAck := true
	store := moveStore{
		fence: func(r *pb.FenceTransferRequest) (*pb.FenceTransferResponse, error) {
			calls = append(calls, "fence")
			if r.ControllerTerm != 12 || r.TransitionId != tr.TransitionId || !proto.Equal(r.Source, source) {
				t.Fatalf("fence %v", r)
			}
			if r.GetRequireExpiredLease() != nil {
				return nil, unavailable("storage clock has not reached expiry")
			}
			if r.GetDrainAcknowledged() == nil || wrongAck {
				t.Fatalf("unsafe early fence: %v", r)
			}
			return &pb.FenceTransferResponse{Transfer: granted}, nil
		},
		finish: func(r *pb.FinishTransferRequest) (*pb.FinishTransferResponse, error) {
			calls = append(calls, "finish")
			if r.ControllerTerm != 21 || !proto.Equal(r.ActivatedOwner, target) || r.TransitionId != tr.TransitionId {
				t.Fatalf("finish %v", r)
			}
			return &pb.FinishTransferResponse{}, nil
		},
	}
	dir := directory{"a": worker{drain: func(r *pb.DrainObjectRequest) (*pb.DrainObjectResponse, error) {
		calls = append(calls, "drain")
		id := r.TransitionId
		if wrongAck {
			id = "prior"
		}
		return &pb.DrainObjectResponse{Source: r.Source, TransitionId: id, Phase: pb.DrainPhase_DRAIN_PHASE_DRAINED}, nil
	}}}
	c, s := activeController(t, store, dir)
	if err := c.resume(t.Context(), s, tr); status.Code(err) != codes.Unavailable {
		t.Fatalf("wrong drain acknowledgment: %v", err)
	}
	wrongAck = false
	if err := c.resume(t.Context(), s, tr); status.Code(err) != codes.Unavailable {
		t.Fatalf("lost leader after fence: %v", err)
	}
	dir["b"] = worker{activate: func(r *pb.ActivateObjectRequest) (*pb.ActivateObjectResponse, error) {
		calls = append(calls, "activate")
		if !proto.Equal(r.Owner, target) || r.GetTransitionId() != tr.TransitionId {
			t.Fatalf("activate %v", r)
		}
		return &pb.ActivateObjectResponse{Owner: r.Owner, TransitionId: r.TransitionId}, nil
	}}
	s.term = 21
	c.reconcile(t.Context(), s, &pb.PlacementEntry{Assignment: &pb.PlacementEntry_Transfer{Transfer: granted}}, map[string]bool{"b": true}, false)
	if !slices.Equal(calls, []string{"drain", "fence", "drain", "fence", "activate", "finish"}) {
		t.Fatal(calls)
	}
	if s.planned != nil || s.lastMoved["lot"].epoch != 5 {
		t.Fatal("resumed transfer still pending")
	}
}

func TestExpiredStillRegisteredOwnerAndAbandonedReservation(t *testing.T) {
	old := &pb.Owner{ObjectId: "lot", WorkerId: "a", Epoch: 8}
	var begun, abandoned int
	store := moveStore{
		begin: func(r *pb.BeginTransferRequest) (*pb.BeginTransferResponse, error) {
			begun++
			if !proto.Equal(r.Source, old) || r.TargetWorkerId != "a" || r.ControllerTerm != 12 {
				t.Fatalf("recovery %v", r)
			}
			return nil, unavailable("storage recovering")
		},
		abandon: func(r *pb.AbandonCreateRequest) (*pb.AbandonCreateResponse, error) {
			abandoned++
			if !proto.Equal(r.Owner, old) || r.RequestId != "r" || !bytes.Equal(r.ReservationToken, []byte("token")) {
				t.Fatalf("abandon %v", r)
			}
			return &pb.AbandonCreateResponse{}, nil
		},
	}
	c, s := activeController(t, store, directory{"a": worker{}})
	live := map[string]bool{"a": true}
	lease := &pb.OwnerLease{Owner: old}
	create := &pb.PlacementEntry{Assignment: &pb.PlacementEntry_Reservation{Reservation: &pb.CreateReservation{Operation: &pb.CreateOperation{ObjectId: "lot", RequestId: "r"}, Lease: lease, ReservationToken: []byte("token")}}}
	c.reconcile(t.Context(), s, create, live, false)
	c.reconcile(t.Context(), s, create, live, true)
	c.reconcile(t.Context(), s, &pb.PlacementEntry{Assignment: &pb.PlacementEntry_Assigned{Assigned: lease}}, live, true)
	if begun != 1 || abandoned != 1 {
		t.Fatalf("begin %d abandon %d", begun, abandoned)
	}
}

type renewalStore struct {
	pb.PlacementServiceClient
	req   *pb.RenewLeasesRequest
	pages [][]byte
}

func (s *renewalStore) RenewLeases(_ context.Context, r *pb.RenewLeasesRequest, _ ...grpc.CallOption) (*pb.RenewLeasesResponse, error) {
	s.req = r
	return &pb.RenewLeasesResponse{}, nil
}
func (s *renewalStore) ListPlacements(_ context.Context, r *pb.ListPlacementsRequest, _ ...grpc.CallOption) (*pb.ListPlacementsResponse, error) {
	s.pages = append(s.pages, bytes.Clone(r.PageToken))
	if len(r.PageToken) == 0 {
		return &pb.ListPlacementsResponse{NextPageToken: []byte("opaque")}, nil
	}
	return &pb.ListPlacementsResponse{}, nil
}

func TestRenewChecksExactConfirmationAndExpiryAndContinuesShortPages(t *testing.T) {
	store := new(renewalStore)
	a := &pb.Owner{ObjectId: "x", WorkerId: "w", Epoch: 3}
	b := &pb.Owner{ObjectId: "y", WorkerId: "w", Epoch: 4}
	expiry := timestamppb.Now()
	dir := directory{"w": worker{confirm: func(r *pb.ConfirmAssignmentsRequest) (*pb.ConfirmAssignmentsResponse, error) {
		if len(r.Assignments) != 2 {
			t.Fatalf("confirm %v", r)
		}
		return &pb.ConfirmAssignmentsResponse{WorkerId: "w", Confirmed: []*pb.Owner{a, a, {ObjectId: b.ObjectId, WorkerId: b.WorkerId, Epoch: 2}, {ObjectId: "unknown", WorkerId: "w", Epoch: 3}}}, nil
	}}}
	c, s := activeController(t, store, dir)
	entries := []*pb.PlacementEntry{}
	for _, o := range []*pb.Owner{a, b} {
		entries = append(entries, &pb.PlacementEntry{Assignment: &pb.PlacementEntry_Assigned{Assigned: &pb.OwnerLease{Owner: o, LeaseUntil: expiry}}})
	}
	c.renew(s, "w", entries)
	want := &pb.RenewLeasesRequest{ControllerTerm: s.term, Renewals: []*pb.LeaseRenewal{{Owner: a, ExpectedLeaseUntil: expiry}}}
	if !proto.Equal(store.req, want) {
		t.Fatalf("renew %v want %v", store.req, want)
	}
	store.req = nil
	delete(dir, "w")
	c.renew(s, "w", entries)
	if store.req != nil {
		t.Fatal("unreachable worker's lease was renewed")
	}
	pages := 0
	if err := c.scan(t.Context(), workerFilter("w"), func([]*pb.PlacementEntry) { pages++ }); err != nil || pages != 2 || string(store.pages[1]) != "opaque" {
		t.Fatalf("pages %d: %v", pages, err)
	}
}

type electionStore struct {
	pb.PlacementServiceClient
	once    sync.Once
	started chan struct{}
}

func (s *electionStore) RegisterControllerTerm(ctx context.Context, r *pb.RegisterControllerTermRequest, _ ...grpc.CallOption) (*pb.RegisterControllerTermResponse, error) {
	if r.ControllerTerm == 1 {
		s.once.Do(func() { close(s.started) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &pb.RegisterControllerTermResponse{}, nil
}
func (s *electionStore) ListPlacements(context.Context, *pb.ListPlacementsRequest, ...grpc.CallOption) (*pb.ListPlacementsResponse, error) {
	return &pb.ListPlacementsResponse{}, nil
}

func TestSupersedingTermCancelsRegistrationAndActiveRequests(t *testing.T) {
	store := &electionStore{started: make(chan struct{})}
	c := New(store, directory{}, placement.HRW)
	first := make(chan error, 1)
	go func() { first <- c.ServeLeader(t.Context(), 1) }()
	<-store.started
	if c.IsLeader() {
		t.Fatal("writes admitted before storage registered term")
	}
	next, cancel := context.WithCancel(t.Context())
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- c.ServeLeader(next, 2) }()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("earlier registration still running")
	}
	deadline := time.After(time.Second)
	for !c.IsLeader() {
		select {
		case <-deadline:
			t.Fatal("new term did not enter leadership")
		case <-time.After(time.Millisecond):
		}
	}
	_, rpc, done, err := c.operation(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	cancel()
	select {
	case <-rpc.Done():
	case <-time.After(time.Second):
		t.Fatal("leadership loss did not cancel request")
	}
	<-second
	if c.IsLeader() {
		t.Fatal("still leading")
	}
	if err := c.ServeLeader(t.Context(), 1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old term returned %v", err)
	}
}

func TestFailedActivatingTargetReplacedOnlyWithExactOldGrant(t *testing.T) {
	tr := &pb.PlacementTransfer{
		ObjectId: "lot", TransitionId: "lost-target", Phase: pb.TransferPhase_TRANSFER_PHASE_ACTIVATING, TargetWorkerId: "dead",
		Source: &pb.OwnerLease{Owner: &pb.Owner{ObjectId: "lot", WorkerId: "a", Epoch: 3}},
		Target: &pb.OwnerLease{Owner: &pb.Owner{ObjectId: "lot", WorkerId: "dead", Epoch: 4}},
	}
	var replaced bool
	store := moveStore{begin: func(r *pb.BeginTransferRequest) (*pb.BeginTransferResponse, error) {
		replaced = true
		if r.ControllerTerm != 12 || !proto.Equal(r.Source, tr.Target.Owner) || r.GetReplacesTransitionId() != tr.TransitionId || r.TransitionId == tr.TransitionId || r.TargetWorkerId != "other" {
			t.Fatalf("unsafe replacement %v", r)
		}
		return nil, unavailable("storage checking target expiry")
	}}
	c, s := activeController(t, store, directory{"other": worker{}})
	c.reconcile(t.Context(), s, &pb.PlacementEntry{Assignment: &pb.PlacementEntry_Transfer{Transfer: tr}}, map[string]bool{"other": true}, true)
	if !replaced {
		t.Fatal("target loss did not trigger conditional replacement")
	}
}

func TestWrongActivationAcknowledgmentCannotFinish(t *testing.T) {
	old := &pb.Owner{ObjectId: "o", WorkerId: "a", Epoch: 1}
	target := &pb.Owner{ObjectId: "o", WorkerId: "b", Epoch: 2}
	tr := &pb.PlacementTransfer{ObjectId: "o", TransitionId: "t", Source: &pb.OwnerLease{Owner: old}, TargetWorkerId: "b", Target: &pb.OwnerLease{Owner: target}, Phase: pb.TransferPhase_TRANSFER_PHASE_ACTIVATING}
	dir := directory{"b": worker{activate: func(r *pb.ActivateObjectRequest) (*pb.ActivateObjectResponse, error) {
		return &pb.ActivateObjectResponse{Owner: r.Owner}, nil
	}}}
	c, s := activeController(t, moveStore{}, dir)
	if err := c.resume(t.Context(), s, tr); status.Code(err) != codes.Unavailable {
		t.Fatalf("bad activation acknowledgment accepted: %v", err)
	}
}

func TestPlanLoadAdmitsOnlyOneConditionalMove(t *testing.T) {
	var requests []*pb.BeginTransferRequest
	store := moveStore{begin: func(r *pb.BeginTransferRequest) (*pb.BeginTransferResponse, error) {
		requests = append(requests, r)
		return nil, unavailable("begin reply lost")
	}}
	c, s := activeController(t, store, directory{})
	now := time.Now()
	window := func(id string, busy float64) placement.LoadWindow {
		sample := func(t time.Time) placement.Sample { return placement.Sample{At: t, QueueLimit: 1, BusyFraction: busy} }
		return placement.LoadWindow{
			Worker:   placement.Candidate{ID: id, Mode: placement.Active, Ready: true, FreeCapacity: 1, Sample: sample(now)},
			Previous: [2]placement.Sample{sample(now.Add(-2 * time.Second)), sample(now.Add(-time.Second))},
		}
	}
	polls := []observation{
		{window: window("a", 1), hot: []placement.HotObject{{Assignment: placement.Assignment{ObjectID: "lot", WorkerID: "a", Epoch: 5}, CallCount: 20, BusyFraction: .5}}},
		{window: window("b", 0)},
	}
	c.planLoad(t.Context(), s, polls, placement.Limits{}, now)
	c.planLoad(t.Context(), s, polls, placement.Limits{}, now)
	if len(requests) != 1 || requests[0].TargetWorkerId != "b" || !proto.Equal(requests[0].Source, &pb.Owner{ObjectId: "lot", WorkerId: "a", Epoch: 5}) || !proto.Equal(s.planned, requests[0]) {
		t.Fatalf("issued moves: %v", requests)
	}
}

func TestFreshLeaderRespectsCooldownForPriorGenerations(t *testing.T) {
	c, s := activeController(t, moveStore{}, directory{})
	s.started = time.Now().Add(-2 * time.Second)
	m := placement.Move{Source: placement.Assignment{ObjectID: "lot", WorkerID: "a", Epoch: 2}, Target: "b"}
	if err := c.plan(t.Context(), s, m, "manual"); status.Code(err) != codes.Unavailable || s.planned != nil {
		t.Fatalf("unobserved cooldown: %v %v", err, s.planned)
	}
}

type joinStore struct {
	moveStore
	owner *pb.Owner
	pages atomic.Int32
}

func (s *joinStore) ListPlacements(_ context.Context, r *pb.ListPlacementsRequest, _ ...grpc.CallOption) (*pb.ListPlacementsResponse, error) {
	s.pages.Add(1)
	page := &pb.ListPlacementsResponse{}
	if r.GetWorkerId() == s.owner.WorkerId {
		page.Placements = []*pb.PlacementEntry{{Assignment: &pb.PlacementEntry_Assigned{Assigned: &pb.OwnerLease{Owner: s.owner}}}}
	}
	return page, nil
}

type joiningDirectory struct {
	directory
	source string
	joined atomic.Bool
}

func (d *joiningDirectory) Workers(ctx context.Context) ([]string, error) {
	if !d.joined.Load() {
		return []string{d.source}, nil
	}
	return d.directory.Workers(ctx)
}

func TestHRWReconcilesCoolingOwnerAfterJoinOrElection(t *testing.T) {
	for _, stage := range []string{"join", "election"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				nodes := []placement.Candidate{
					{ID: "a", Mode: placement.Active, Ready: true, FreeCapacity: 1},
					{ID: "b", Mode: placement.Active, Ready: true, FreeCapacity: 1},
				}
				best, err := placement.Select(placement.HRW, "lot", nodes, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				source := "a"
				if best.WorkerID == source {
					source = "b"
				}
				store := &joinStore{owner: &pb.Owner{ObjectId: "lot", WorkerId: source, Epoch: 2}}
				started := make(chan *pb.BeginTransferRequest, 2)
				store.begin = func(r *pb.BeginTransferRequest) (*pb.BeginTransferResponse, error) {
					started <- r
					return nil, unavailable("transfer recorded, reply lost")
				}
				dir := &joiningDirectory{directory: directory{source: worker{}, best.WorkerID: worker{}}, source: source}
				c, s := activeController(t, store, dir.directory)
				c.dir = dir
				if stage == "election" {
					dir.joined.Store(true)
					s.started = time.Now()
				} else {
					s.lastMoved["lot"] = completedMove{at: time.Now(), epoch: 2}
				}
				go c.policyLoop(s)
				defer func() { s.cancel(); synctest.Wait() }()
				synctest.Wait()
				if stage == "join" {
					dir.joined.Store(true)
				}
				time.Sleep(15 * time.Second)
				synctest.Wait()
				pages := store.pages.Load()
				time.Sleep(40 * time.Second)
				synctest.Wait()
				if pages == 0 || store.pages.Load() != pages || len(started) != 0 {
					t.Fatalf("cooldown: page reads %d then %d, moves %d", pages, store.pages.Load(), len(started))
				}
				s.mu.Lock()
				s.recovering++
				s.mu.Unlock()
				time.Sleep(10 * time.Second)
				synctest.Wait()
				if len(started) != 0 {
					t.Fatal("started a planned move during recovery")
				}
				s.mu.Lock()
				s.recovering--
				s.mu.Unlock()
				time.Sleep(5 * time.Second)
				synctest.Wait()
				if len(started) != 1 {
					t.Fatalf("moves after cooldown: %d", len(started))
				}
				if r := <-started; r.TargetWorkerId != best.WorkerID || !proto.Equal(r.Source, store.owner) {
					t.Fatalf("unexpected HRW move: %v", r)
				}
			})
		})
	}
}

func TestMetricsExcludeExpiredAndDepartedSamples(t *testing.T) {
	dir := directory{}
	for _, id := range []string{"a", "b", "stale"} {
		dir[id] = worker{poll: func(r *pb.PollWorkerRequest) (*pb.PollWorkerResponse, error) {
			load := &pb.WorkerLoadSample{WindowMs: 1000, QueueLimit: 100, QueueDepth: 7, BusyFraction: .25}
			if r.WorkerId == "stale" {
				load.AgeMs = 3500
			}
			return &pb.PollWorkerResponse{WorkerId: r.WorkerId, Ready: true, HasCapacityForNewAssignments: true, Load: load}, nil
		}}
	}
	c, s := activeController(t, moveStore{}, dir)
	if _, err := c.snapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []WorkerMetric{{ID: "a", Queue: 7, BusyFraction: .25}, {ID: "b", Queue: 7, BusyFraction: .25}}
	if got := c.Metrics(); !slices.Equal(got, want) {
		t.Fatalf("metrics %v want %v", got, want)
	}
	delete(dir, "a")
	if _, err := c.snapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := c.Metrics(); !slices.Equal(got, want[1:]) {
		t.Fatalf("metrics after departure %v", got)
	}
	old := []workerReading{{at: time.Now().Add(-4 * time.Second), metric: want[1]}}
	c.readings.Store(&old)
	if len(c.Metrics()) != 0 {
		t.Fatal("expired sample visible between polls")
	}

	tr := &pb.PlacementTransfer{TransitionId: "recover-1", ObjectId: "x", Target: &pb.OwnerLease{Owner: &pb.Owner{ObjectId: "x", WorkerId: "b", Epoch: 2}}}
	c.finished(s, tr)
	c.finished(s, tr)
	if _, moves := c.Counts(); moves != 1 || c.Recoveries() != 1 {
		t.Fatalf("duplicate finish counted %d/%d", moves, c.Recoveries())
	}
}
