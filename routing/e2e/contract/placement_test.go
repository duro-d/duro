package contract_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/controller"
	"github.com/duro-d/duro/routing/internal/gateway"
	"github.com/duro-d/duro/routing/internal/placement"
	"github.com/duro-d/duro/routing/internal/teststand"
	"github.com/duro-d/duro/routing/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Pause term 1 at the fence RPC. Cancellation drops the reply at that exact
// boundary, including when storage has already granted the target epoch.
type pausedFence struct {
	*teststand.World
	afterGrant bool
	paused     chan struct{}
	once       sync.Once
}

func (s *pausedFence) FenceTransfer(ctx context.Context, r *pb.FenceTransferRequest) (*pb.FenceTransferResponse, error) {
	if r.ControllerTerm != 1 {
		return s.World.FenceTransfer(ctx, r)
	}
	if s.afterGrant {
		if _, err := s.World.FenceTransfer(ctx, r); err != nil {
			return nil, err
		}
	}
	s.once.Do(func() { close(s.paused) })
	<-ctx.Done()
	return nil, ctx.Err()
}

type observedWorker struct {
	*teststand.Worker
	polls atomic.Int32
	fresh *atomic.Bool
}

func (w *observedWorker) PollWorker(ctx context.Context, r *pb.PollWorkerRequest) (*pb.PollWorkerResponse, error) {
	p, err := w.Worker.PollWorker(ctx, r)
	if err != nil {
		return nil, err
	}
	p.Load = &pb.WorkerLoadSample{WindowMs: 1000, QueueLimit: 10}
	if !w.fresh.Load() {
		p.Load.AgeMs = 4000
	}
	if r.WorkerId == "a" {
		p.Load.QueueDepth = 10
		p.Load.BusyFraction = 1
		p.Load.HotObjects = []*pb.HotObject{{Owner: &pb.Owner{WorkerId: "a", ObjectId: "c", Epoch: 1}, CallCount: 8, BusyFraction: 0.5}}
	}
	w.polls.Add(1)
	return p, nil
}

type directory struct {
	joined  atomic.Bool
	workers map[string]pb.WorkerServiceClient
}

func (d *directory) Workers(context.Context) ([]string, error) {
	if d.joined.Load() {
		return []string{"a", "b"}, nil
	}
	return []string{"a"}, nil
}

func (d *directory) Worker(_ context.Context, id string) (pb.WorkerServiceClient, error) {
	w := d.workers[id]
	if w == nil {
		return nil, errors.New("unknown worker")
	}
	return w, nil
}

func endpoint(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	socket := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(socket) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return socket.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func await(t *testing.T, what string, f func() bool) {
	t.Helper()
	until := time.Now().Add(12 * time.Second)
	for !f() {
		if time.Now().After(until) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func startLeader(t *testing.T, c *controller.Controller, term uint64) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = c.ServeLeader(ctx, term)
	}()
	stop := func() { cancel(); <-stopped }
	t.Cleanup(stop)
	await(t, "active term", c.IsLeader)
	return stop
}

func checkDuroCode(t *testing.T, err error, want pb.ErrorCode) {
	t.Helper()
	for _, detail := range status.Convert(err).Details() {
		if d, ok := detail.(*pb.ErrorDetail); ok && d.Code == want {
			return
		}
	}
	t.Fatalf("wanted %s, received %v", want, err)
}

func quotaClient(t *testing.T, c *controller.Controller, dir *directory, store pb.PlacementServiceClient, ids ...string) *sdk.Client {
	t.Helper()
	ctx := t.Context()
	backend := struct {
		*controller.Controller
		*directory
	}{c, dir}
	const token = "test"
	httpServer := httptest.NewServer(gateway.New(gateway.Config{Token: token}, store, backend))
	t.Cleanup(httpServer.Close)
	client, err := sdk.NewClient([]string{httpServer.URL}, token)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := os.ReadFile("../fixtures/quota.js")
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(bundle)
	_, err = client.RegisterType(ctx, sdk.Manifest{Type: "quota", Version: "1", Format: "esm", Runtime: "javascript-v8", Methods: []string{"spend"}, SHA256: hex.EncodeToString(hash[:])}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		op, err := client.PrepareCreate(sdk.CreateRequest{ObjectID: id, Type: "quota", Version: "1", Input: []byte(`{"limit":10}`), RequestID: "create-" + id})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := op.Do(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return client
}

func TestAutomaticTransferSurvivesLeaderChange(t *testing.T) {
	for _, policy := range []placement.Policy{placement.HRW, placement.Load} {
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			var fresh atomic.Bool
			s := &pausedFence{World: teststand.New(time.Minute), afterGrant: policy == placement.Load, paused: make(chan struct{})}
			store := pb.NewPlacementServiceClient(endpoint(t, func(g *grpc.Server) { pb.RegisterPlacementServiceServer(g, s) }))
			dir := &directory{workers: make(map[string]pb.WorkerServiceClient)}
			workers := make(map[string]*observedWorker)
			for _, id := range []string{"a", "b"} {
				w := &observedWorker{Worker: s.Worker(id), fresh: &fresh}
				workers[id] = w
				dir.workers[id] = pb.NewWorkerServiceClient(endpoint(t, func(g *grpc.Server) { pb.RegisterWorkerServiceServer(g, w) }))
			}
			c := controller.New(store, dir, policy)
			stop := startLeader(t, c, 1)
			// For the HRW wire format, a stays on a and c moves to b.
			client := quotaClient(t, c, dir, store, "a", "c")
			expectOwner := func(id, worker string, epoch uint64) {
				t.Helper()
				r, err := store.GetOwner(ctx, &pb.GetOwnerRequest{ObjectId: id})
				o := r.GetActive().GetOwner()
				if err != nil || o.GetObjectId() != id || o.GetWorkerId() != worker || o.GetEpoch() != epoch {
					t.Fatalf("owner of %s = %v, %v; want %s at %d", id, o, err, worker, epoch)
				}
			}
			expectOwner("a", "a", 1)
			expectOwner("c", "a", 1)
			// Observe an initial one-worker policy tick before adding b.
			await(t, "two polls of a", func() bool { return workers["a"].polls.Load() >= 2 })
			dir.joined.Store(true)
			if policy == placement.Load {
				await(t, "three stale load reports", func() bool { return workers["b"].polls.Load() >= 3 })
				fallbacks, _ := c.Counts()
				w, err := c.Candidate(ctx, "c")
				after, _ := c.Counts()
				if err != nil || w != "b" || after != fallbacks+1 {
					t.Fatalf("stale reports: candidate %q, err %v, HRW fallbacks %d -> %d", w, err, fallbacks, after)
				}
				expectOwner("c", "a", 1)
			}
			fresh.Store(true)
			select {
			case <-s.paused:
			case <-time.After(12 * time.Second):
				t.Fatal("automatic transfer never reached the fence")
			}
			stop()
			pending, err := store.ListPlacements(ctx, &pb.ListPlacementsRequest{Filter: &pb.ListPlacementsRequest_Pending{Pending: &emptypb.Empty{}}})
			if err != nil || len(pending.GetPlacements()) != 1 {
				t.Fatalf("pending transfer: %v, %v", pending, err)
			}
			tr := pending.Placements[0].GetTransfer()
			old := &pb.Owner{ObjectId: "c", WorkerId: "a", Epoch: 1}
			newOwner := &pb.Owner{ObjectId: "c", WorkerId: "b", Epoch: 2}
			if !proto.Equal(tr.GetSource().GetOwner(), old) || tr.GetTargetWorkerId() != "b" {
				t.Fatalf("unexpected transfer %v", tr)
			}
			if s.afterGrant {
				if tr.Phase != pb.TransferPhase_TRANSFER_PHASE_ACTIVATING || !proto.Equal(tr.GetTarget().GetOwner(), newOwner) {
					t.Fatalf("lost fence reply: %v", tr)
				}
			} else if tr.Phase != pb.TransferPhase_TRANSFER_PHASE_DRAINING || tr.Target != nil {
				t.Fatalf("cancelled fence: %v", tr)
			}
			expectOwner("a", "a", 1)
			spend := &pb.InvokeOperation{ObjectId: "c", Method: "spend", Argument: &pb.JsonValue{Utf8: []byte(`{"amount":1}`)}, RequestId: "spend"}
			for _, owner := range []*pb.Owner{old, newOwner} {
				_, err = dir.workers[owner.WorkerId].InvokeObject(ctx, &pb.InvokeObjectRequest{Owner: owner, Operation: spend})
				checkDuroCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
			}
			next := controller.New(store, dir, policy)
			startLeader(t, next, 2)
			await(t, "resumed and activated transfer", func() bool {
				r, err := store.GetOwner(ctx, &pb.GetOwnerRequest{ObjectId: "c"})
				return err == nil && proto.Equal(r.GetActive().GetOwner(), newOwner)
			})
			expectOwner("a", "a", 1)
			_, err = store.RegisterControllerTerm(ctx, &pb.RegisterControllerTermRequest{ControllerTerm: 1})
			checkDuroCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_CONTROLLER)
			_, err = dir.workers["a"].InvokeObject(ctx, &pb.InvokeObjectRequest{Owner: old, Operation: spend})
			checkDuroCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
			op, err := client.PrepareInvoke(sdk.InvokeRequest{ObjectID: "c", Method: "spend", Argument: spend.Argument.Utf8, RequestID: spend.RequestId})
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				result, err := op.Do(ctx)
				if err != nil || string(result.Result) != `{"remaining":9}` {
					t.Fatalf("confirmed spend: %s, %v", result.Result, err)
				}
			}
			state, err := client.Read(ctx, "c")
			if err != nil || string(state["remaining"]) != "9" {
				t.Fatalf("state after replay: %v, %v", state, err)
			}
		})
	}
}

// The first spend reaches the selected commit fault. Before the gateway gets
// the transport error, the source goes offline and storage stops confirming reads.
type failingSource struct {
	*teststand.Worker
	state *teststand.World
	fault string
	armed atomic.Bool
}

func (s *failingSource) InvokeObject(ctx context.Context, r *pb.InvokeObjectRequest) (*pb.InvokeObjectResponse, error) {
	if !s.armed.CompareAndSwap(true, false) {
		return s.Worker.InvokeObject(ctx, r)
	}
	if err := s.state.SetFault(s.fault, "a", 1); err != nil {
		return nil, err
	}
	resp, err := s.Worker.InvokeObject(ctx, r)
	if setupErr := s.state.SetFault("worker-down", "a", 100000); setupErr != nil {
		return nil, setupErr
	}
	if setupErr := s.state.SetFault("durability-unknown", "", 100000); setupErr != nil {
		return nil, setupErr
	}
	return resp, err
}

func TestOwnerLossAtCommit(t *testing.T) {
	for _, policy := range []placement.Policy{placement.HRW, placement.Load} {
		for _, fault := range []string{"precommit", "postcommit"} {
			t.Run(string(policy)+"/"+fault, func(t *testing.T) {
				t.Parallel()
				ctx := t.Context()
				world := teststand.New(5 * time.Second)
				store := pb.NewPlacementServiceClient(endpoint(t, func(g *grpc.Server) { pb.RegisterPlacementServiceServer(g, world) }))
				source := &failingSource{Worker: world.Worker("a"), state: world, fault: fault}
				dir := &directory{workers: map[string]pb.WorkerServiceClient{}}
				dir.workers["a"] = pb.NewWorkerServiceClient(endpoint(t, func(g *grpc.Server) { pb.RegisterWorkerServiceServer(g, source) }))
				dir.workers["b"] = pb.NewWorkerServiceClient(endpoint(t, func(g *grpc.Server) { pb.RegisterWorkerServiceServer(g, world.Worker("b")) }))
				c := controller.New(store, dir, policy)
				startLeader(t, c, 1)
				client := quotaClient(t, c, dir, store, "a")
				lease, err := store.GetOwner(ctx, &pb.GetOwnerRequest{ObjectId: "a"})
				old := lease.GetActive().GetOwner()
				if err != nil || old.GetWorkerId() != "a" || old.GetEpoch() != 1 {
					t.Fatalf("initial owner = %v, %v", old, err)
				}

				arg := []byte(`{"amount":1}`)
				spend, err := client.PrepareInvoke(sdk.InvokeRequest{ObjectID: "a", Method: "spend", RequestID: "failed-spend", Argument: arg})
				if err != nil {
					t.Fatal(err)
				}
				source.armed.Store(true)
				_, err = spend.Do(ctx)
				var apiErr *sdk.Error
				if !errors.As(err, &apiErr) || !apiErr.UnknownOutcome || apiErr.Code != "UNAVAILABLE" || apiErr.RequestID != spend.RequestID() {
					t.Fatalf("expected unconfirmed spend with original request ID, got %v", err)
				}
				if err := world.SetFault("durability-unknown", "", 0); err != nil {
					t.Fatal(err)
				}
				balance := "10"
				if fault == "postcommit" {
					balance = "9"
				}
				state, err := client.Read(ctx, "a")
				if err != nil || string(state["remaining"]) != balance {
					t.Fatalf("primary state at %s: %v, %v; want %s", fault, state, err, balance)
				}
				desc, err := client.Describe(ctx, "a")
				if err != nil || desc.ObjectID != "a" || desc.Type != "quota" {
					t.Fatalf("describe with owner offline: %v, %v", desc, err)
				}

				dir.joined.Store(true)
				await(t, "replacement of failed owner", func() bool {
					own, err := store.GetOwner(ctx, &pb.GetOwnerRequest{ObjectId: "a"})
					o := own.GetActive().GetOwner()
					return err == nil && o.GetWorkerId() == "b" && o.GetEpoch() == old.Epoch+1
				})
				if err := world.SetFault("worker-down", "a", 0); err != nil {
					t.Fatal(err)
				}
				stale := &pb.InvokeOperation{ObjectId: "a", Method: "spend", RequestId: "old-owner-write", Argument: &pb.JsonValue{Utf8: arg}}
				_, err = dir.workers["a"].InvokeObject(ctx, &pb.InvokeObjectRequest{Owner: old, Operation: stale})
				checkDuroCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
				for range 2 {
					result, err := spend.Do(ctx)
					if err != nil || string(result.Result) != `{"remaining":9}` {
						t.Fatalf("replay after %s: %s, %v", fault, result.Result, err)
					}
				}
				next, err := client.PrepareInvoke(sdk.InvokeRequest{ObjectID: "a", Method: "spend", RequestID: "new-spend", Argument: arg})
				if err != nil {
					t.Fatal(err)
				}
				result, err := next.Do(ctx)
				if err != nil || string(result.Result) != `{"remaining":8}` {
					t.Fatalf("new write on replacement owner: %s, %v", result.Result, err)
				}
				state, err = client.Read(ctx, "a")
				if err != nil || string(state["remaining"]) != "8" {
					t.Fatalf("state after owner recovery: %v, %v", state, err)
				}
			})
		}
	}
}
