package teststand

import (
	"context"
	"crypto/sha256"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type stand struct {
	*World
	t     *testing.T
	ctx   context.Context
	state pb.PlacementServiceClient
	a, b  pb.WorkerServiceClient
	clock time.Time
}

func dialFixture(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	l := bufconn.Listen(4 << 20)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(l) }()
	t.Cleanup(server.Stop)
	c, err := grpc.NewClient("passthrough:///fixture", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return l.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func start(t *testing.T) *stand {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	s := &stand{World: New(9 * time.Second), t: t, ctx: ctx, clock: time.Unix(100, 0)}
	s.World.now = func() time.Time { return s.clock }
	s.state = pb.NewPlacementServiceClient(dialFixture(t, func(g *grpc.Server) { pb.RegisterPlacementServiceServer(g, s.World) }))
	for _, id := range []string{"a", "b"} {
		worker := s.Worker(id)
		client := pb.NewWorkerServiceClient(dialFixture(t, func(g *grpc.Server) { pb.RegisterWorkerServiceServer(g, worker) }))
		if id == "a" {
			s.a = client
		} else {
			s.b = client
		}
	}
	_, err := s.state.RegisterControllerTerm(ctx, &pb.RegisterControllerTermRequest{ControllerTerm: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"Auction", "Quota"} {
		p := fixturePackage(typ)
		if _, err = s.a.ValidateCode(ctx, &pb.ValidateCodeRequest{CodePackage: p}); err != nil {
			t.Fatal(err)
		}
		if _, err = s.state.RegisterType(ctx, &pb.RegisterTypeRequest{Code: p}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}
func fixturePackage(typ string) *pb.CodePackage {
	bundle := []byte("fixture " + typ)
	digest := sha256.Sum256(bundle)
	return &pb.CodePackage{Manifest: &pb.CodeManifest{Code: &pb.CodeRef{Type: typ, Version: "1", Sha256: digest[:]}, Runtime: "javascript-v8", Format: "esm", Methods: fixtureMethods(typ)}, Bundle: bundle}
}
func (s *stand) advance(d time.Duration) { s.mu.Lock(); defer s.mu.Unlock(); s.clock = s.clock.Add(d) }
func raw(s string) *pb.JsonValue         { return &pb.JsonValue{Utf8: []byte(s)} }
func op(id, req string) *pb.CreateOperation {
	return &pb.CreateOperation{ObjectId: id, RequestId: req, Type: "Quota", Version: "1", Input: raw(`{"limit":10}`)}
}
func (s *stand) reserve(r *pb.CreateOperation, token string) *pb.CreateReservation {
	s.t.Helper()
	v, err := s.state.ReserveCreate(s.ctx, &pb.ReserveCreateRequest{ControllerTerm: 1, Operation: r, TargetWorkerId: "a", ReservationToken: []byte(token)})
	if err != nil || v.GetReservation() == nil {
		s.t.Fatalf("reserve = %v, %v", v, err)
	}
	return v.GetReservation()
}
func (s *stand) activate(c pb.WorkerServiceClient, id *pb.Owner, transfer *string) {
	s.t.Helper()
	_, err := c.ActivateObject(s.ctx, &pb.ActivateObjectRequest{Owner: id, TransitionId: transfer})
	if err != nil {
		s.t.Fatal(err)
	}
}
func (s *stand) create(r *pb.CreateReservation) *pb.OperationResult {
	s.t.Helper()
	s.activate(s.a, r.Lease.Owner, nil)
	v, err := s.a.CreateObject(s.ctx, &pb.CreateObjectRequest{Owner: r.Lease.Owner, Operation: r.Operation, ReservationToken: r.ReservationToken})
	if err != nil || v.GetOutcome() == nil {
		s.t.Fatalf("create = %v, %v", v, err)
	}
	return v.Outcome
}
func expectCode(t *testing.T, err error, code pb.ErrorCode) {
	t.Helper()
	for _, detail := range status.Convert(err).Details() {
		if d, ok := detail.(*pb.ErrorDetail); ok && d.Code == code {
			return
		}
	}
	t.Fatalf("expected %s, got %v", code, err)
}
func invokeReq(id *pb.Owner, req, method, arg string) *pb.InvokeObjectRequest {
	return &pb.InvokeObjectRequest{Owner: id, Operation: &pb.InvokeOperation{ObjectId: id.ObjectId, RequestId: req, Method: method, Argument: raw(arg)}}
}

func TestReceiptAndStateCommitTogether(t *testing.T) {
	s := start(t)
	r := s.reserve(op("quota", "create"), "token")
	s.create(r)
	call := invokeReq(r.Lease.Owner, "debit", "spend", `{"amount":3,"trace":{"f":1,"g":2}}`)
	if err := s.SetFault("precommit", "a", 1); err != nil {
		t.Fatal(err)
	}
	_, err := s.a.InvokeObject(s.ctx, call)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	lookup := &pb.LookupOperationRequest{Operation: &pb.LookupOperationRequest_Invoke{Invoke: call.Operation}}
	seen, err := s.state.LookupOperation(s.ctx, lookup)
	if err != nil || seen.GetAbsent() == nil {
		t.Fatalf("precommit receipt = %v %v", seen, err)
	}
	if err := s.SetFault("postcommit", "a", 1); err != nil {
		t.Fatal(err)
	}
	_, err = s.a.InvokeObject(s.ctx, call)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	if err := s.SetFault("durability-unknown", "", 2); err != nil {
		t.Fatal(err)
	}
	_, err = s.state.LookupOperation(s.ctx, lookup)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	_, err = s.state.ReadObject(s.ctx, &pb.ReadObjectRequest{ObjectId: "quota"})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	again := proto.CloneOf(call)
	again.Operation.Argument = raw(`{"trace":{"g":2,"f":1e0},"amount":3.0}`)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := s.a.InvokeObject(s.ctx, again)
			if err != nil || string(v.GetOutcome().GetResult().GetUtf8()) != `{"remaining":7}` {
				t.Errorf("replay = %v, %v", v, err)
			}
		}()
	}
	wg.Wait()
	seen, err = s.state.LookupOperation(s.ctx, lookup)
	if err != nil || string(seen.GetCompleted().GetResult().GetUtf8()) != `{"remaining":7}` {
		t.Fatalf("confirmed receipt = %v, %v", seen, err)
	}
	again.Operation.Argument = raw(`{"amount":4}`)
	_, err = s.a.InvokeObject(s.ctx, again)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_REQUEST_ID_CONFLICT)
	again.Operation.RequestId = "create"
	_, err = s.a.InvokeObject(s.ctx, again)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_REQUEST_ID_CONFLICT)
	rej := invokeReq(r.Lease.Owner, "too-large", "spend", `{"amount":10}`)
	if err = s.SetFault("postcommit", "", 1); err != nil {
		t.Fatal(err)
	}
	_, err = s.a.InvokeObject(s.ctx, rej)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	v, err := s.a.InvokeObject(s.ctx, rej)
	if err != nil || v.GetOutcome().GetDomainRejection() == "" {
		t.Fatalf("rejection = %v, %v", v, err)
	}
	obj, err := s.state.ReadObject(s.ctx, &pb.ReadObjectRequest{ObjectId: "quota"})
	if err != nil || string(obj.GetKv()["remaining"].GetUtf8()) != "7" {
		t.Fatalf("balance = %v, %v", obj, err)
	}
}
func TestReservationAndInvisibleRejection(t *testing.T) {
	s := start(t)
	bad := op("q", "first")
	bad.Input = raw(`{"limit":-1}`)
	r := s.reserve(bad, "one")
	_, err := s.state.ReadObject(s.ctx, &pb.ReadObjectRequest{ObjectId: "q"})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_NOT_FOUND)
	second := op("q", "another")
	_, err = s.state.ReserveCreate(s.ctx, &pb.ReserveCreateRequest{ControllerTerm: 1, Operation: second, TargetWorkerId: "b", ReservationToken: []byte("two")})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	same, err := s.state.ReserveCreate(s.ctx, &pb.ReserveCreateRequest{ControllerTerm: 1, Operation: bad, TargetWorkerId: "b", ReservationToken: []byte("other")})
	if err != nil || !proto.Equal(same.GetReservation(), r) {
		t.Fatalf("pending repeat = %v, %v", same, err)
	}
	if got := s.create(r).GetDomainRejection(); got == "" {
		t.Fatal("missing init rejection")
	}
	_, err = s.state.DescribeObject(s.ctx, &pb.DescribeObjectRequest{ObjectId: "q"})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_NOT_FOUND)
	next := s.reserve(second, "two")
	release := &pb.AbandonCreateRequest{ControllerTerm: 1, Owner: next.Lease.Owner, RequestId: second.RequestId, ReservationToken: []byte("two")}
	if _, err = s.state.AbandonCreate(s.ctx, release); err != nil {
		t.Fatal(err)
	}
	if _, err = s.state.AbandonCreate(s.ctx, release); err != nil {
		t.Fatal(err)
	}
	_, err = s.state.ReserveCreate(s.ctx, &pb.ReserveCreateRequest{ControllerTerm: 1, Operation: second, TargetWorkerId: "a", ReservationToken: []byte("two")})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
	last := s.reserve(second, "three")
	if last.Lease.Owner.Epoch <= next.Lease.Owner.Epoch {
		t.Fatal("abandon reused an epoch")
	}
	s.activate(s.a, last.Lease.Owner, nil)
	_, err = s.a.CreateObject(s.ctx, &pb.CreateObjectRequest{Owner: next.Lease.Owner, Operation: second, ReservationToken: next.ReservationToken})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
	s.create(last)
	v, err := s.state.ReserveCreate(s.ctx, &pb.ReserveCreateRequest{ControllerTerm: 1, Operation: bad, TargetWorkerId: "a", ReservationToken: []byte("one")})
	if err != nil || v.GetCompleted().GetDomainRejection() == "" {
		t.Fatalf("previous rejected init = %v, %v", v, err)
	}
	if _, err = s.state.ReserveCreate(s.ctx, &pb.ReserveCreateRequest{ControllerTerm: 1, Operation: op("q", "new"), TargetWorkerId: "a", ReservationToken: []byte("four")}); err == nil {
		t.Fatal("expected existing object")
	} else {
		expectCode(t, err, pb.ErrorCode_ERROR_CODE_OBJECT_ALREADY_EXISTS)
	}
}

func TestDrainFenceAndActivation(t *testing.T) {
	s := start(t)
	r := s.reserve(op("q", "c"), "one")
	s.create(r)
	a := r.Lease.Owner
	req := invokeReq(a, "mutate", "spend", `{"amount":1}`)
	prior, err := s.a.InvokeObject(s.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	begin := &pb.BeginTransferRequest{ControllerTerm: 1, ObjectId: "q", TransitionId: "move", Source: a, TargetWorkerId: "b"}
	if _, err = s.state.BeginTransfer(s.ctx, begin); err != nil {
		t.Fatal(err)
	}
	_, err = s.state.GetOwner(s.ctx, &pb.GetOwnerRequest{ObjectId: "q"})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	fence := &pb.FenceTransferRequest{ControllerTerm: 1, TransitionId: "move", Source: a, Authority: &pb.FenceTransferRequest_RequireExpiredLease{RequireExpiredLease: &emptypb.Empty{}}}
	_, err = s.state.FenceTransfer(s.ctx, fence)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	_, err = s.a.DrainObject(s.ctx, &pb.DrainObjectRequest{Source: a, TransitionId: "move"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.a.ActivateObject(s.ctx, &pb.ActivateObjectRequest{Owner: a})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
	_, err = s.a.InvokeObject(s.ctx, req)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
	fence.Authority = &pb.FenceTransferRequest_DrainAcknowledged{DrainAcknowledged: &emptypb.Empty{}}
	if err = s.SetFault("durability-unknown", "", 1); err != nil {
		t.Fatal(err)
	}
	_, err = s.state.FenceTransfer(s.ctx, fence)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	_, err = s.state.RegisterControllerTerm(s.ctx, &pb.RegisterControllerTermRequest{ControllerTerm: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.state.FenceTransfer(s.ctx, fence)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_CONTROLLER)
	fence.ControllerTerm = 2
	grant, err := s.state.FenceTransfer(s.ctx, fence)
	if err != nil {
		t.Fatal(err)
	}
	b := grant.Transfer.Target.Owner
	if b.WorkerId != "b" || b.Epoch <= a.Epoch {
		t.Fatalf("bad target %v", b)
	}
	retry, err := s.state.FenceTransfer(s.ctx, fence)
	if err != nil || !proto.Equal(retry, grant) {
		t.Fatalf("repeated fence = %v, %v", retry, err)
	}
	finish := &pb.FinishTransferRequest{ControllerTerm: 2, TransitionId: "move", ActivatedOwner: b}
	_, err = s.state.FinishTransfer(s.ctx, finish)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
	s.activate(s.b, b, &begin.TransitionId)
	req.Owner = b
	_, err = s.b.InvokeObject(s.ctx, req)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
	if _, err = s.state.FinishTransfer(s.ctx, finish); err != nil {
		t.Fatal(err)
	}
	if _, err = s.state.FinishTransfer(s.ctx, finish); err != nil {
		t.Fatal(err)
	}
	out, err := s.b.InvokeObject(s.ctx, req)
	if err != nil || !proto.Equal(prior.GetOutcome(), out.GetOutcome()) {
		t.Fatalf("receipt after move = %v, %v", out, err)
	}
	req.Owner = a
	_, err = s.a.InvokeObject(s.ctx, req)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
}
