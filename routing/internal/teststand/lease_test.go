package teststand

import (
	"testing"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestPagingAndStorageClock(t *testing.T) {
	s := start(t)
	first := s.reserve(op("a1", "c"), "a1-token")
	s.create(first)
	s.reserve(op("b1", "c"), "b1-token")
	s.reserve(op("c1", "c"), "c1-token")
	req := &pb.ListPlacementsRequest{Filter: &pb.ListPlacementsRequest_Pending{Pending: &emptypb.Empty{}}, PageSize: 1}
	one, err := s.state.ListPlacements(s.ctx, req)
	if err != nil || len(one.GetPlacements()) != 1 || one.Placements[0].GetReservation().Operation.ObjectId != "b1" || len(one.NextPageToken) == 0 {
		t.Fatalf("page 1 = %v %v", one, err)
	}
	// New keys before the cursor belong to the next pass; keys after it remain discoverable.
	s.reserve(op("b0", "c"), "b0-token")
	req.PageToken = one.NextPageToken
	two, err := s.state.ListPlacements(s.ctx, req)
	if err != nil || len(two.GetPlacements()) != 1 || two.Placements[0].GetReservation().Operation.ObjectId != "c1" || len(two.NextPageToken) > 0 {
		t.Fatalf("page 2 = %v %v", two, err)
	}
	req.Filter = &pb.ListPlacementsRequest_WorkerId{WorkerId: "a"}
	_, err = s.state.ListPlacements(s.ctx, req)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT)

	owner := first.Lease.Owner
	if err = s.SetFault("worker-down", "a", 2); err != nil {
		t.Fatal(err)
	}
	confirm := &pb.ConfirmAssignmentsRequest{WorkerId: "a", Assignments: []*pb.Owner{owner}}
	_, err = s.a.ConfirmAssignments(s.ctx, confirm)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	_, err = s.a.PollWorker(s.ctx, &pb.PollWorkerRequest{WorkerId: "a"})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_UNAVAILABLE)
	confirmed, err := s.a.ConfirmAssignments(s.ctx, confirm)
	if err != nil || len(confirmed.GetConfirmed()) != 1 {
		t.Fatalf("confirm = %v %v", confirmed, err)
	}
	s.advance(3 * time.Second)
	renew := &pb.RenewLeasesRequest{ControllerTerm: 1, Renewals: []*pb.LeaseRenewal{{Owner: owner, ExpectedLeaseUntil: first.Lease.LeaseUntil}}}
	extended, err := s.state.RenewLeases(s.ctx, renew)
	if err != nil || len(extended.GetResults()) != 1 || extended.Results[0].GetCurrent().GetLeaseUntil().AsTime().Sub(first.Lease.LeaseUntil.AsTime()) != 3*time.Second {
		t.Fatalf("renew = %v %v", extended, err)
	}
	s.advance(time.Second)
	replay, err := s.state.RenewLeases(s.ctx, renew)
	if err != nil || !proto.Equal(replay, extended) {
		t.Fatalf("stale report extended lease twice: %v %v", replay, err)
	}
	s.advance(8 * time.Second)
	replay, err = s.state.RenewLeases(s.ctx, renew)
	if err != nil || replay.Results[0].GetRejected() == nil {
		t.Fatalf("renew at expiry = %v %v", replay, err)
	}
	_, err = s.a.InvokeObject(s.ctx, invokeReq(owner, "late", "getRemaining", "null"))
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
	expired, err := s.state.ListPlacements(s.ctx, &pb.ListPlacementsRequest{Filter: &pb.ListPlacementsRequest_Expired{Expired: &emptypb.Empty{}}})
	if err != nil || len(expired.GetPlacements()) != 4 {
		t.Fatalf("expired = %v %v", expired, err)
	}
	begin := &pb.BeginTransferRequest{ControllerTerm: 1, ObjectId: "a1", Source: owner, TargetWorkerId: "b", TransitionId: "first"}
	if _, err = s.state.BeginTransfer(s.ctx, begin); err != nil {
		t.Fatal(err)
	}
	grant, err := s.state.FenceTransfer(s.ctx, &pb.FenceTransferRequest{ControllerTerm: 1, Source: owner, TransitionId: "first", Authority: &pb.FenceTransferRequest_RequireExpiredLease{RequireExpiredLease: &emptypb.Empty{}}})
	if err != nil {
		t.Fatal(err)
	}
	b := grant.Transfer.Target.Owner
	replacement := &pb.BeginTransferRequest{ControllerTerm: 1, ObjectId: "a1", Source: b, TargetWorkerId: "a", TransitionId: "replace", ReplacesTransitionId: &begin.TransitionId}
	_, err = s.state.BeginTransfer(s.ctx, replacement)
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
	s.advance(9 * time.Second)
	latest, err := s.state.BeginTransfer(s.ctx, replacement)
	if err != nil || latest.GetTransfer().GetTarget().GetOwner().GetEpoch() != b.Epoch+1 {
		t.Fatalf("replacement = %v %v", latest, err)
	}
	_, err = s.b.ActivateObject(s.ctx, &pb.ActivateObjectRequest{Owner: b, TransitionId: &begin.TransitionId})
	expectCode(t, err, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
}

func TestAuctionAndAdmissionFaults(t *testing.T) {
	s := start(t)
	op := &pb.CreateOperation{ObjectId: "lot", RequestId: "c", Type: "Auction", Version: "1", Input: raw(`{"startPrice":5}`)}
	r := s.reserve(op, "new")
	s.create(r)
	owner := r.Lease.Owner
	bid := invokeReq(owner, "ok", "bid", `{"bidder":"Ann","amount":10}`)
	for kind, code := range map[string]pb.ErrorCode{"overloaded": pb.ErrorCode_ERROR_CODE_OVERLOADED, "execution-timeout": pb.ErrorCode_ERROR_CODE_EXECUTION_TIMEOUT, "stale": pb.ErrorCode_ERROR_CODE_STALE_OWNER} {
		if err := s.SetFault(kind, "a", 1); err != nil {
			t.Fatal(err)
		}
		_, err := s.a.InvokeObject(s.ctx, bid)
		expectCode(t, err, code)
	}
	if _, err := s.a.InvokeObject(s.ctx, bid); err != nil {
		t.Fatal(err)
	}
	low, err := s.a.InvokeObject(s.ctx, invokeReq(owner, "low", "bid", `{"bidder":"Bob","amount":8}`))
	if err != nil || low.GetOutcome().GetDomainRejection() == "" {
		t.Fatalf("low bid = %v %v", low, err)
	}
	if _, err = s.a.InvokeObject(s.ctx, invokeReq(owner, "close", "close", "null")); err != nil {
		t.Fatal(err)
	}
	high, err := s.a.InvokeObject(s.ctx, invokeReq(owner, "late", "bid", `{"bidder":"Bob","amount":20}`))
	if err != nil || high.GetOutcome().GetDomainRejection() == "" {
		t.Fatalf("late bid = %v %v", high, err)
	}
	state, err := s.state.ReadObject(s.ctx, &pb.ReadObjectRequest{ObjectId: "lot"})
	if err != nil || string(state.GetKv()["auction"].GetUtf8()) != `{"price":10,"bidder":"Ann","closed":true}` {
		t.Fatalf("final auction = %v %v", state, err)
	}
}
