package teststand

import (
	"context"
	"encoding/json"
	"slices"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (w *World) BeginTransfer(_ context.Context, r *pb.BeginTransferRequest) (*pb.BeginTransferResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.controller(r.ControllerTerm); err != nil {
		return nil, err
	}
	if r.ObjectId == "" || r.TransitionId == "" || r.TargetWorkerId == "" {
		return nil, invalid("incomplete transfer")
	}
	o := w.objects[r.ObjectId]
	if o == nil || o.code == nil {
		return nil, missing("object is absent")
	}
	t := o.transfer
	if t != nil && t.TransitionId == r.TransitionId {
		if t.TargetWorkerId != r.TargetWorkerId || !proto.Equal(t.Source.GetOwner(), r.Source) || r.GetReplacesTransitionId() != o.replaces {
			return nil, stale("different parameters for this transition")
		}
		return &pb.BeginTransferResponse{Transfer: t}, nil
	}
	if w.transitions[r.TransitionId] || !proto.Equal(o.lease.GetOwner(), r.Source) {
		return nil, stale("superseded transition or source")
	}
	replacing := r.ReplacesTransitionId != nil
	if replacing {
		if t == nil || t.Phase != pb.TransferPhase_TRANSFER_PHASE_ACTIVATING || t.TransitionId != r.GetReplacesTransitionId() || w.live(t.Target) {
			return nil, stale("replacement requires an expired activation")
		}
	} else if o.pending() {
		return nil, stale("transfer in progress")
	}
	t = &pb.PlacementTransfer{ObjectId: r.ObjectId, TransitionId: r.TransitionId, Source: o.lease, TargetWorkerId: r.TargetWorkerId, Phase: pb.TransferPhase_TRANSFER_PHASE_DRAINING}
	if r.Source == nil || replacing {
		t.Target = w.grant(o, r.ObjectId, r.TargetWorkerId)
		t.Phase = pb.TransferPhase_TRANSFER_PHASE_ACTIVATING
	}
	o.transfer = t
	o.replaces = r.GetReplacesTransitionId()
	w.transitions[r.TransitionId] = true
	return &pb.BeginTransferResponse{Transfer: t}, nil
}

func (w *World) FenceTransfer(_ context.Context, r *pb.FenceTransferRequest) (*pb.FenceTransferResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.controller(r.ControllerTerm); err != nil {
		return nil, err
	}
	o := w.objects[r.Source.GetObjectId()]
	if r.Source == nil || o == nil || o.transfer == nil || o.transfer.TransitionId != r.TransitionId || !proto.Equal(o.transfer.Source.GetOwner(), r.Source) {
		return nil, stale("source or transition changed")
	}
	if r.GetDrainAcknowledged() == nil && r.GetRequireExpiredLease() == nil {
		return nil, invalid("missing fence authority")
	}
	t := o.transfer
	if t.Phase == pb.TransferPhase_TRANSFER_PHASE_DRAINING {
		if r.GetRequireExpiredLease() != nil && w.live(t.Source) {
			return nil, unavailable("source lease is live")
		}
		if r.GetDrainAcknowledged() != nil && !o.drained {
			return nil, stale("source did not acknowledge drain")
		}
		t = proto.CloneOf(t)
		t.Target = w.grant(o, t.ObjectId, t.TargetWorkerId)
		t.Phase = pb.TransferPhase_TRANSFER_PHASE_ACTIVATING
		o.transfer = t
	}
	return &pb.FenceTransferResponse{Transfer: t}, nil
}

func (w *World) FinishTransfer(_ context.Context, r *pb.FinishTransferRequest) (*pb.FinishTransferResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.controller(r.ControllerTerm); err != nil {
		return nil, err
	}
	o := w.objects[r.ActivatedOwner.GetObjectId()]
	if r.ActivatedOwner == nil || o == nil || o.transfer == nil || o.transfer.TransitionId != r.TransitionId || !proto.Equal(o.transfer.Target.GetOwner(), r.ActivatedOwner) {
		return nil, stale("target or transition changed")
	}
	if o.transfer.Phase != pb.TransferPhase_TRANSFER_PHASE_COMPLETE {
		if o.transfer.Phase != pb.TransferPhase_TRANSFER_PHASE_ACTIVATING || !w.live(o.lease) || !o.activated {
			return nil, stale("target did not activate a live lease")
		}
		t := proto.CloneOf(o.transfer)
		t.Phase = pb.TransferPhase_TRANSFER_PHASE_COMPLETE
		o.transfer = t
	}
	return &pb.FinishTransferResponse{}, nil
}

func (w *World) RenewLeases(_ context.Context, r *pb.RenewLeasesRequest) (*pb.RenewLeasesResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.controller(r.ControllerTerm); err != nil {
		return nil, err
	}
	if len(r.Renewals) > 128 {
		return nil, invalid("renewal batch exceeds 128")
	}
	resp := &pb.RenewLeasesResponse{}
	for _, renew := range r.Renewals {
		owner := renew.GetOwner()
		result := &pb.LeaseRenewalResult{RequestedOwner: owner, Outcome: &pb.LeaseRenewalResult_Rejected{Rejected: &emptypb.Empty{}}}
		o := w.objects[owner.GetObjectId()]
		if o != nil && o.code != nil && !o.pending() && w.live(o.lease) && proto.Equal(o.lease.Owner, owner) && renew.ExpectedLeaseUntil != nil {
			if proto.Equal(o.lease.LeaseUntil, renew.ExpectedLeaseUntil) {
				o.lease = &pb.OwnerLease{Owner: owner, LeaseUntil: timestamppb.New(w.now().Add(w.ttl))}
			}
			result.Outcome = &pb.LeaseRenewalResult_Current{Current: o.lease}
		}
		resp.Results = append(resp.Results, result)
	}
	return resp, nil
}

type cursor struct{ Filter, After string }

func (w *World) ListPlacements(_ context.Context, r *pb.ListPlacementsRequest) (*pb.ListPlacementsResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.confirmed(); err != nil {
		return nil, err
	}
	var filter string
	switch {
	case r.GetPending() != nil:
		filter = "pending"
	case r.GetExpired() != nil:
		filter = "expired"
	case r.GetWorkerId() != "":
		filter = "worker/" + r.GetWorkerId()
	default:
		return nil, invalid("missing placement filter")
	}
	pos := cursor{Filter: filter}
	if len(r.PageToken) > 0 && (json.Unmarshal(r.PageToken, &pos) != nil || pos.Filter != filter || pos.After == "") {
		return nil, invalid("invalid page token")
	}
	var ids []string
	for id, o := range w.objects {
		if id <= pos.After || o.lease == nil {
			continue
		}
		matches := false
		switch {
		case r.GetPending() != nil:
			matches = o.reservation != nil || o.pending()
		case r.GetExpired() != nil:
			matches = !w.live(o.lease)
		default:
			matches = o.lease.Owner.WorkerId == r.GetWorkerId() || (o.pending() && o.transfer.TargetWorkerId == r.GetWorkerId())
		}
		if matches {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	limit := r.PageSize
	if limit == 0 || limit > 128 {
		limit = 128
	}
	result := &pb.ListPlacementsResponse{}
	size := 0
	for _, id := range ids {
		o := w.objects[id]
		entry := &pb.PlacementEntry{}
		switch {
		case o.reservation != nil:
			entry.Assignment = &pb.PlacementEntry_Reservation{Reservation: o.reservation}
		case o.pending():
			entry.Assignment = &pb.PlacementEntry_Transfer{Transfer: o.transfer}
		default:
			entry.Assignment = &pb.PlacementEntry_Assigned{Assigned: o.lease}
		}
		nextSize := size + proto.Size(entry) + 6
		// Leave space for the cursor with the last returned ID.
		token, _ := json.Marshal(cursor{Filter: filter, After: id})
		if len(result.Placements) >= int(limit) || nextSize+len(token)+6 > 4<<20 {
			if len(result.Placements) == 0 {
				return nil, duroError(pb.ErrorCode_ERROR_CODE_LIMIT_EXCEEDED, "placement exceeds frame limit")
			}
			result.NextPageToken, _ = json.Marshal(pos)
			break
		}
		result.Placements = append(result.Placements, entry)
		pos.After = id
		size = nextSize
	}
	return result, nil
}
