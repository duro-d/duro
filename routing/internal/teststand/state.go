package teststand

import (
	"bytes"
	"context"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (w *World) RegisterControllerTerm(_ context.Context, r *pb.RegisterControllerTermRequest) (*pb.RegisterControllerTermResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if r.ControllerTerm == 0 || r.ControllerTerm < w.term {
		return nil, duroError(pb.ErrorCode_ERROR_CODE_STALE_CONTROLLER, "earlier controller term")
	}
	if err := w.confirmed(); err != nil {
		return nil, err
	}
	w.term = r.ControllerTerm
	return &pb.RegisterControllerTermResponse{}, nil
}
func (w *World) RegisterType(_ context.Context, r *pb.RegisterTypeRequest) (*pb.RegisterTypeResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.confirmed(); err != nil {
		return nil, err
	}
	if err := checkFixture(r.Code); err != nil {
		return nil, err
	}
	c := r.Code.Manifest.Code
	key := packageKey{c.Type, c.Version}
	if p := w.packages[key]; p != nil && !proto.Equal(p, r.Code) {
		return nil, duroError(pb.ErrorCode_ERROR_CODE_CODE_VERSION_CONFLICT, "package differs at this version")
	}
	w.packages[key] = r.Code
	return &pb.RegisterTypeResponse{Code: c}, nil
}
func (w *World) committed(id string) (*object, error) {
	if err := w.confirmed(); err != nil {
		return nil, err
	}
	o := w.objects[id]
	if o == nil || o.code == nil {
		return nil, missing("object is absent")
	}
	return o, nil
}
func (w *World) DescribeObject(_ context.Context, r *pb.DescribeObjectRequest) (*pb.DescribeObjectResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	o, err := w.committed(r.ObjectId)
	if err != nil {
		return nil, err
	}
	return &pb.DescribeObjectResponse{ObjectId: r.ObjectId, Code: o.code.Manifest.Code, Methods: o.code.Manifest.Methods}, nil
}
func (w *World) ReadObject(_ context.Context, r *pb.ReadObjectRequest) (*pb.ReadObjectResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	o, err := w.committed(r.ObjectId)
	if err != nil {
		return nil, err
	}
	// A later commit replaces the whole map. A response can outlive the lock.
	return &pb.ReadObjectResponse{Kv: o.kv}, nil
}
func (w *World) GetOwner(_ context.Context, r *pb.GetOwnerRequest) (*pb.GetOwnerResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	o, err := w.committed(r.ObjectId)
	if err != nil {
		return nil, err
	}
	if o.pending() {
		return nil, unavailable("transfer pending")
	}
	if o.lease == nil {
		return &pb.GetOwnerResponse{Assignment: &pb.GetOwnerResponse_Unassigned{Unassigned: &emptypb.Empty{}}}, nil
	}
	if !w.live(o.lease) {
		return nil, unavailable("lease expired")
	}
	return &pb.GetOwnerResponse{Assignment: &pb.GetOwnerResponse_Active{Active: o.lease}}, nil
}
func (w *World) LookupOperation(_ context.Context, r *pb.LookupOperationRequest) (*pb.LookupOperationResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.confirmed(); err != nil {
		return nil, err
	}
	op := operation{create: r.GetCreate(), invoke: r.GetInvoke()}
	if !op.valid() {
		return nil, invalid("invalid operation")
	}
	id, _ := op.ids()
	o := w.objects[id]
	res, err := o.lookup(op)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return &pb.LookupOperationResponse{Observation: &pb.LookupOperationResponse_Completed{Completed: res}}, nil
	}
	if o != nil && o.reservation != nil && !op.same(operation{create: o.reservation.Operation}) {
		return nil, unavailable("create is pending")
	}
	return &pb.LookupOperationResponse{Observation: &pb.LookupOperationResponse_Absent{Absent: &emptypb.Empty{}}}, nil
}
func (w *World) ReserveCreate(_ context.Context, r *pb.ReserveCreateRequest) (*pb.ReserveCreateResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.controller(r.ControllerTerm); err != nil {
		return nil, err
	}
	op := operation{create: r.Operation}
	if !op.valid() || len(r.ReservationToken) == 0 || r.TargetWorkerId == "" {
		return nil, invalid("invalid create reservation")
	}
	id := r.Operation.ObjectId
	o := w.objects[id]
	res, err := o.lookup(op)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return &pb.ReserveCreateResponse{Outcome: &pb.ReserveCreateResponse_Completed{Completed: res}}, nil
	}
	if o != nil && o.code != nil {
		return nil, duroError(pb.ErrorCode_ERROR_CODE_OBJECT_ALREADY_EXISTS, "object exists")
	}
	if o != nil && o.reservation != nil {
		if !op.same(operation{create: o.reservation.Operation}) {
			return nil, unavailable("another create reservation is pending")
		}
		return &pb.ReserveCreateResponse{Outcome: &pb.ReserveCreateResponse_Reservation{Reservation: o.reservation}}, nil
	}
	if _, used := w.tokens[string(r.ReservationToken)]; used {
		return nil, stale("retired reservation token")
	}
	if w.packages[packageKey{r.Operation.Type, r.Operation.Version}] == nil {
		return nil, missing("code version is absent")
	}
	if o == nil {
		o = &object{receipts: make(map[string]receipt)}
		w.objects[id] = o
	}
	o.reservation = &pb.CreateReservation{Operation: r.Operation, ReservationToken: r.ReservationToken, Lease: w.grant(o, id, r.TargetWorkerId)}
	w.tokens[string(r.ReservationToken)] = tokenRecord{reservation: o.reservation}
	return &pb.ReserveCreateResponse{Outcome: &pb.ReserveCreateResponse_Reservation{Reservation: o.reservation}}, nil
}
func (w *World) AbandonCreate(_ context.Context, r *pb.AbandonCreateRequest) (*pb.AbandonCreateResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.controller(r.ControllerTerm); err != nil {
		return nil, err
	}
	token := string(r.ReservationToken)
	rec, ok := w.tokens[token]
	if !ok || !proto.Equal(rec.reservation.Lease.Owner, r.Owner) || rec.reservation.Operation.RequestId != r.RequestId {
		return nil, stale("reservation no longer matches")
	}
	if rec.abandoned {
		return &pb.AbandonCreateResponse{}, nil
	}
	o := w.objects[r.Owner.ObjectId]
	if o.reservation == nil || !bytes.Equal(o.reservation.ReservationToken, r.ReservationToken) {
		return nil, stale("reservation already resolved")
	}
	o.reservation = nil
	o.lease = nil
	o.activated = false
	rec.abandoned = true
	w.tokens[token] = rec
	return &pb.AbandonCreateResponse{}, nil
}
