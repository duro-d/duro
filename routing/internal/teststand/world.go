// Package teststand models confirmed state and fixture workers in a single process.
// Its mutex is the serialization point for commits, term changes and lease fences.
package teststand

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"
	"sync"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/jsonvalue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type packageKey struct{ typ, version string }
type operation struct {
	create *pb.CreateOperation
	invoke *pb.InvokeOperation
}

func (o operation) valid() bool {
	if p := o.create; p != nil {
		return p.ObjectId != "" && p.RequestId != "" && p.Type != "" && p.Version != "" && jsonvalue.Valid(p.Input)
	}
	p := o.invoke
	return p != nil && p.ObjectId != "" && p.RequestId != "" && p.Method != "" && jsonvalue.Valid(p.Argument)
}

func (o operation) ids() (string, string) {
	if o.create != nil {
		return o.create.ObjectId, o.create.RequestId
	}
	return o.invoke.ObjectId, o.invoke.RequestId
}

func (o operation) same(other operation) bool {
	if a, b := o.create, other.create; a != nil && b != nil {
		return a.ObjectId == b.ObjectId && a.RequestId == b.RequestId && a.Type == b.Type && a.Version == b.Version && jsonvalue.Equal(a.Input, b.Input)
	}
	a, b := o.invoke, other.invoke
	return a != nil && b != nil && a.ObjectId == b.ObjectId && a.RequestId == b.RequestId && a.Method == b.Method && jsonvalue.Equal(a.Argument, b.Argument)
}

type receipt struct {
	op     operation
	result *pb.OperationResult
}
type object struct {
	epoch              uint64
	lease              *pb.OwnerLease
	reservation        *pb.CreateReservation
	transfer           *pb.PlacementTransfer
	replaces           string
	activated, drained bool
	code               *pb.CodePackage
	kv                 map[string]*pb.JsonValue
	receipts           map[string]receipt
}

func (o *object) pending() bool {
	return o.transfer != nil && o.transfer.Phase != pb.TransferPhase_TRANSFER_PHASE_COMPLETE
}

func (o *object) lookup(op operation) (*pb.OperationResult, error) {
	if !op.valid() {
		return nil, invalid("incomplete operation or invalid JSON")
	}
	if o == nil {
		return nil, nil
	}
	_, id := op.ids()
	if saved, ok := o.receipts[id]; ok {
		if !op.same(saved.op) {
			return nil, duroError(pb.ErrorCode_ERROR_CODE_REQUEST_ID_CONFLICT, "request ID was used with different parameters")
		}
		return saved.result, nil
	}
	return nil, nil
}

type tokenRecord struct {
	reservation *pb.CreateReservation
	abandoned   bool
}

type World struct {
	pb.UnimplementedPlacementServiceServer
	mu          sync.Mutex
	now         func() time.Time
	ttl         time.Duration
	term        uint64
	objects     map[string]*object
	packages    map[packageKey]*pb.CodePackage
	tokens      map[string]tokenRecord
	transitions map[string]bool
	faults      map[faultKey]int
	workers     map[string]*Worker
}

func New(ttl time.Duration) *World {
	return &World{ttl: ttl, now: time.Now, objects: make(map[string]*object),
		packages: make(map[packageKey]*pb.CodePackage), tokens: make(map[string]tokenRecord),
		transitions: make(map[string]bool), faults: make(map[faultKey]int), workers: make(map[string]*Worker)}
}

func (w *World) grant(o *object, id, worker string) *pb.OwnerLease {
	o.epoch++
	o.lease = &pb.OwnerLease{Owner: &pb.Owner{ObjectId: id, WorkerId: worker, Epoch: o.epoch}, LeaseUntil: timestamppb.New(w.now().Add(w.ttl))}
	o.activated = false
	o.drained = false
	return o.lease
}

func (w *World) live(l *pb.OwnerLease) bool { return l != nil && l.LeaseUntil.AsTime().After(w.now()) }

func (w *World) controller(term uint64) error {
	if term == 0 || term != w.term {
		return duroError(pb.ErrorCode_ERROR_CODE_STALE_CONTROLLER, "controller term is stale")
	}
	return w.confirmed()
}

func validate(p *pb.CodePackage) error {
	m := p.GetManifest()
	c := m.GetCode()
	if c.GetType() == "" || c.GetVersion() == "" || m.GetRuntime() != "javascript-v8" || m.GetFormat() != "esm" || len(p.GetBundle()) == 0 {
		return invalid("incomplete code manifest or bundle")
	}
	if proto.Size(p) > 1<<20 {
		return duroError(pb.ErrorCode_ERROR_CODE_LIMIT_EXCEEDED, "code package exceeds 1 MiB")
	}
	digest := sha256.Sum256(p.Bundle)
	if !bytes.Equal(digest[:], c.Sha256) {
		return invalid("bundle digest mismatch")
	}
	methods := slices.Clone(m.Methods)
	slices.Sort(methods)
	for i, name := range methods {
		if name == "" || (i > 0 && methods[i-1] == name) {
			return invalid("invalid method name or duplicate")
		}
	}
	return nil
}

func duroError(c pb.ErrorCode, msg string) error {
	code := codes.FailedPrecondition
	switch c {
	case pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT:
		code = codes.InvalidArgument
	case pb.ErrorCode_ERROR_CODE_NOT_FOUND:
		code = codes.NotFound
	case pb.ErrorCode_ERROR_CODE_LIMIT_EXCEEDED, pb.ErrorCode_ERROR_CODE_OVERLOADED:
		code = codes.ResourceExhausted
	case pb.ErrorCode_ERROR_CODE_EXECUTION_TIMEOUT:
		code = codes.DeadlineExceeded
	case pb.ErrorCode_ERROR_CODE_CODE_VERSION_CONFLICT, pb.ErrorCode_ERROR_CODE_REQUEST_ID_CONFLICT, pb.ErrorCode_ERROR_CODE_OBJECT_ALREADY_EXISTS:
		code = codes.AlreadyExists
	case pb.ErrorCode_ERROR_CODE_UNAVAILABLE:
		code = codes.Unavailable
	}
	s, err := status.New(code, msg).WithDetails(&pb.ErrorDetail{Code: c})
	if err != nil {
		panic(fmt.Sprintf("error detail: %v", err))
	}
	return s.Err()
}
func invalid(s string) error     { return duroError(pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, s) }
func unavailable(s string) error { return duroError(pb.ErrorCode_ERROR_CODE_UNAVAILABLE, s) }
func stale(s string) error       { return duroError(pb.ErrorCode_ERROR_CODE_STALE_OWNER, s) }
func missing(s string) error     { return duroError(pb.ErrorCode_ERROR_CODE_NOT_FOUND, s) }
