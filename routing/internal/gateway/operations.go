package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/jsonvalue"
	"google.golang.org/protobuf/proto"
)

func validMethods(methods []string) bool {
	seen := make(map[string]bool, len(methods))
	for _, m := range methods {
		if m == "" || seen[m] {
			return false
		}
		seen[m] = true
	}
	return true
}

func (h *Handler) register(ctx context.Context, e *Event, body []byte) (any, error) {
	f, err := parseFields(body, "manifest", "bundle")
	if err != nil {
		return nil, err
	}
	m, err := parseFields(f["manifest"], "type", "version", "runtime", "format", "methods", "sha256")
	if err != nil {
		return nil, err
	}
	bundle, decodeErr := base64.StdEncoding.Strict().DecodeString(f.str("bundle"))
	var methods []string
	if decodeErr != nil || len(bundle) == 0 ||
		json.Unmarshal(m["methods"], &methods) != nil || methods == nil || !validMethods(methods) {
		return nil, invalid
	}
	if len(bundle)+len(f["manifest"]) > maxPackage {
		return nil, limitExceeded
	}
	digest := sha256.Sum256(bundle)
	given, err := hex.DecodeString(m.str("sha256"))
	ref := &pb.CodeRef{Type: m.str("type"), Version: m.str("version"), Sha256: digest[:]}
	if err != nil || !bytes.Equal(given, digest[:]) || ref.Type == "" || ref.Version == "" || m.str("runtime") != "javascript-v8" || m.str("format") != "esm" {
		return nil, invalid
	}
	pkg := &pb.CodePackage{Manifest: &pb.CodeManifest{Code: ref, Runtime: "javascript-v8", Format: "esm", Methods: methods}, Bundle: bundle}
	id, err := h.backend.Candidate(ctx, ref.Type)
	if err != nil {
		return nil, err
	}
	if id == "" {
		return nil, unavailable
	}
	e.WorkerID = id
	worker, err := h.backend.Worker(ctx, id)
	if err != nil {
		return nil, err
	}
	if worker == nil {
		return nil, unavailable
	}
	verified, err := worker.ValidateCode(ctx, &pb.ValidateCodeRequest{CodePackage: pkg})
	if err != nil {
		return nil, err
	}
	if !proto.Equal(verified.GetValidated(), ref) {
		return nil, unavailable
	}
	stored, err := h.placement.RegisterType(ctx, &pb.RegisterTypeRequest{Code: pkg})
	if err != nil {
		return nil, err
	}
	if !proto.Equal(stored.GetCode(), ref) {
		return nil, unavailable
	}
	return map[string]string{"type": ref.Type, "version": ref.Version, "sha256": hex.EncodeToString(digest[:])}, nil
}

func (h *Handler) read(ctx context.Context, id string) (any, error) {
	res, err := h.placement.ReadObject(ctx, &pb.ReadObjectRequest{ObjectId: id})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, unavailable
	}
	kv := make(map[string]json.RawMessage, len(res.Kv))
	total := 0
	for k, v := range res.Kv {
		total += len(k) + len(v.GetUtf8())
		if len(k) > 256 || total > maxPackage || v == nil || !jsonvalue.Valid(v) {
			return nil, unavailable
		}
		kv[k] = v.Utf8
	}
	return map[string]any{"kv": kv}, nil
}

func (h *Handler) describe(ctx context.Context, id string) (any, error) {
	res, err := h.placement.DescribeObject(ctx, &pb.DescribeObjectRequest{ObjectId: id})
	if err != nil {
		return nil, err
	}
	ref := res.GetCode()
	if res.GetObjectId() != id || ref.GetType() == "" || ref.GetVersion() == "" || len(ref.GetSha256()) != sha256.Size || !validMethods(res.GetMethods()) {
		return nil, unavailable
	}
	methods := res.GetMethods()
	if methods == nil {
		methods = []string{}
	}
	return map[string]any{"object_id": id, "type": ref.Type, "version": ref.Version, "methods": methods}, nil
}

func result(out *pb.OperationResult, createID string) (any, error) {
	switch o := out.GetOutcome().(type) {
	case *pb.OperationResult_DomainRejection:
		return nil, &apiError{"DOMAIN_REJECTED", o.DomainRejection, http.StatusUnprocessableEntity}
	case *pb.OperationResult_Result:
		if o.Result != nil && len(o.Result.Utf8) <= maxRequest && jsonvalue.Valid(o.Result) {
			reply := map[string]any{"result": json.RawMessage(o.Result.Utf8)}
			if createID != "" {
				reply["object_id"] = createID
			}
			return reply, nil
		}
	}
	return nil, unavailable
}

func sameCreate(a, b *pb.CreateOperation) bool {
	return a != nil && b != nil && a.ObjectId == b.ObjectId && a.RequestId == b.RequestId && a.Type == b.Type && a.Version == b.Version && jsonvalue.Equal(a.Input, b.Input)
}

func (h *Handler) create(ctx context.Context, e *Event, op *pb.CreateOperation) (any, error) {
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reply, err := h.backend.Create(ctx, op)
		if err != nil {
			return nil, err
		}
		if completed := reply.GetCompleted(); completed != nil {
			return result(completed, op.ObjectId)
		}
		res := reply.GetReservation()
		r, err := ownerRoute(res.GetLease().GetOwner(), op.ObjectId)
		if err != nil || len(res.GetReservationToken()) == 0 || !sameCreate(res.GetOperation(), op) {
			return nil, unavailable
		}
		e.WorkerID, e.Epoch = r.workerID, r.epoch
		out, err := h.initialize(ctx, r, op, res.ReservationToken)
		if err == nil {
			return result(out, op.ObjectId)
		}
		if publicError(err) != staleOwner {
			return nil, err
		}
	}
	return nil, staleOwner
}

func (h *Handler) initialize(ctx context.Context, r route, op *pb.CreateOperation, token []byte) (*pb.OperationResult, error) {
	w, err := h.backend.Worker(ctx, r.workerID)
	if err != nil {
		return nil, err
	}
	if w == nil {
		return nil, unavailable
	}
	owner := r.owner()
	activation, err := w.ActivateObject(ctx, &pb.ActivateObjectRequest{Owner: owner})
	if err != nil {
		return nil, err
	}
	if !proto.Equal(activation.GetOwner(), owner) || activation.TransitionId != nil {
		return nil, unavailable
	}
	reply, err := w.CreateObject(ctx, &pb.CreateObjectRequest{Owner: owner, Operation: op, ReservationToken: token})
	if err != nil {
		return nil, err
	}
	if !proto.Equal(reply.GetOwner(), owner) {
		return nil, unavailable
	}
	return reply.GetOutcome(), nil
}

func (h *Handler) invoke(ctx context.Context, e *Event, op *pb.InvokeOperation) (any, error) {
	for attempt := 0; attempt < 2; attempt++ {
		r, err := h.routes.resolve(ctx, op.ObjectId)
		if err != nil {
			if publicError(err) == notFound {
				return h.lookupMissing(ctx, op)
			}
			return nil, err
		}
		e.WorkerID, e.Epoch = r.workerID, r.epoch
		w, err := h.backend.Worker(ctx, r.workerID)
		if err != nil || w == nil {
			h.routes.invalidate(r, false)
			if err != nil {
				return nil, err
			}
			return nil, unavailable
		}
		owner := r.owner()
		reply, err := w.InvokeObject(ctx, &pb.InvokeObjectRequest{Owner: owner, Operation: op})
		if err == nil {
			if !proto.Equal(reply.GetOwner(), owner) {
				h.routes.invalidate(r, false)
				return nil, unavailable
			}
			return result(reply.GetOutcome(), "")
		}
		public := publicError(err)
		if public == staleOwner {
			h.routes.invalidate(r, true)
			continue
		}
		if public == unavailable {
			h.routes.invalidate(r, false)
		}
		return nil, err
	}
	return nil, staleOwner
}

func (h *Handler) lookupMissing(ctx context.Context, op *pb.InvokeOperation) (any, error) {
	reply, err := h.placement.LookupOperation(ctx, &pb.LookupOperationRequest{Operation: &pb.LookupOperationRequest_Invoke{Invoke: op}})
	if err != nil {
		return nil, err
	}
	if reply.GetAbsent() != nil {
		return nil, notFound
	}
	return result(reply.GetCompleted(), "")
}
