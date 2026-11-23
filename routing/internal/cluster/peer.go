package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"crypto/subtle"
	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type PlacementController interface {
	IsLeader() bool
	Candidate(context.Context, string) (string, error)
	Create(context.Context, *pb.CreateOperation) (*pb.ReserveCreateResponse, error)
	Assign(context.Context, string) (*pb.OwnerLease, error)
	Move(context.Context, string, string) error
}

type Peers struct {
	*Directory
	ctrl   PlacementController
	token  string
	client *http.Client
}

func NewPeers(directory *Directory, ctrl PlacementController, token string) *Peers {
	return &Peers{Directory: directory, ctrl: ctrl, token: token, client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (p *Peers) Candidate(ctx context.Context, id string) (string, error) {
	return p.ctrl.Candidate(ctx, id)
}
func (p *Peers) Create(ctx context.Context, op *pb.CreateOperation) (*pb.ReserveCreateResponse, error) {
	if p.ctrl.IsLeader() {
		return p.ctrl.Create(ctx, op)
	}
	var result pb.ReserveCreateResponse
	err := p.call(ctx, "create", op, &result)
	return &result, err
}
func (p *Peers) Assign(ctx context.Context, id string) (*pb.OwnerLease, error) {
	if p.ctrl.IsLeader() {
		return p.ctrl.Assign(ctx, id)
	}
	var result pb.OwnerLease
	err := p.call(ctx, "assign", &pb.GetOwnerRequest{ObjectId: id}, &result)
	return &result, err
}

const peerBodyLimit = 2 << 20

func (p *Peers) call(ctx context.Context, route string, req, reply proto.Message) error {
	address, err := Leader(ctx, p.etcd)
	if err != nil {
		return status.Error(codes.Unavailable, "controller unavailable")
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid controller request")
	}
	msg, err := http.NewRequestWithContext(ctx, http.MethodPost, address+"/internal/"+route, bytes.NewReader(data))
	if err != nil {
		return status.Error(codes.Unavailable, "invalid controller address")
	}
	msg.Header.Set("Authorization", "Bearer "+p.token)
	msg.Header.Set("Content-Type", "application/protobuf")
	if until, ok := ctx.Deadline(); ok {
		msg.Header.Set("Duro-Timeout-Ms", strconv.FormatInt(max(1, time.Until(until).Milliseconds()), 10))
	}
	out, err := p.client.Do(msg)
	if err != nil {
		return status.Error(codes.Unavailable, "controller request interrupted")
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, peerBodyLimit+1))
	if err != nil || len(body) > peerBodyLimit {
		return status.Error(codes.Unavailable, "invalid controller reply")
	}
	if out.StatusCode != http.StatusOK {
		var s statuspb.Status
		if proto.Unmarshal(body, &s) != nil || s.Code == 0 {
			return status.Error(codes.Unavailable, "controller request failed")
		}
		return status.ErrorProto(&s)
	}
	if proto.Unmarshal(body, reply) != nil {
		return status.Error(codes.Unavailable, "invalid controller reply")
	}
	return nil
}

// Internal serves other router instances on the private listener. It never forwards.
func (p *Peers) Internal() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+p.token)) != 1 {
			w.WriteHeader(401)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(405)
			return
		}
		timeout := 5 * time.Second
		if raw := r.Header.Get("Duro-Timeout-Ms"); raw != "" {
			ms, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || ms < 1 {
				w.WriteHeader(400)
				return
			}
			timeout = time.Duration(min(ms, timeout.Milliseconds())) * time.Millisecond
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		buf, err := io.ReadAll(http.MaxBytesReader(w, r.Body, peerBodyLimit))
		if err != nil {
			w.WriteHeader(413)
			return
		}
		w.Header().Set("Content-Type", "application/protobuf")
		var result proto.Message
		switch r.URL.Path {
		case "/internal/create":
			var op pb.CreateOperation
			if proto.Unmarshal(buf, &op) != nil || op.ObjectId == "" || op.RequestId == "" {
				w.WriteHeader(400)
				return
			}
			result, err = p.ctrl.Create(ctx, &op)
		case "/internal/assign":
			var in pb.GetOwnerRequest
			if proto.Unmarshal(buf, &in) != nil || in.ObjectId == "" {
				w.WriteHeader(400)
				return
			}
			result, err = p.ctrl.Assign(ctx, in.ObjectId)
		case "/admin/move":
			var move struct {
				ObjectID string `json:"object_id"`
				Target   string `json:"target"`
			}
			if json.Unmarshal(buf, &move) != nil || move.ObjectID == "" || move.Target == "" {
				w.WriteHeader(400)
				return
			}
			err = p.ctrl.Move(ctx, move.ObjectID, move.Target)
			if err == nil {
				w.WriteHeader(204)
				return
			}
			http.Error(w, status.Convert(err).Message(), http.StatusServiceUnavailable)
			return
		default:
			http.NotFound(w, r)
			return
		}
		if err != nil {
			s := status.Convert(err)
			data, _ := proto.Marshal(s.Proto())
			w.WriteHeader(503)
			_, _ = w.Write(data)
			return
		}
		data, err := proto.Marshal(result)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		_, _ = w.Write(data)
	})
}
