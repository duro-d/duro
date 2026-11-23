package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type storageStub struct {
	pb.PlacementServiceClient
	get      func(context.Context, string) (*pb.GetOwnerResponse, error)
	lookup   func(*pb.LookupOperationRequest) (*pb.LookupOperationResponse, error)
	register func(*pb.CodePackage) (*pb.RegisterTypeResponse, error)
	read     func(context.Context, string) (*pb.ReadObjectResponse, error)
	describe func(string) (*pb.DescribeObjectResponse, error)
}

func (s storageStub) GetOwner(ctx context.Context, q *pb.GetOwnerRequest, _ ...grpc.CallOption) (*pb.GetOwnerResponse, error) {
	return s.get(ctx, q.ObjectId)
}
func (s storageStub) LookupOperation(_ context.Context, q *pb.LookupOperationRequest, _ ...grpc.CallOption) (*pb.LookupOperationResponse, error) {
	return s.lookup(q)
}
func (s storageStub) RegisterType(_ context.Context, q *pb.RegisterTypeRequest, _ ...grpc.CallOption) (*pb.RegisterTypeResponse, error) {
	return s.register(q.Code)
}
func (s storageStub) ReadObject(ctx context.Context, q *pb.ReadObjectRequest, _ ...grpc.CallOption) (*pb.ReadObjectResponse, error) {
	return s.read(ctx, q.ObjectId)
}
func (s storageStub) DescribeObject(_ context.Context, q *pb.DescribeObjectRequest, _ ...grpc.CallOption) (*pb.DescribeObjectResponse, error) {
	return s.describe(q.ObjectId)
}

type workerStub struct {
	pb.WorkerServiceClient
	validate func(*pb.CodePackage) (*pb.ValidateCodeResponse, error)
	create   func(*pb.CreateObjectRequest) (*pb.CreateObjectResponse, error)
	invoke   func(context.Context, *pb.InvokeObjectRequest) (*pb.InvokeObjectResponse, error)
}

func (w workerStub) ValidateCode(_ context.Context, q *pb.ValidateCodeRequest, _ ...grpc.CallOption) (*pb.ValidateCodeResponse, error) {
	return w.validate(q.CodePackage)
}
func (w workerStub) ActivateObject(_ context.Context, q *pb.ActivateObjectRequest, _ ...grpc.CallOption) (*pb.ActivateObjectResponse, error) {
	return &pb.ActivateObjectResponse{Owner: q.Owner}, nil
}
func (w workerStub) CreateObject(_ context.Context, q *pb.CreateObjectRequest, _ ...grpc.CallOption) (*pb.CreateObjectResponse, error) {
	return w.create(q)
}
func (w workerStub) InvokeObject(ctx context.Context, q *pb.InvokeObjectRequest, _ ...grpc.CallOption) (*pb.InvokeObjectResponse, error) {
	return w.invoke(ctx, q)
}

type backendStub struct {
	worker  workerStub
	reserve func(*pb.CreateOperation) (*pb.ReserveCreateResponse, error)
}

func (b backendStub) Worker(context.Context, string) (pb.WorkerServiceClient, error) {
	return b.worker, nil
}
func (b backendStub) Candidate(context.Context, string) (string, error) { return "w1", nil }
func (b backendStub) Create(_ context.Context, q *pb.CreateOperation) (*pb.ReserveCreateResponse, error) {
	return b.reserve(q)
}

func rpcError(c codes.Code, d pb.ErrorCode) error {
	s, err := status.New(c, "private internal error").WithDetails(&pb.ErrorDetail{Code: d})
	if err != nil {
		panic(err)
	}
	return s.Err()
}
func jsonValue(s string) *pb.JsonValue { return &pb.JsonValue{Utf8: []byte(s)} }
func outcome(s string) *pb.OperationResult {
	return &pb.OperationResult{Outcome: &pb.OperationResult_Result{Result: jsonValue(s)}}
}
func active(id string, epoch uint64) *pb.GetOwnerResponse {
	return &pb.GetOwnerResponse{Assignment: &pb.GetOwnerResponse_Active{Active: &pb.OwnerLease{Owner: &pb.Owner{ObjectId: id, WorkerId: "w", Epoch: epoch}}}}
}

const invokeBody = `{"object_id":"a/b","request_id":"k","method":"inc","argument":{"n":9007199254740993}}`
const createBody = `{"object_id":"a/b","request_id":"c","type":"T","version":"1","input":{"amount":10,"rate":0.5}}`

func post(t *testing.T, h http.Handler, op, body string, want int) map[string]json.RawMessage {
	t.Helper()
	r := httptest.NewRequest("POST", "/v1/"+op, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("%s: status %d, want %d: %s", op, w.Code, want, w.Body)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestPublicOperations(t *testing.T) {
	bundle := []byte("export const methods = {}")
	sum := sha256.Sum256(bundle)
	registerBody := fmt.Sprintf(`{"manifest":{"type":"T","version":"1","runtime":"javascript-v8","format":"esm","methods":[],"sha256":"%x"},"bundle":"%s"}`, sum, base64.StdEncoding.EncodeToString(bundle))
	seenPackage := false
	s := storageStub{
		get: func(_ context.Context, id string) (*pb.GetOwnerResponse, error) { return active(id, 2), nil },
		register: func(pkg *pb.CodePackage) (*pb.RegisterTypeResponse, error) {
			if !seenPackage {
				t.Fatal("unvalidated registration")
			}
			return &pb.RegisterTypeResponse{Code: pkg.Manifest.Code}, nil
		},
		read: func(context.Context, string) (*pb.ReadObjectResponse, error) {
			return &pb.ReadObjectResponse{Kv: map[string]*pb.JsonValue{"a/b": jsonValue("null"), "n": jsonValue("9007199254740993")}}, nil
		},
		describe: func(id string) (*pb.DescribeObjectResponse, error) {
			return &pb.DescribeObjectResponse{ObjectId: id, Code: &pb.CodeRef{Type: "T", Version: "1", Sha256: sum[:]}}, nil
		},
	}
	b := backendStub{worker: workerStub{
		validate: func(pkg *pb.CodePackage) (*pb.ValidateCodeResponse, error) {
			if string(pkg.Bundle) != string(bundle) {
				t.Fatal("bundle changed")
			}
			seenPackage = true
			return &pb.ValidateCodeResponse{Validated: pkg.Manifest.Code}, nil
		},
		create: func(q *pb.CreateObjectRequest) (*pb.CreateObjectResponse, error) {
			if string(q.ReservationToken) != "token" || q.Operation.RequestId != "c" || q.Owner.Epoch != 2 {
				t.Fatalf("create %v", q)
			}
			return &pb.CreateObjectResponse{Owner: q.Owner, Outcome: outcome("null")}, nil
		},
		invoke: func(_ context.Context, q *pb.InvokeObjectRequest) (*pb.InvokeObjectResponse, error) {
			if string(q.Operation.Argument.Utf8) != `{"n":9007199254740993}` || q.Owner.Epoch != 2 {
				t.Fatalf("invoke %v", q)
			}
			return &pb.InvokeObjectResponse{Owner: q.Owner, Outcome: outcome("9007199254740993")}, nil
		},
	}, reserve: func(op *pb.CreateOperation) (*pb.ReserveCreateResponse, error) {
		copy := proto.CloneOf(op)
		copy.Input = jsonValue(`{"rate":5e-1,"amount":10.00}`)
		return &pb.ReserveCreateResponse{Outcome: &pb.ReserveCreateResponse_Reservation{Reservation: &pb.CreateReservation{Operation: copy, ReservationToken: []byte("token"), Lease: active(op.ObjectId, 2).GetActive()}}}, nil
	}}
	var event Event
	h := New(Config{Token: "secret", OnRequest: func(e Event) { event = e }}, s, b)
	if got := post(t, h, "registerType", registerBody, 200); string(got["sha256"]) != fmt.Sprintf("%q", fmt.Sprintf("%x", sum)) {
		t.Fatal(got)
	}
	if got := post(t, h, "create", createBody, 200); string(got["object_id"]) != `"a/b"` || string(got["result"]) != "null" {
		t.Fatal(got)
	}
	if got := post(t, h, "invoke", invokeBody, 200); string(got["result"]) != "9007199254740993" {
		t.Fatal(got)
	}
	if event.WorkerID != "w" || event.Epoch != 2 || event.RequestID != "k" || event.ObjectID != "a/b" || event.Operation != "invoke" || event.Code != "OK" || event.Duration <= 0 {
		t.Fatal(event)
	}
	h.backend = nil // These reads must not consult discovery or execution.
	if got := post(t, h, "describe", `{"object_id":"a/b"}`, 200); string(got["methods"]) != "[]" {
		t.Fatal(got)
	}
	if got := post(t, h, "read", `{"object_id":"a/b"}`, 200); string(got["kv"]) != `{"a/b":null,"n":9007199254740993}` {
		t.Fatal(got)
	}
}

func TestPublicErrors(t *testing.T) {
	for _, tc := range []struct {
		c    codes.Code
		d    pb.ErrorCode
		want int
		code string
	}{
		{codes.InvalidArgument, pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, 400, "INVALID_ARGUMENT"},
		{codes.Unauthenticated, pb.ErrorCode_ERROR_CODE_UNAUTHORIZED, 401, "UNAUTHORIZED"},
		{codes.NotFound, pb.ErrorCode_ERROR_CODE_NOT_FOUND, 404, "NOT_FOUND"},
		{codes.AlreadyExists, pb.ErrorCode_ERROR_CODE_OBJECT_ALREADY_EXISTS, 409, "OBJECT_ALREADY_EXISTS"},
		{codes.AlreadyExists, pb.ErrorCode_ERROR_CODE_CODE_VERSION_CONFLICT, 409, "CODE_VERSION_CONFLICT"},
		{codes.AlreadyExists, pb.ErrorCode_ERROR_CODE_REQUEST_ID_CONFLICT, 409, "REQUEST_ID_CONFLICT"},
		{codes.ResourceExhausted, pb.ErrorCode_ERROR_CODE_LIMIT_EXCEEDED, 413, "LIMIT_EXCEEDED"},
		{codes.ResourceExhausted, pb.ErrorCode_ERROR_CODE_OVERLOADED, 503, "OVERLOADED"},
		{codes.DeadlineExceeded, pb.ErrorCode_ERROR_CODE_EXECUTION_TIMEOUT, 504, "EXECUTION_TIMEOUT"},
		{codes.Unavailable, pb.ErrorCode_ERROR_CODE_UNAVAILABLE, 503, "UNAVAILABLE"},
		{codes.Internal, pb.ErrorCode_ERROR_CODE_INTERNAL_ERROR, 500, "INTERNAL_ERROR"},
		{codes.FailedPrecondition, pb.ErrorCode_ERROR_CODE_STALE_CONTROLLER, 503, "UNAVAILABLE"},
		{codes.Unknown, pb.ErrorCode_ERROR_CODE_UNSPECIFIED, 503, "UNAVAILABLE"},
		{codes.Canceled, pb.ErrorCode_ERROR_CODE_EXECUTION_TIMEOUT, 503, "UNAVAILABLE"},
	} {
		t.Run(tc.d.String()+tc.c.String(), func(t *testing.T) {
			s := storageStub{read: func(context.Context, string) (*pb.ReadObjectResponse, error) { return nil, rpcError(tc.c, tc.d) }}
			got := post(t, New(Config{Token: "secret"}, s, nil), "read", `{"object_id":"o"}`, tc.want)
			if string(got["code"]) != fmt.Sprintf("%q", tc.code) || strings.Contains(string(got["message"]), "private") {
				t.Fatal(got)
			}
		})
	}
}

func TestRejectInputBeforeRPC(t *testing.T) {
	h := New(Config{Token: "secret"}, nil, nil)
	for _, bad := range []string{`{}`, `null`, invokeBody + " false", strings.Replace(invokeBody, "9007199254740993", `0,"n":1`, 1), strings.Replace(invokeBody, `"argument"`, `"Argument"`, 1), strings.Replace(invokeBody, `"a/b"`, `null`, 1)} {
		post(t, h, "invoke", bad, 400)
	}
	post(t, h, "invoke", strings.Repeat(" ", maxRequest+1), 413)
	post(t, h, "registerType", strings.Repeat(" ", maxRegistration+1), 413)
	for _, timeout := range []string{"0", "-1", "nan", "18446744073709551616"} {
		r := httptest.NewRequest("POST", "/v1/invoke", strings.NewReader(invokeBody))
		r.Header.Set("Duro-Timeout-Ms", timeout)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatal(w.Code)
		}
		r.Header.Set("Authorization", "Bearer secret")
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
}

func TestRetryOnlyExplicitStaleOwner(t *testing.T) {
	for _, mode := range []string{"fresh", "twice", "lost", "wrong-epoch", "missing-outcome", "bad-json", "domain"} {
		t.Run(mode, func(t *testing.T) {
			var calls, lookups int
			s := storageStub{get: func(_ context.Context, id string) (*pb.GetOwnerResponse, error) {
				lookups++
				return active(id, uint64(lookups)), nil
			}}
			b := backendStub{worker: workerStub{invoke: func(ctx context.Context, q *pb.InvokeObjectRequest) (*pb.InvokeObjectResponse, error) {
				calls++
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 500*time.Millisecond || q.Operation.RequestId != "k" {
					t.Fatalf("deadline or ID changed: %v %s", deadline, q.Operation.RequestId)
				}
				if mode == "twice" || mode == "fresh" && calls == 1 {
					return nil, rpcError(codes.FailedPrecondition, pb.ErrorCode_ERROR_CODE_STALE_OWNER)
				}
				if mode == "lost" {
					return nil, status.Error(codes.DeadlineExceeded, "transport timeout")
				}
				reply := &pb.InvokeObjectResponse{Owner: proto.CloneOf(q.Owner), Outcome: outcome("null")}
				switch mode {
				case "wrong-epoch":
					reply.Owner.Epoch++
				case "missing-outcome":
					reply.Outcome = nil
				case "bad-json":
					reply.Outcome = outcome("{")
				case "domain":
					reply.Outcome = &pb.OperationResult{Outcome: &pb.OperationResult_DomainRejection{DomainRejection: "insufficient balance"}}
				}
				return reply, nil
			}}}
			h := New(Config{Token: "secret", Timeout: 500 * time.Millisecond}, s, b)
			want, wantCalls := 503, 1
			switch mode {
			case "fresh":
				want, wantCalls = 200, 2
			case "twice":
				wantCalls = 2
			case "domain":
				want = 422
			}
			got := post(t, h, "invoke", invokeBody, want)
			if calls != wantCalls || lookups != wantCalls {
				t.Fatalf("%d calls, %d lookups", calls, lookups)
			}
			if want == 422 && string(got["code"]) != `"DOMAIN_REJECTED"` {
				t.Fatal(got)
			}
		})
	}
}

func TestLookupOnMissingObjectAndUnassignedOwner(t *testing.T) {
	for _, mode := range []string{"absent", "unassigned", "invalid", "completed", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			s := storageStub{
				get: func(context.Context, string) (*pb.GetOwnerResponse, error) {
					if mode == "unassigned" {
						return &pb.GetOwnerResponse{Assignment: &pb.GetOwnerResponse_Unassigned{Unassigned: &emptypb.Empty{}}}, nil
					}
					if mode == "invalid" {
						return &pb.GetOwnerResponse{}, nil
					}
					return nil, rpcError(codes.NotFound, pb.ErrorCode_ERROR_CODE_NOT_FOUND)
				},
				lookup: func(q *pb.LookupOperationRequest) (*pb.LookupOperationResponse, error) {
					if q.GetInvoke().GetRequestId() != "k" {
						t.Fatal("lost lookup identity")
					}
					switch mode {
					case "absent":
						return &pb.LookupOperationResponse{Observation: &pb.LookupOperationResponse_Absent{Absent: &emptypb.Empty{}}}, nil
					case "completed":
						return &pb.LookupOperationResponse{Observation: &pb.LookupOperationResponse_Completed{Completed: outcome("42")}}, nil
					case "conflict":
						return nil, rpcError(codes.AlreadyExists, pb.ErrorCode_ERROR_CODE_REQUEST_ID_CONFLICT)
					default:
						t.Fatal("unexpected lookup")
						return nil, nil
					}
				},
			}
			want := map[string]int{"absent": 404, "unassigned": 503, "invalid": 503, "completed": 200, "conflict": 409}[mode]
			got := post(t, New(Config{Token: "secret"}, s, nil), "invoke", invokeBody, want)
			if mode == "completed" && string(got["result"]) != "42" {
				t.Fatal(got)
			}
		})
	}
}

func TestCreateReservationAndReplay(t *testing.T) {
	for _, mode := range []string{"empty", "missing-token", "different-input", "wrong-owner", "recorded-null", "recorded-rejection", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			b := backendStub{reserve: func(op *pb.CreateOperation) (*pb.ReserveCreateResponse, error) {
				calls++
				r := &pb.CreateReservation{Operation: proto.CloneOf(op), ReservationToken: []byte("token"), Lease: active(op.ObjectId, 1).GetActive()}
				switch mode {
				case "empty":
					return nil, nil
				case "missing-token":
					r.ReservationToken = nil
				case "different-input":
					r.Operation.Input = jsonValue(`{"amount":10.0000000000000000001,"rate":0.5}`)
				case "wrong-owner":
					r.Lease.Owner.ObjectId = "another-object"
				case "recorded-null":
					return &pb.ReserveCreateResponse{Outcome: &pb.ReserveCreateResponse_Completed{Completed: outcome("null")}}, nil
				case "recorded-rejection":
					return &pb.ReserveCreateResponse{Outcome: &pb.ReserveCreateResponse_Completed{Completed: &pb.OperationResult{Outcome: &pb.OperationResult_DomainRejection{DomainRejection: "refused"}}}}, nil
				case "unknown":
					return nil, status.Error(codes.Unavailable, "reply lost")
				}
				return &pb.ReserveCreateResponse{Outcome: &pb.ReserveCreateResponse_Reservation{Reservation: r}}, nil
			}}
			want := 503
			if mode == "recorded-null" {
				want = 200
			}
			if mode == "recorded-rejection" {
				want = 422
			}
			got := post(t, New(Config{Token: "secret"}, nil, b), "create", createBody, want)
			if want == 200 && (string(got["result"]) != "null" || string(got["object_id"]) != `"a/b"`) {
				t.Fatal(got)
			}
			if calls != 1 {
				t.Fatalf("reserved %d times", calls)
			}
		})
	}
}

func TestRegisterChecksValidatedAndStoredIdentity(t *testing.T) {
	digest := sha256.Sum256([]byte("a"))
	body := fmt.Sprintf(`{"manifest":{"type":"T","version":"1","runtime":"javascript-v8","format":"esm","methods":["m"],"sha256":"%x"},"bundle":"YQ=="}`, digest)
	for _, mode := range []string{"worker", "store", "digest", "methods"} {
		t.Run(mode, func(t *testing.T) {
			raw := body
			if mode == "digest" {
				raw = strings.Replace(raw, "YQ==", "Yg==", 1)
			}
			if mode == "methods" {
				raw = strings.Replace(raw, `["m"]`, `["m","m"]`, 1)
			}
			b := backendStub{worker: workerStub{validate: func(pkg *pb.CodePackage) (*pb.ValidateCodeResponse, error) {
				if mode == "digest" || mode == "methods" {
					t.Fatal("invalid manifest reached worker")
				}
				ref := proto.CloneOf(pkg.Manifest.Code)
				if mode == "worker" {
					ref.Version = "2"
				}
				return &pb.ValidateCodeResponse{Validated: ref}, nil
			}}}
			s := storageStub{register: func(pkg *pb.CodePackage) (*pb.RegisterTypeResponse, error) {
				if mode != "store" {
					t.Fatal("unvalidated package published")
				}
				ref := proto.CloneOf(pkg.Manifest.Code)
				ref.Sha256 = nil
				return &pb.RegisterTypeResponse{Code: ref}, nil
			}}
			want := 503
			if mode == "digest" || mode == "methods" {
				want = 400
			}
			post(t, New(Config{Token: "secret"}, s, b), "registerType", raw, want)
		})
	}
}
