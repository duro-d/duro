package contracttest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"testing"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type worker struct {
	pb.UnimplementedWorkerServiceServer
}

func (worker) ValidateCode(_ context.Context, req *pb.ValidateCodeRequest) (*pb.ValidateCodeResponse, error) {
	ref := req.GetCodePackage().GetManifest().GetCode()
	digest := sha256.Sum256(req.GetCodePackage().GetBundle())
	if ref == nil || !bytes.Equal(ref.GetSha256(), digest[:]) {
		s, err := status.New(codes.InvalidArgument, "bundle digest mismatch").WithDetails(&pb.ErrorDetail{Code: pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT})
		if err != nil {
			return nil, err
		}
		return nil, s.Err()
	}
	return &pb.ValidateCodeResponse{Validated: ref}, nil
}

type state struct {
	pb.UnimplementedPlacementServiceServer
}

func (state) ReadObject(_ context.Context, req *pb.ReadObjectRequest) (*pb.ReadObjectResponse, error) {
	if req.GetObjectId() != "auction/1" {
		return nil, status.Error(codes.NotFound, "unknown object")
	}
	return &pb.ReadObjectResponse{Kv: map[string]*pb.JsonValue{
		"winner": {Utf8: []byte("null")},
		"bid":    {Utf8: []byte("9007199254740993")},
	}}, nil
}

// Exercise both generated clients and servers, including protobuf serialization
// and gRPC status details. The handlers provide transport fixtures for routing.
func TestRoutingWire(t *testing.T) {
	socket := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterWorkerServiceServer(server, worker{})
	pb.RegisterPlacementServiceServer(server, state{})
	go func() { _ = server.Serve(socket) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///routing-test", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return socket.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bundle := []byte("export const methods = {};")
	hash := sha256.Sum256(bundle)
	ref := &pb.CodeRef{Type: "auction", Version: "1", Sha256: hash[:]}
	req := &pb.ValidateCodeRequest{CodePackage: &pb.CodePackage{
		Bundle: bundle, Manifest: &pb.CodeManifest{Code: ref, Format: "esm", Runtime: "javascript-v8"},
	}}
	client := pb.NewWorkerServiceClient(conn)
	validated, err := client.ValidateCode(ctx, req)
	if err != nil || !proto.Equal(validated.GetValidated(), ref) {
		t.Fatalf("validation = %v, %v", validated, err)
	}
	req.CodePackage.Bundle = []byte("changed")
	_, err = client.ValidateCode(ctx, req)
	s := status.Convert(err)
	if s.Code() != codes.InvalidArgument || len(s.Details()) != 1 {
		t.Fatalf("expected a typed invalid-argument error, got %v", s)
	}
	detail, ok := s.Details()[0].(*pb.ErrorDetail)
	if !ok || detail.GetCode() != pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Fatalf("unexpected error details %v", s.Details())
	}
	obj, err := pb.NewPlacementServiceClient(conn).ReadObject(ctx, &pb.ReadObjectRequest{ObjectId: "auction/1"})
	if err != nil {
		t.Fatal(err)
	}
	_, missing := obj.Kv["missing"]
	if missing || string(obj.Kv["winner"].GetUtf8()) != "null" || string(obj.Kv["bid"].GetUtf8()) != "9007199254740993" {
		t.Fatalf("unexpected JSON fields %v", obj.Kv)
	}
}
