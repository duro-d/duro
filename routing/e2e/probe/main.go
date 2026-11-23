// Command probe checks that a returned worker rejects a late request at an old epoch.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	addr := flag.String("address", "", "fixture worker or state gRPC address")
	term := flag.Uint64("old-term", 0, "verify a previous leader is fenced by storage")
	id := flag.String("worker-id", "", "original worker incarnation")
	obj := flag.String("object", "q", "fixture quota ID")
	epoch := flag.Uint64("epoch", 0, "revoked generation")
	flag.Parse()
	conn, err := grpc.NewClient("passthrough:///"+*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	want := pb.ErrorCode_ERROR_CODE_STALE_OWNER
	if *term != 0 {
		want = pb.ErrorCode_ERROR_CODE_STALE_CONTROLLER
		_, err = pb.NewPlacementServiceClient(conn).RegisterControllerTerm(ctx, &pb.RegisterControllerTermRequest{ControllerTerm: *term})
	} else {
		_, err = pb.NewWorkerServiceClient(conn).InvokeObject(ctx, &pb.InvokeObjectRequest{
			Owner:     &pb.Owner{WorkerId: *id, ObjectId: *obj, Epoch: *epoch},
			Operation: &pb.InvokeOperation{ObjectId: *obj, RequestId: "late-old-owner", Method: "spend", Argument: &pb.JsonValue{Utf8: []byte(`{"amount":1}`)}},
		})
	}
	if err != nil {
		for _, d := range status.Convert(err).Details() {
			if v, ok := d.(*pb.ErrorDetail); ok && v.Code == want {
				return
			}
		}
	}
	fmt.Fprintf(os.Stderr, "expected %v, got %v\n", want, err)
	os.Exit(1)
}
