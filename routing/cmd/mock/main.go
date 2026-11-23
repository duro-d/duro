// Command mock runs the two routing contracts against one in-memory fixture.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/teststand"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	state := flag.String("state-listen", "127.0.0.1:7100", "placement gRPC address")
	workers := flag.String("workers", "a=127.0.0.1:7101,b=127.0.0.1:7102", "comma-separated worker_id=listen_address entries")
	admin := flag.String("admin-listen", "127.0.0.1:7109", "loopback HTTP address for test faults; empty disables")
	ttl := flag.Duration("lease", 9*time.Second, "ownership TTL on the fixture storage clock")
	etcd := flag.String("etcd", "", "optional comma-separated etcd discovery endpoints")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *state, *workers, *admin, *etcd, *ttl); err != nil {
		log.Fatal(err)
	}
}

func run(parent context.Context, state, workers, admin, etcd string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("lease must be positive")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	w := teststand.New(ttl)
	results := make(chan error, strings.Count(workers, ",")+3)
	var servers []*grpc.Server
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()
	serve := func(address, name string, register func(*grpc.Server)) (string, error) {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return "", err
		}
		s := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
		servers = append(servers, s)
		register(s)
		h := health.NewServer()
		h.SetServingStatus(name, healthpb.HealthCheckResponse_SERVING)
		healthpb.RegisterHealthServer(s, h)
		go func() { results <- s.Serve(listener) }()
		return listener.Addr().String(), nil
	}
	if _, err := serve(state, pb.PlacementService_ServiceDesc.ServiceName, func(s *grpc.Server) { pb.RegisterPlacementServiceServer(s, w) }); err != nil {
		return err
	}
	addresses := make(map[string]string)
	for _, entry := range strings.Split(workers, ",") {
		id, address, ok := strings.Cut(entry, "=")
		if !ok || id == "" || address == "" || strings.ContainsAny(id, "/ \t\r\n") || addresses[id] != "" {
			return fmt.Errorf("invalid or duplicate worker entry %q", entry)
		}
		worker := w.Worker(id)
		bound, err := serve(address, pb.WorkerService_ServiceDesc.ServiceName, func(s *grpc.Server) { pb.RegisterWorkerServiceServer(s, worker) })
		if err != nil {
			return err
		}
		addresses[id] = bound
	}
	if admin != "" {
		listener, err := net.Listen("tcp", admin)
		if err != nil {
			return err
		}
		if !listener.Addr().(*net.TCPAddr).IP.IsLoopback() {
			_ = listener.Close()
			return fmt.Errorf("admin-listen must bind to loopback")
		}
		s := &http.Server{Handler: w.Admin(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second}
		defer s.Close()
		go func() { results <- s.Serve(listener) }()
	}
	if etcd != "" {
		client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(etcd, ","), DialTimeout: 3 * time.Second})
		if err != nil {
			return err
		}
		defer client.Close()
		discovery, stop := context.WithCancel(ctx)
		done := make(chan struct{}, len(addresses))
		for id, address := range addresses {
			go func() { registerWorker(discovery, client, id, address); done <- struct{}{} }()
		}
		defer func() {
			stop()
			for range addresses {
				<-done
			}
		}()
	}
	log.Printf("fixture serving placement=%s workers=%s admin=%s lease=%s", state, workers, admin, ttl)
	select {
	case <-ctx.Done():
		return nil
	case err := <-results:
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	}
}

func registerWorker(ctx context.Context, c *clientv3.Client, id, address string) {
	for ctx.Err() == nil {
		err := registration(ctx, c, id, address)
		if ctx.Err() != nil {
			return
		}
		log.Printf("worker %s renewing discovery registration: %v", id, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
func registration(ctx context.Context, c *clientv3.Client, id, address string) error {
	short, cancel := context.WithTimeout(ctx, 3*time.Second)
	lease, err := c.Grant(short, 6)
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		revoke, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = c.Revoke(revoke, lease.ID)
	}()
	key := "/duro/workers/" + id
	short, cancel = context.WithTimeout(ctx, 3*time.Second)
	tx, err := c.Txn(short).If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, address, clientv3.WithLease(lease.ID))).Commit()
	cancel()
	if err != nil {
		return err
	}
	if !tx.Succeeded {
		return fmt.Errorf("worker incarnation already registered")
	}
	updates, err := c.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-updates:
			if !ok {
				return fmt.Errorf("keepalive ended")
			}
		}
	}
}
