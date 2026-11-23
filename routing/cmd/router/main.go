package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	"github.com/duro-d/duro/routing/internal/cluster"
	"github.com/duro-d/duro/routing/internal/controller"
	"github.com/duro-d/duro/routing/internal/gateway"
	"github.com/duro-d/duro/routing/internal/placement"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("router stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	public := flag.String("listen", "127.0.0.1:8080", "public HTTP listener")
	private := flag.String("private-listen", "127.0.0.1:8081", "controller and operator listener")
	advertise := flag.String("advertise", "", "private HTTP URL available to other routers")
	state := flag.String("state", "127.0.0.1:7100", "state service gRPC address")
	endpoints := flag.String("etcd", "127.0.0.1:2379", "comma-separated etcd endpoints")
	policy := flag.String("policy", "hrw", "hrw or load")
	flag.Parse()
	token, peerToken := os.Getenv("DURO_TOKEN"), os.Getenv("DURO_CONTROLLER_TOKEN")
	if token == "" || peerToken == "" {
		return errors.New("set DURO_TOKEN and DURO_CONTROLLER_TOKEN")
	}
	if *policy != "hrw" && *policy != "load" {
		return fmt.Errorf("unsupported policy %q", *policy)
	}
	if *advertise == "" {
		*advertise = "http://" + *private
	}
	u, err := url.Parse(*advertise)
	if err != nil || u.Scheme != "http" || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("advertise must be a private http://host:port URL")
	}
	etcd, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*endpoints, ","), DialTimeout: 3 * time.Second})
	if err != nil {
		return err
	}
	defer etcd.Close()
	rpcDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "duro_routing_rpc_seconds", Help: "Time spent in outgoing service calls."}, []string{"method", "status"})
	requests := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "duro_routing_request_seconds", Help: "Public request latency including failed attempts."}, []string{"operation", "code"})
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		rpcDuration.WithLabelValues(method, status.Code(err).String()).Observe(time.Since(start).Seconds())
		return err
	}
	dial := grpc.WithUnaryInterceptor(interceptor)
	conn, err := grpc.NewClient("passthrough:///"+*state, grpc.WithTransportCredentials(insecure.NewCredentials()), dial)
	if err != nil {
		return err
	}
	defer conn.Close()
	store := pb.NewPlacementServiceClient(conn)
	dir := cluster.NewDirectory(etcd, dial)
	defer dir.Close()
	ctrl := controller.New(store, dir, placement.Policy(*policy))
	peers := cluster.NewPeers(dir, ctrl, peerToken)
	api := gateway.New(gateway.Config{Token: token, OnRequest: func(ev gateway.Event) {
		op := ev.Operation
		switch op {
		case "create", "invoke", "read", "describe", "registerType":
		default:
			op = "unknown"
		}
		requests.WithLabelValues(op, ev.Code).Observe(ev.Duration.Seconds())
		slog.Info("public request", "operation", op, "code", ev.Code, "request_id", ev.RequestID, "object_id", ev.ObjectID, "worker_id", ev.WorkerID, "epoch", ev.Epoch, "duration_ms", ev.Duration.Milliseconds())
	}}, store, peers)
	reg := prometheus.NewRegistry()
	reg.MustRegister(newLoadMetrics(ctrl), rpcDuration, requests, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	for _, metric := range []struct {
		name string
		f    func() float64
	}{
		{"recoveries_total", func() float64 { return float64(ctrl.Recoveries()) }},
		{"cache_hits_total", func() float64 { return float64(api.Stats().Hits) }},
		{"cache_misses_total", func() float64 { return float64(api.Stats().Misses) }},
		{"cache_stale_total", func() float64 { return float64(api.Stats().Stale) }},
		{"hrw_fallbacks_total", func() float64 { f, _ := ctrl.Counts(); return float64(f) }},
		{"moves_total", func() float64 { _, m := ctrl.Counts(); return float64(m) }},
	} {
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "duro_routing_" + metric.name, Help: metric.name}, metric.f))
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "duro_routing_cache_entries", Help: "Current number of cached routes."}, func() float64 { return float64(api.Stats().Entries) }))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "duro_routing_is_controller", Help: "One for the elected router.", ConstLabels: prometheus.Labels{"policy": *policy}}, func() float64 {
		if ctrl.IsLeader() {
			return 1
		}
		return 0
	}))
	publicMux := http.NewServeMux()
	publicMux.Handle("/v1/", api)
	publicMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	publicMux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		reply, err := health.NewHealthClient(conn).Check(c, &health.HealthCheckRequest{})
		if err != nil || reply.GetStatus() != health.HealthCheckResponse_SERVING {
			http.Error(w, "state unavailable", 503)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	privateMux := http.NewServeMux()
	privateMux.Handle("/", peers.Internal())
	privateMux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	publicLn, err := net.Listen("tcp", *public)
	if err != nil {
		return err
	}
	defer publicLn.Close()
	privateLn, err := net.Listen("tcp", *private)
	if err != nil {
		return err
	}
	defer privateLn.Close()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); dir.Run(runCtx) }()
	leadDone := make(chan struct{})
	go func() { defer close(leadDone); cluster.Campaign(runCtx, etcd, *advertise, ctrl.ServeLeader) }()
	servers := []*http.Server{{Handler: publicMux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 6 * time.Second, WriteTimeout: 6 * time.Second, IdleTimeout: 30 * time.Second}, {Handler: privateMux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 6 * time.Second, WriteTimeout: 6 * time.Second, IdleTimeout: 30 * time.Second}}
	exits := make(chan error, 2)
	go func() { exits <- servers[0].Serve(publicLn) }()
	go func() { exits <- servers[1].Serve(privateLn) }()
	slog.Info("router started", "public", *public, "private", *private, "policy", *policy)
	select {
	case <-ctx.Done():
	case err = <-exits:
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	for _, s := range servers {
		_ = s.Shutdown(shutdown)
	}
	select {
	case <-leadDone:
	case <-shutdown.Done():
	}
	select {
	case <-done:
	case <-shutdown.Done():
	}
	return err
}
