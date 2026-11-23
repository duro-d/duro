package cluster

import (
	"context"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const workerPrefix = "/duro/workers/"

type endpoint struct {
	address string
	conn    *grpc.ClientConn
}

type Directory struct {
	dialOptions []grpc.DialOption
	etcd        *clientv3.Client
	mu          sync.Mutex
	workers     map[string]*endpoint
	synced      bool
}

func NewDirectory(etcd *clientv3.Client, dialOptions ...grpc.DialOption) *Directory {
	return &Directory{dialOptions: dialOptions, etcd: etcd, workers: make(map[string]*endpoint)}
}

func (d *Directory) Workers(_ context.Context) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.synced {
		return nil, status.Error(codes.Unavailable, "worker registry is synchronizing")
	}
	ids := make([]string, 0, len(d.workers))
	for id := range d.workers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (d *Directory) Worker(_ context.Context, id string) (pb.WorkerServiceClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry := d.workers[id]
	if entry == nil {
		return nil, status.Error(codes.Unavailable, "worker endpoint unavailable")
	}
	if entry.conn == nil {
		conn, err := grpc.NewClient("passthrough:///"+entry.address, append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, d.dialOptions...)...)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "invalid worker endpoint")
		}
		entry.conn = conn
	}
	return pb.NewWorkerServiceClient(entry.conn), nil
}

func (d *Directory) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for id := range d.workers {
		d.remove(id)
	}
	d.synced = false
}

func (d *Directory) remove(id string) {
	if old := d.workers[id]; old != nil && old.conn != nil {
		_ = old.conn.Close()
	}
	delete(d.workers, id)
}

func (d *Directory) set(id, address string) {
	if old := d.workers[id]; old != nil && old.address == address {
		return
	}
	d.remove(id)
	if _, _, err := net.SplitHostPort(address); err == nil && id != "" && !strings.Contains(id, "/") {
		d.workers[id] = &endpoint{address: address}
	}
}

func (d *Directory) Run(ctx context.Context) {
	for ctx.Err() == nil {
		err := d.watch(ctx)
		d.mu.Lock()
		d.synced = false
		d.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		slog.Warn("worker registry resync", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (d *Directory) watch(ctx context.Context) error {
	read, cancel := context.WithTimeout(ctx, 3*time.Second)
	resp, err := d.etcd.Get(read, workerPrefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(resp.Kvs))
	d.mu.Lock()
	for _, kv := range resp.Kvs {
		id := strings.TrimPrefix(string(kv.Key), workerPrefix)
		seen[id] = true
		d.set(id, string(kv.Value))
	}
	for id := range d.workers {
		if !seen[id] {
			d.remove(id)
		}
	}
	d.synced = true
	d.mu.Unlock()
	stream := d.etcd.Watch(ctx, workerPrefix, clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
	for update := range stream {
		if err := update.Err(); err != nil {
			return err
		}
		d.mu.Lock()
		for _, ev := range update.Events {
			id := strings.TrimPrefix(string(ev.Kv.Key), workerPrefix)
			if ev.Type == clientv3.EventTypeDelete {
				d.remove(id)
			} else {
				d.set(id, string(ev.Kv.Value))
			}
		}
		d.mu.Unlock()
	}
	return ctx.Err()
}
