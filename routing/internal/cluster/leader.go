package cluster

import (
	"context"
	"errors"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const electionPrefix = "/duro/controllers"

func Campaign(ctx context.Context, etcd *clientv3.Client, address string, lead func(context.Context, uint64) error) {
	for ctx.Err() == nil {
		if err := campaignOnce(ctx, etcd, address, lead); err != nil && ctx.Err() == nil {
			slog.Warn("controller election ended", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func campaignOnce(ctx context.Context, etcd *clientv3.Client, address string, lead func(context.Context, uint64) error) error {
	session, err := concurrency.NewSession(etcd, concurrency.WithTTL(6), concurrency.WithContext(ctx))
	if err != nil {
		return err
	}
	defer session.Close()
	election := concurrency.NewElection(session, electionPrefix)
	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-session.Done():
			cancel()
		case <-leaderCtx.Done():
		}
	}()
	if err := election.Campaign(leaderCtx, address); err != nil {
		return err
	}
	slog.Info("controller elected", "address", address, "term", election.Rev())
	return lead(leaderCtx, uint64(election.Rev()))
}

func Leader(ctx context.Context, etcd *clientv3.Client) (string, error) {
	resp, err := etcd.Get(ctx, electionPrefix+"/", clientv3.WithFirstCreate()...)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", errors.New("controller election in progress")
	}
	return string(resp.Kvs[0].Value), nil
}
