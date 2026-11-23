package gateway

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/duro-d/duro/api/gen/go/duro/v1"
)

func TestRouteCacheBoundedLRUAndTTL(t *testing.T) {
	for _, tc := range []struct {
		name           string
		entries, bytes int
	}{
		{"entry limit", 2, 1 << 20},
		{"byte budget", 100, 2 * (route{"a", "w", 1}).size()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(0, 0)
			var calls atomic.Uint64
			s := New(Config{CacheEntries: tc.entries, CacheBytes: tc.bytes, CacheTTL: time.Second}, nil, nil).routes
			s.now = func() time.Time { return now }
			s.find = func(_ context.Context, id string) (route, error) { return route{id, "w", calls.Add(1)}, nil }
			resolve := func(id string) route {
				t.Helper()
				r, err := s.resolve(context.Background(), id)
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			a, b := resolve("a"), resolve("b")
			if resolve("a") != a {
				t.Fatal("cache hit changed the route")
			}
			resolve("c")
			if resolve("a") != a || resolve("b") == b {
				t.Fatal("LRU eviction order")
			}
			now = now.Add(time.Second)
			if resolve("a") == a {
				t.Fatal("served an expired route")
			}
			resolve(strings.Repeat("x", tc.bytes))
			if s.bytes > tc.bytes || s.lru.Len() > tc.entries {
				t.Fatalf("cache has %d entries, %d bytes", s.lru.Len(), s.bytes)
			}
		})
	}
}

func TestLateStaleResponses(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var epoch atomic.Uint64
	s := New(Config{}, nil, nil).routes
	s.find = func(ctx context.Context, id string) (route, error) {
		e := epoch.Add(1)
		if e == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return route{}, ctx.Err()
			}
		}
		return route{id, "w", e}, nil
	}
	errc := make(chan error, 1)
	go func() { _, err := s.resolve(context.Background(), "a"); errc <- err }()
	<-entered
	old := route{"a", "w", 1}
	s.invalidate(old, true)
	close(release)
	if err := <-errc; err != unavailable {
		t.Fatalf("obsolete lookup: %v", err)
	}
	next, err := s.resolve(context.Background(), "a")
	if err != nil || next.epoch != 2 {
		t.Fatal(next, err)
	}
	s.invalidate(old, true)
	again, err := s.resolve(context.Background(), "a")
	if err != nil || again != next || epoch.Load() != 2 {
		t.Fatal("late error discarded current route")
	}
}

func TestLookupCoalescesWithIndependentCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started, release := make(chan struct{}), make(chan struct{})
		var n atomic.Int32
		s := New(Config{MaxLookups: 1}, nil, nil).routes
		s.find = func(ctx context.Context, id string) (route, error) {
			if n.Add(1) == 1 {
				close(started)
			}
			select {
			case <-ctx.Done():
				return route{}, ctx.Err()
			case <-release:
				return route{id, "w", 1}, nil
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		waiter1 := make(chan error, 1)
		go func() { _, err := s.resolve(ctx, "a"); waiter1 <- err }()
		<-started
		if _, err := s.resolve(context.Background(), "b"); err != overloaded {
			t.Fatalf("second lookup: got %v", err)
		}
		waiter2 := make(chan error, 1)
		go func() { _, err := s.resolve(context.Background(), "a"); waiter2 <- err }()
		synctest.Wait()
		cancel()
		if err := <-waiter1; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		close(release)
		if err := <-waiter2; err != nil || n.Load() != 1 {
			t.Fatal(err, n.Load())
		}
	})
}

func TestLookupStopsWhenLastWaiterCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(Config{}, nil, nil).routes
		started := make(chan context.Context, 1)
		s.find = func(ctx context.Context, id string) (route, error) {
			started <- ctx
			<-ctx.Done()
			return route{}, ctx.Err()
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		done := make(chan error, 1)
		go func() { _, err := s.resolve(ctx, "a"); done <- err }()
		rpc := <-started
		deadline, _ := ctx.Deadline()
		if got, ok := rpc.Deadline(); !ok || !got.Equal(deadline) {
			t.Fatalf("RPC deadline %v, want %v", got, deadline)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		synctest.Wait()
		if rpc.Err() != context.Canceled || len(s.pending) != 0 {
			t.Fatalf("abandoned RPC: %v, pending: %d", rpc.Err(), len(s.pending))
		}
	})
}

func TestLongWaiterContinuesAfterSharedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(Config{MaxLookups: 1}, nil, nil).routes
		short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancelShort()
		long, cancelLong := context.WithTimeout(context.Background(), time.Second)
		defer cancelLong()
		deadlines := make(chan time.Time, 2)
		var calls atomic.Int32
		s.find = func(ctx context.Context, id string) (route, error) {
			deadline, _ := ctx.Deadline()
			deadlines <- deadline
			if calls.Add(1) == 1 {
				<-ctx.Done()
				return route{}, ctx.Err()
			}
			return route{id, "w", 1}, nil
		}
		first := make(chan error, 1)
		go func() { _, err := s.resolve(short, "a"); first <- err }()
		if got, want := <-deadlines, time.Now().Add(50*time.Millisecond); !got.Equal(want) {
			t.Fatalf("first deadline %v, want %v", got, want)
		}
		second := make(chan error, 1)
		go func() { _, err := s.resolve(long, "a"); second <- err }()
		synctest.Wait()
		if err := <-first; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if err := <-second; err != nil {
			t.Fatal(err)
		}
		want, _ := long.Deadline()
		if got := <-deadlines; !got.Equal(want) || calls.Load() != 2 {
			t.Fatalf("second deadline %v, want %v, lookup count %d", got, want, calls.Load())
		}
	})
}

func TestHTTPAdmissionAndTimeout(t *testing.T) {
	started := make(chan struct{})
	s := storageStub{read: func(ctx context.Context, _ string) (*pb.ReadObjectResponse, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	h := New(Config{Token: "secret", MaxRequests: 1}, s, nil)
	req := httptest.NewRequest("POST", "/v1/read", strings.NewReader(`{"object_id":"obj"}`))
	req.Header.Set("Duro-Timeout-Ms", "150")
	req.Header.Set("Authorization", "Bearer secret")
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { w := httptest.NewRecorder(); h.ServeHTTP(w, req); done <- w }()
	<-started
	got := post(t, h, "read", `{"object_id":"obj"}`, 503)
	if string(got["code"]) != `"OVERLOADED"` {
		t.Fatal(got)
	}
	select {
	case w := <-done:
		if w.Code != 503 || !strings.Contains(w.Body.String(), "UNAVAILABLE") {
			t.Fatal(w.Result(), w.Body)
		}
	case <-time.After(time.Second):
		t.Fatal("ignored Duro-Timeout-Ms")
	}
}

func TestDecodedPackageAndRequestLimits(t *testing.T) {
	h := New(Config{Token: "secret"}, nil, nil)
	manifest := `{"type":"t","version":"1","runtime":"javascript-v8","format":"esm","methods":[],"sha256":"bad"}`
	// Each quartet decodes to three bytes. The envelope remains below 2 MiB.
	body := `{"manifest":` + manifest + `,"bundle":"` + strings.Repeat("YWFh", (maxPackage+2)/3) + `"}`
	post(t, h, "registerType", body, 413)
	post(t, h, "registerType", `{"manifest":`+manifest+`,"bundle":[97]}`, 400)
	r := httptest.NewRequest("POST", "/v1/invoke", strings.NewReader(strings.Repeat(" ", maxRequest+1)))
	r.ContentLength = -1
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatal(w.Code)
	}
}
