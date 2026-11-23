package placement

import (
	"crypto/sha256"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

func worker(id string, queue int, busy float64) Candidate {
	return Candidate{
		ID: id, Ready: true, Mode: Active, FreeCapacity: 1,
		Sample: Sample{At: now, Queue: queue, QueueLimit: 10, BusyFraction: busy},
	}
}

func TestRendezvousWireFormat(t *testing.T) {
	fixtures := []struct{ object, worker, digest string }{
		{"object-17", "worker-a", "b0ca858617da91afa4b76a02718402f87f6f83d2d0b48138e4221902b7fd6ea3"},
		{"ключ", "узел", "0959b080b83f6ac063ce2729c116b38093df9ab196840ac2b4cdbcb60f2f4dd7"},
		{"a", "bc", "b534ce16ac9c8b36823f39a395ce8e0e3c7ad9605b82b5444f18cadacd217a5d"},
		{"ab", "c", "f2939f903016e5bb29b1e4a61cdbd376220ca03a24180b39995f2d50f2e0a647"},
	}
	for _, f := range fixtures {
		if got := fmt.Sprintf("%x", weight(f.object, f.worker)); got != f.digest {
			t.Errorf("weight(%q, %q) = %s, want %s", f.object, f.worker, got, f.digest)
		}
	}
	var tied [sha256.Size]byte
	if !outranks(tied, "a", tied, "b") || outranks(tied, "b", tied, "a") {
		t.Fatal("equal HRW weights must prefer the smaller worker ID")
	}
}

func TestSelectOrderAndLoad(t *testing.T) {
	a, b, c := worker("worker-a", 0, 0), worker("worker-b", 2, .2), worker("worker-c", 9, .1)
	// Scores are .5, .2 and .5. The HRW ranking for object-17 is a, c, b.
	a.Sample.Queue = 200
	for _, candidates := range [][]Candidate{{a, b, c}, {b, c, a}, {c, b, a}, {a, c, b}, {b, a, c}, {c, a, b}} {
		for policy, want := range map[Policy]string{Load: b.ID, HRW: a.ID} {
			got, err := Select(policy, "object-17", candidates, now)
			if err != nil || got != (Decision{WorkerID: want}) {
				t.Errorf("Select(%s, %v) = %+v, %v; want %s", policy, candidates, got, err, want)
			}
		}
	}
	for _, candidates := range [][]Candidate{{c, a}, {a, c}} {
		if got, err := Select(Load, "object-17", candidates, now); err != nil || got.WorkerID != a.ID || got.Fallback {
			t.Fatalf("tied load scores: %+v, %v", got, err)
		}
	}
	if score, ok := a.Sample.score(now); !ok || score != .5 {
		t.Fatalf("clamped queue score = %v, %v", score, ok)
	}
}

func TestSelectAdmissibleWorkers(t *testing.T) {
	candidates := []Candidate{
		worker("worker-a", 0, 0), worker("worker-b", 9, 1), worker("other", 0, 0),
		worker("reserve", 0, 0), worker("draining", 0, 0), worker("", 0, 0),
	}
	candidates[0].Ready = false
	candidates[2].FreeCapacity = 0
	candidates[3].Mode = Reserve
	candidates[4].Mode = Draining
	for _, p := range []Policy{HRW, Load} {
		got, err := Select(p, "object-17", candidates, now)
		if err != nil || got != (Decision{WorkerID: "worker-b"}) {
			t.Errorf("Select(%s) = %+v, %v", p, got, err)
		}
	}
	candidates[1].FreeCapacity = 0
	for _, p := range []Policy{HRW, Load} {
		if _, err := Select(p, "object-17", candidates, now); err != ErrUnavailable {
			t.Errorf("Select(%s) without capacity = %v", p, err)
		}
	}
	if _, err := Select(HRW, "", candidates, now); err != ErrInvalidObjectID {
		t.Errorf("empty ID: %v", err)
	}
	if _, err := Select("unknown", "a", candidates, now); err != ErrUnknownPolicy {
		t.Errorf("unknown policy: %v", err)
	}
}

func TestSampleFreshnessAndFallback(t *testing.T) {
	fresh := worker("worker-b", 10, 1)
	fresh.Sample.At = now.Add(-3 * time.Second)
	invalid := map[string]Sample{
		"absent":         {},
		"stale":          {At: fresh.Sample.At.Add(-time.Nanosecond), QueueLimit: 10},
		"future":         {At: now.Add(time.Nanosecond), QueueLimit: 10},
		"zero limit":     {At: now},
		"negative queue": {At: now, QueueLimit: 10, Queue: -1},
		"negative busy":  {At: now, QueueLimit: 10, BusyFraction: -.01},
		"busy over one":  {At: now, QueueLimit: 10, BusyFraction: 1.001},
		"NaN":            {At: now, QueueLimit: 10, BusyFraction: math.NaN()},
		"infinity":       {At: now, QueueLimit: 10, BusyFraction: math.Inf(1)},
	}
	for name, sample := range invalid {
		t.Run(name, func(t *testing.T) {
			a := worker("worker-a", 0, 0)
			a.Sample = sample
			c := worker("worker-c", 0, 0)
			c.Sample = Sample{}
			got, err := Select(Load, "object-17", []Candidate{a, c, fresh}, now)
			if err != nil || got != (Decision{WorkerID: fresh.ID}) {
				t.Fatalf("one fresh worker: %+v, %v", got, err)
			}
			got, err = Select(Load, "object-17", []Candidate{c, a}, now)
			if err != nil || got != (Decision{WorkerID: a.ID, Fallback: true}) {
				t.Fatalf("fallback: %+v, %v", got, err)
			}
		})
	}
}

func TestSelectSharedSnapshot(t *testing.T) {
	candidates := []Candidate{worker("worker-c", 4, .25), worker("worker-a", 4, .25)}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if got, err := Select(Load, "object-17", candidates, now); err != nil || got.WorkerID != "worker-a" {
					t.Errorf("concurrent selection: %+v, %v", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
