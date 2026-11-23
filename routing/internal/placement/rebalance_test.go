package placement

import (
	"math"
	"testing"
	"time"
)

func window(c Candidate) LoadWindow {
	w := LoadWindow{Worker: c, Previous: [2]Sample{c.Sample, c.Sample}}
	w.Previous[0].At = c.Sample.At.Add(-2 * time.Second)
	w.Previous[1].At = c.Sample.At.Add(-time.Second)
	return w
}

func TestPlanLoad(t *testing.T) {
	source := window(worker("source", 10, 1))
	source.Worker.FreeCapacity = 0
	target := window(worker("target", 5, 0))
	// Scores 1 and .25 yield a .75 gap. Moving "medium" subtracts .375
	// from the source and adds .375 to the target, closing the gap.
	hot := []HotObject{
		{Assignment: Assignment{ObjectID: "small", WorkerID: "source", Epoch: 2}, CallCount: 100, BusyFraction: .125},
		{Assignment: Assignment{ObjectID: "medium", WorkerID: "source", Epoch: 3}, CallCount: 40, BusyFraction: .75},
	}
	want := Move{Source: hot[1].Assignment, Target: "target"}
	extra := window(worker("warmer", 5, .5))
	for _, targets := range [][]LoadWindow{{target, extra}, {extra, target}} {
		for _, objects := range [][]HotObject{hot, {hot[1], hot[0]}} {
			if got, ok := PlanLoad(source, targets, objects, Limits{}, now); !ok || got != want {
				t.Fatalf("PlanLoad() = %+v, %v; want %+v", got, ok, want)
			}
		}
	}
}

func TestPlanLoadRequiresConsecutiveFreshGaps(t *testing.T) {
	src := window(worker("source", 0, 1))
	dst := window(worker("target", 0, .25))
	obj := HotObject{Assignment: Assignment{ObjectID: "x", WorkerID: "source", Epoch: 3}, CallCount: 4, BusyFraction: .25}
	tests := []struct {
		name string
		edit func(*LoadWindow, *LoadWindow, *HotObject)
	}{
		{"gap at threshold", func(s, d *LoadWindow, o *HotObject) { s.Previous[1].BusyFraction = .75 }},
		{"absent poll", func(s, d *LoadWindow, o *HotObject) { d.Previous[1] = Sample{} }},
		{"duplicate window", func(s, d *LoadWindow, o *HotObject) { s.Previous[1] = s.Previous[0] }},
		{"out of order", func(s, d *LoadWindow, o *HotObject) { s.Previous[0].At = now }},
		{"stale", func(s, d *LoadWindow, o *HotObject) { d.Previous[0].At = now.Add(-4 * time.Second) }},
		{"invalid sample", func(s, d *LoadWindow, o *HotObject) { s.Worker.Sample.BusyFraction = math.NaN() }},
		{"target full", func(s, d *LoadWindow, o *HotObject) { d.Worker.FreeCapacity = 0 }},
		{"source unavailable", func(s, d *LoadWindow, o *HotObject) { s.Worker.Ready = false }},
		{"target draining", func(s, d *LoadWindow, o *HotObject) { d.Worker.Mode = Draining }},
		{"same worker", func(s, d *LoadWindow, o *HotObject) { d.Worker.ID = s.Worker.ID }},
		{"changed owner", func(s, d *LoadWindow, o *HotObject) { o.WorkerID = "another" }},
		{"missing epoch", func(s, d *LoadWindow, o *HotObject) { o.Epoch = 0 }},
		{"bad object share", func(s, d *LoadWindow, o *HotObject) { o.BusyFraction = math.NaN() }},
		{"too much slot time", func(s, d *LoadWindow, o *HotObject) { o.BusyFraction = 1.5 }},
		{"no calls", func(s, d *LoadWindow, o *HotObject) { o.CallCount = 0 }},
		{"gap unchanged after move", func(s, d *LoadWindow, o *HotObject) { o.BusyFraction = .75 }},
		{"gap wider after move", func(s, d *LoadWindow, o *HotObject) { o.BusyFraction = 1 }},
		{"object cooldown", func(s, d *LoadWindow, o *HotObject) { o.LastMoved = now.Add(-time.Minute + time.Nanosecond) }},
	}
	if _, ok := PlanLoad(src, []LoadWindow{dst}, []HotObject{obj}, Limits{}, now); !ok {
		t.Fatal("fixture should permit transfer before each edit")
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, d, o := src, dst, obj
			tt.edit(&s, &d, &o)
			if move, ok := PlanLoad(s, []LoadWindow{d}, []HotObject{o}, Limits{}, now); ok {
				t.Errorf("unexpected move %+v", move)
			}
		})
	}
	top := make([]HotObject, 17)
	top[16] = obj
	if move, ok := PlanLoad(src, []LoadWindow{dst}, top, Limits{}, now); ok {
		t.Errorf("accepted oversized top-object report: %+v", move)
	}
}

func TestPlanJoin(t *testing.T) {
	a, b, c := worker("worker-a", 0, 0), worker("worker-b", 0, 0), worker("worker-c", 0, 0)
	b.FreeCapacity = 0
	assignment := Assignment{ObjectID: "object-17", WorkerID: b.ID, Epoch: 4}
	candidates := []Candidate{b, c, a}
	move, ok := PlanJoin(a.ID, candidates, []Assignment{assignment}, Limits{}, now)
	if !ok || move != (Move{Source: assignment, Target: a.ID}) {
		t.Fatalf("new HRW winner: %+v, %v", move, ok)
	}
	assignment.WorkerID = a.ID
	a.FreeCapacity = 0
	if move, ok := PlanJoin(c.ID, []Candidate{a, c}, []Assignment{assignment}, Limits{}, now); ok {
		t.Errorf("joiner lost HRW to the existing full source: %+v", move)
	}
	assignment.WorkerID = b.ID
	if move, ok := PlanJoin(c.ID, candidates, []Assignment{assignment}, Limits{}, now); ok {
		t.Errorf("best worker was already in cluster: %+v", move)
	}
	assignment.LastMoved = now.Add(-10 * time.Second)
	if move, ok := PlanJoin(a.ID, candidates, []Assignment{assignment}, Limits{}, now); ok {
		t.Errorf("assignment in cooldown: %+v", move)
	}
}

func TestPlannedMoveAdmission(t *testing.T) {
	a := Assignment{ObjectID: "x", WorkerID: "source", Epoch: 1, LastMoved: now.Add(-time.Minute)}
	l := Limits{LastStart: now.Add(-time.Second)}
	if !l.Allows(a, now) {
		t.Fatal("rate and cooldown boundaries should admit the move")
	}
	if l.Allows(a, now.Add(-time.Nanosecond)) {
		t.Fatal("admitted before the cooldown")
	}
	a.LastMoved = time.Time{}
	if l.Allows(a, now.Add(-time.Nanosecond)) {
		t.Fatal("admitted within one second of the previous start")
	}
	l.InFlight = true
	if l.Allows(a, now) {
		t.Fatal("admitted during another planned transfer")
	}
}
