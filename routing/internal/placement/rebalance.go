package placement

import (
	"math"
	"time"
)

// Assignment describes a confirmed owner generation and its last transfer.
type Assignment struct {
	ObjectID  string
	WorkerID  string
	Epoch     uint64
	LastMoved time.Time
}

type Move struct {
	Source Assignment
	Target string
}

// Limits is a snapshot of cluster-wide planned transfer admission. The
// controller serializes admission and checks these limits again at start.
type Limits struct {
	InFlight  bool
	LastStart time.Time
}

func (l Limits) canStart(now time.Time) bool {
	return !l.InFlight && elapsed(l.LastStart, now, time.Second)
}

func elapsed(last, now time.Time, interval time.Duration) bool {
	return last.IsZero() || now.Sub(last) >= interval
}

// Allows checks the rate, concurrency and per-object limits for a manual
// or planned move. Recovery uses a separate admission pool.
func (l Limits) Allows(a Assignment, now time.Time) bool {
	return l.canStart(now) && a.movable(now)
}

func (a Assignment) movable(now time.Time) bool {
	return a.Epoch != 0 && validID(a.ObjectID) && validID(a.WorkerID) && elapsed(a.LastMoved, now, time.Minute)
}

// PlanJoin examines one bounded storage page after a worker joins. It proposes
// at most one transfer to the new worker, retaining the source epoch for the
// controller's conditional transfer. The caller controls the page cursor.
func PlanJoin(joinedWorkerID string, candidates []Candidate, page []Assignment, limits Limits, now time.Time) (Move, bool) {
	if !limits.canStart(now) {
		return Move{}, false
	}
	sources := make(map[string]bool, len(candidates))
	var joinedReady bool
	for _, c := range candidates {
		sources[c.ID] = c.Ready && c.Mode == Active && validID(c.ID)
		if c.ID == joinedWorkerID {
			joinedReady = c.canReceive()
		}
	}
	if !joinedReady {
		return Move{}, false
	}

	var best Move
	for _, a := range page {
		if !a.movable(now) || !sources[a.WorkerID] || a.WorkerID == joinedWorkerID {
			continue
		}
		// Keeping an existing object on a full source needs no free slot.
		if outranks(weight(a.ObjectID, a.WorkerID), a.WorkerID, weight(a.ObjectID, joinedWorkerID), joinedWorkerID) {
			continue
		}
		target, err := Select(HRW, a.ObjectID, candidates, now)
		if err == nil && target.WorkerID == joinedWorkerID && (best.Target == "" || a.ObjectID < best.Source.ObjectID) {
			best = Move{Source: a, Target: joinedWorkerID}
		}
	}
	return best, best.Target != ""
}

// LoadWindow stores three consecutive polls of one worker incarnation. Previous
// is ordered oldest first. A missed poll occupies its slot with a zero Sample.
type LoadWindow struct {
	Worker   Candidate
	Previous [2]Sample
}

func (w LoadWindow) scores(now time.Time) ([3]float64, bool) {
	var scores [3]float64
	samples := [...]Sample{w.Previous[0], w.Previous[1], w.Worker.Sample}
	for i, s := range samples {
		score, ok := s.score(now)
		if !ok || (i > 0 && !s.At.After(samples[i-1].At)) {
			return scores, false
		}
		scores[i] = score
	}
	return scores, true
}

// HotObject comes from the source's latest window. BusyFraction is this
// object's share of all available execution slot time on that worker.
type HotObject struct {
	Assignment
	CallCount    uint64
	BusyFraction float64
}

// PlanLoad selects a move from one source and its most recent top-16 list.
// If several moves shrink the gap, it picks the largest reduction, then the
// smaller object and target IDs. A full worker may still shed load.
func PlanLoad(source LoadWindow, targets []LoadWindow, hot []HotObject, limits Limits, now time.Time) (Move, bool) {
	if !limits.canStart(now) || !source.Worker.Ready || source.Worker.Mode != Active ||
		!validID(source.Worker.ID) || len(hot) == 0 || len(hot) > 16 {
		return Move{}, false
	}
	sourceScores, ok := source.scores(now)
	if !ok {
		return Move{}, false
	}
	var best Move
	var bestReduction float64
	for _, target := range targets {
		if !target.Worker.canReceive() || target.Worker.ID == source.Worker.ID {
			continue
		}
		targetScores, ok := target.scores(now)
		if !ok || !sustainedGap(sourceScores, targetScores) {
			continue
		}
		gap := sourceScores[2] - targetScores[2]
		for _, obj := range hot {
			if obj.WorkerID != source.Worker.ID || !obj.movable(now) || obj.CallCount == 0 ||
				!(obj.BusyFraction > 0 && obj.BusyFraction <= source.Worker.Sample.BusyFraction) {
				continue
			}
			// Keeping the measured queues fixed, moving the object subtracts
			// half its busy fraction from one score and adds half to the other.
			reduction := gap - math.Abs(gap-obj.BusyFraction)
			if reduction <= 0 {
				continue
			}
			if reduction > bestReduction || (reduction == bestReduction &&
				(obj.ObjectID < best.Source.ObjectID || (obj.ObjectID == best.Source.ObjectID && target.Worker.ID < best.Target))) {
				best = Move{Source: obj.Assignment, Target: target.Worker.ID}
				bestReduction = reduction
			}
		}
	}
	return best, best.Target != ""
}

func sustainedGap(source, target [3]float64) bool {
	for i := range source {
		if source[i]-target[i] <= 0.25 {
			return false
		}
	}
	return true
}
