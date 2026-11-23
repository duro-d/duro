// Package placement selects workers and proposes moves from controller snapshots.
package placement

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"time"
	"unicode/utf8"
)

type Policy string

const (
	HRW  Policy = "hrw"
	Load Policy = "load"
)

type Mode string

const (
	Active   Mode = "active"
	Reserve  Mode = "reserve"
	Draining Mode = "draining"
)

var (
	ErrUnavailable     = errors.New("no ready active worker with free capacity")
	ErrInvalidObjectID = errors.New("invalid placement object ID")
	ErrUnknownPolicy   = errors.New("unknown placement policy")
)

type Candidate struct {
	ID           string
	Ready        bool
	Mode         Mode
	FreeCapacity int
	Sample       Sample
}

func (c Candidate) canReceive() bool {
	return validID(c.ID) && c.Ready && c.Mode == Active && c.FreeCapacity > 0
}

// At is the end of the measurement window on the controller's clock,
// accounting for the age reported by the worker and the poll round trip.
type Sample struct {
	At           time.Time
	Queue        int
	QueueLimit   int
	BusyFraction float64
}

func (s Sample) score(now time.Time) (float64, bool) {
	age := now.Sub(s.At)
	if s.At.IsZero() || age < 0 || age > 3*time.Second || s.Queue < 0 ||
		s.QueueLimit <= 0 || !(s.BusyFraction >= 0 && s.BusyFraction <= 1) {
		return 0, false
	}
	queueFraction := math.Min(float64(s.Queue)/float64(s.QueueLimit), 1)
	return (queueFraction + s.BusyFraction) / 2, true
}

type Decision struct {
	WorkerID string
	Fallback bool // Count this load placement as an HRW fallback.
}

// Select chooses a target for an unassigned object from one membership snapshot.
// Requests for an assigned object use the owner confirmed by storage.
func Select(policy Policy, objectID string, candidates []Candidate, now time.Time) (Decision, error) {
	if policy != HRW && policy != Load {
		return Decision{}, ErrUnknownPolicy
	}
	if !validID(objectID) {
		return Decision{}, ErrInvalidObjectID
	}
	var winner string
	var winnerHash [sha256.Size]byte
	best := math.Inf(1)
	for _, c := range candidates {
		if !c.canReceive() {
			continue
		}
		score := 0.0
		if policy == Load {
			value, ok := c.Sample.score(now)
			if ok {
				score = value
			} else {
				score = math.Inf(1)
			}
		}
		if score > best {
			continue
		}
		hash := weight(objectID, c.ID)
		if winner == "" || score < best || outranks(hash, c.ID, winnerHash, winner) {
			best, winnerHash, winner = score, hash, c.ID
		}
	}
	if winner == "" {
		return Decision{}, ErrUnavailable
	}
	return Decision{WorkerID: winner, Fallback: math.IsInf(best, 1)}, nil
}

func validID(id string) bool {
	return id != "" && uint64(len(id)) <= math.MaxUint32 && utf8.ValidString(id)
}

func weight(objectID, workerID string) [sha256.Size]byte {
	input := make([]byte, 0, 8+len(objectID)+len(workerID))
	input = binary.BigEndian.AppendUint32(input, uint32(len(objectID)))
	input = append(input, objectID...)
	input = binary.BigEndian.AppendUint32(input, uint32(len(workerID)))
	input = append(input, workerID...)
	return sha256.Sum256(input)
}

func outranks(hash [sha256.Size]byte, id string, other [sha256.Size]byte, otherID string) bool {
	cmp := bytes.Compare(hash[:], other[:])
	return cmp > 0 || (cmp == 0 && id < otherID)
}
