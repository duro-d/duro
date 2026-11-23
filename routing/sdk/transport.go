package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

const (
	defaultTimeout = 5 * time.Second
	maxAttempts    = 3
	maxResponse    = 2 << 20 // The read envelope can contain 1 MiB of KV.
	maxErrorBody   = 64 << 10
)

// Error carries the Duro code and the key needed to resume a mutation.
// UnknownOutcome means that the operation may already have taken effect.
type Error struct {
	StatusCode     int
	Code           string
	Message        string
	RequestID      string
	UnknownOutcome bool
	Cause          error
}

func (e *Error) Error() string {
	message := e.Message
	if message == "" {
		message = http.StatusText(e.StatusCode)
	}
	if e.Code != "" {
		message = e.Code + ": " + message
	}
	if e.Cause != nil {
		if message == "" {
			return "duro: " + e.Cause.Error()
		}
		message += ": " + e.Cause.Error()
	}
	return "duro: " + message
}

func (e *Error) Unwrap() error { return e.Cause }

func tooLarge(id string) *Error {
	return &Error{StatusCode: http.StatusRequestEntityTooLarge, Code: "LIMIT_EXCEEDED", Message: "request exceeds size limit", RequestID: id}
}

func changesState(operation string) bool {
	return operation == "create" || operation == "invoke" || operation == "registerType"
}

func invalidResponse(operation, id string) *Error {
	return &Error{Message: "invalid " + operation + " response", RequestID: id, UnknownOutcome: changesState(operation)}
}

func (c *Client) send(ctx context.Context, operation, id string, body []byte, result any) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultTimeout)
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var last *Error
	var unknown bool
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return stopped(last, id, err)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return stopped(last, id, context.DeadlineExceeded)
		}
		endpoint := c.endpoints[attempt%len(c.endpoints)]
		failure, retry := c.attempt(ctx, endpoint, operation, id, body, remaining, result)
		if failure == nil {
			return nil
		}
		if !retry {
			return failure
		}
		unknown = unknown || failure.UnknownOutcome
		failure.UnknownOutcome = unknown
		last = failure
		if err := ctx.Err(); err != nil {
			return stopped(last, id, err)
		}
		if attempt+1 < maxAttempts {
			// The first two backoffs have ceilings of 50 and 100 ms.
			ceiling := min(500*time.Millisecond, (50*time.Millisecond)<<attempt)
			delay := time.Duration(rand.Int63n(int64(ceiling) + 1))
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return stopped(last, id, ctx.Err())
			case <-timer.C:
			}
		}
	}
	return last
}

func stopped(last *Error, id string, err error) error {
	if last == nil {
		return &Error{RequestID: id, Cause: err}
	}
	if !errors.Is(last.Cause, err) {
		last.Cause = errors.Join(last.Cause, err)
	}
	return last
}

func (c *Client) attempt(ctx context.Context, endpoint, operation, id string, body []byte, remaining time.Duration, result any) (*Error, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/"+operation, bytes.NewReader(body))
	if err != nil {
		return &Error{RequestID: id, Cause: err}, false
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Duro-Timeout-Ms", strconv.FormatInt(max(1, remaining.Milliseconds()), 10))
	resp, err := c.http.Do(req)
	if err != nil {
		return &Error{RequestID: id, Cause: err, UnknownOutcome: changesState(operation)}, true
	}
	defer resp.Body.Close()

	success := resp.StatusCode/100 == 2
	limit := int64(maxErrorBody)
	if success {
		limit = maxResponse
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if int64(len(data)) > limit {
		err = fmt.Errorf("response exceeds %d bytes", limit)
		failure := &Error{StatusCode: resp.StatusCode, RequestID: id, Cause: err, UnknownOutcome: success && changesState(operation)}
		return failure, resp.StatusCode == http.StatusServiceUnavailable
	}
	if err != nil {
		failure := &Error{StatusCode: resp.StatusCode, RequestID: id, Cause: err, UnknownOutcome: changesState(operation) && (success || resp.StatusCode >= 500)}
		return failure, success || resp.StatusCode == http.StatusServiceUnavailable
	}
	if success {
		if err := json.Unmarshal(data, result); err != nil {
			failure := invalidResponse(operation, id)
			failure.StatusCode = resp.StatusCode
			failure.Cause = err
			return failure, false
		}
		return nil, false
	}

	var detail struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	err = json.Unmarshal(data, &detail)
	failure := &Error{StatusCode: resp.StatusCode, Code: detail.Code, Message: detail.Message, RequestID: id, Cause: err}
	retry := resp.StatusCode == http.StatusServiceUnavailable
	failure.UnknownOutcome = changesState(operation) && ((retry && detail.Code != "OVERLOADED") || resp.StatusCode == http.StatusInternalServerError)
	return failure, retry
}
