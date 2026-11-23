package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, urls ...string) *Client {
	t.Helper()
	client, err := NewClient(urls, "deployment-token")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func invoke(t *testing.T, client *Client, id string) *Mutation {
	t.Helper()
	m, err := client.PrepareInvoke(InvokeRequest{ObjectID: "account/1", Method: "add", RequestID: id})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func readBody(t *testing.T, r *http.Request) string {
	t.Helper()
	data, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read request: %v", err)
	}
	return string(data)
}

func budget(t *testing.T, r *http.Request) int64 {
	t.Helper()
	ms, err := strconv.ParseInt(r.Header.Get("Duro-Timeout-Ms"), 10, 64)
	if err != nil || ms < 1 || ms > 5000 {
		t.Errorf("request budget %d, err %v", ms, err)
	}
	return ms
}

func TestPublicOperations(t *testing.T) {
	requests := make(chan map[string]json.RawMessage, 5)
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer deployment-token" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected method or request headers")
		}
		budget(t, r)
		var fields map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
			t.Errorf("request JSON: %v", err)
		}
		requests <- fields
		switch r.URL.Path {
		case "/v1/registerType":
			fmt.Fprint(w, `{"type":"counter","version":"v1","sha256":"hash"}`)
		case "/v1/create":
			fmt.Fprint(w, `{"object_id":"account/1","result":0}`)
		case "/v1/invoke":
			fmt.Fprint(w, `{"result":null}`)
		case "/v1/describe":
			fmt.Fprint(w, `{"object_id":"account/1","type":"counter","version":"v1","methods":["add"]}`)
		case "/v1/read":
			fmt.Fprint(w, `{"kv":{"empty":null,"n":9007199254740993}}`)
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer router.Close()
	client := testClient(t, router.URL)
	ctx := context.Background()
	manifest := Manifest{Type: "counter", Version: "v1", Runtime: "javascript-v8", Format: "esm", Methods: []string{"add"}, SHA256: "hash"}
	registered, err := client.RegisterType(ctx, manifest, []byte{0xff, 0x00})
	if err != nil || registered.SHA256 != manifest.SHA256 {
		t.Fatalf("register: %+v, %v", registered, err)
	}
	fields := <-requests
	manifestJSON, _ := json.Marshal(manifest)
	if len(fields) != 2 || string(fields["manifest"]) != string(manifestJSON) || string(fields["bundle"]) != `"/wA="` {
		t.Fatalf("registration body: %v", fields)
	}

	create, err := client.PrepareCreate(CreateRequest{ObjectID: "account/1", Type: "counter", Version: "v1", Input: json.RawMessage(`{"n":1}`), RequestID: "create-1"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-requests:
		t.Fatal("PrepareCreate sent a request")
	default:
	}
	created, err := create.Do(ctx)
	if err != nil || created.ObjectID != "account/1" || string(created.Result) != "0" {
		t.Fatalf("create: %+v, %v", created, err)
	}
	fields = <-requests
	if len(fields) != 5 || string(fields["object_id"]) != `"account/1"` || string(fields["type"]) != `"counter"` || string(fields["version"]) != `"v1"` ||
		string(fields["request_id"]) != `"create-1"` || string(fields["input"]) != `{"n":1}` {
		t.Fatalf("create body: %v", fields)
	}

	call := invoke(t, client, "")
	if call.RequestID() == "" {
		t.Fatal("missing generated request ID")
	}
	res, err := call.Do(ctx)
	if err != nil || string(res.Result) != "null" {
		t.Fatalf("invoke: %+v, %v", res, err)
	}
	fields = <-requests
	if len(fields) != 4 || string(fields["object_id"]) != `"account/1"` || string(fields["method"]) != `"add"` ||
		string(fields["argument"]) != "null" || string(fields["request_id"]) != strconv.Quote(call.RequestID()) {
		t.Fatalf("invoke body: %v", fields)
	}

	desc, err := client.Describe(ctx, "account/1")
	if err != nil || desc.ObjectID != "account/1" || desc.Methods[0] != "add" {
		t.Fatalf("describe: %+v, %v", desc, err)
	}
	kv, err := client.Read(ctx, "account/1")
	if err != nil || string(kv["empty"]) != "null" || string(kv["n"]) != "9007199254740993" {
		t.Fatalf("read: %s, %v", kv, err)
	}
	for i := 0; i < 2; i++ {
		fields = <-requests
		if len(fields) != 1 || string(fields["object_id"]) != `"account/1"` {
			t.Fatalf("read/describe body: %v", fields)
		}
	}
}

func TestFailoverKeepsOriginalBytesAndDeadline(t *testing.T) {
	for _, firstFailure := range []string{"503", "lost-response"} {
		t.Run(firstFailure, func(t *testing.T) {
			type seen struct {
				body string
				ms   int64
			}
			requests := make(chan seen, 3)
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- seen{readBody(t, r), budget(t, r)}
				time.Sleep(15 * time.Millisecond)
				if firstFailure == "503" {
					w.WriteHeader(503)
					fmt.Fprint(w, `{"code":"UNAVAILABLE","message":"temporarily unavailable"}`)
					return
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return
				}
				conn.Close()
			}))
			defer first.Close()
			next := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- seen{readBody(t, r), budget(t, r)}
				fmt.Fprint(w, `{"result":7}`)
			}))
			defer next.Close()
			client := testClient(t, first.URL, next.URL)
			argument := json.RawMessage(`{"n":7}`)
			call, err := client.PrepareInvoke(InvokeRequest{ObjectID: "account/1", Method: "add", Argument: argument})
			if err != nil {
				t.Fatal(err)
			}
			copy(argument, `{"n":9}`)
			result, err := call.Do(context.Background())
			if err != nil || string(result.Result) != "7" {
				t.Fatalf("failover: %s, %v", result.Result, err)
			}
			a, b := <-requests, <-requests
			if a.body != b.body || !strings.Contains(a.body, call.RequestID()) || !strings.Contains(a.body, `"n":7`) || b.ms >= a.ms {
				t.Fatalf("attempts: %+v, %+v", a, b)
			}
		})
	}
}

func TestErrorContract(t *testing.T) {
	for _, tc := range []struct {
		code             string
		status, attempts int
		unknown          bool
	}{
		{"INVALID_ARGUMENT", 400, 1, false}, {"UNAUTHORIZED", 401, 1, false}, {"NOT_FOUND", 404, 1, false},
		{"CODE_VERSION_CONFLICT", 409, 1, false}, {"OBJECT_ALREADY_EXISTS", 409, 1, false}, {"REQUEST_ID_CONFLICT", 409, 1, false},
		{"LIMIT_EXCEEDED", 413, 1, false}, {"DOMAIN_REJECTED", 422, 1, false}, {"EXECUTION_TIMEOUT", 504, 1, false}, {"INTERNAL_ERROR", 500, 1, true},
		{"OVERLOADED", 503, 3, false}, {"UNAVAILABLE", 503, 3, true},
	} {
		t.Run(tc.code, func(t *testing.T) {
			var attempts atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.WriteHeader(tc.status)
				fmt.Fprintf(w, `{"code":%q,"message":"reason"}`, tc.code)
			}))
			defer s.Close()
			_, err := invoke(t, testClient(t, s.URL), "saved").Do(context.Background())
			var failure *Error
			if !errors.As(err, &failure) || failure.StatusCode != tc.status || failure.Code != tc.code || failure.Message != "reason" || failure.RequestID != "saved" ||
				failure.UnknownOutcome != tc.unknown || int(attempts.Load()) != tc.attempts {
				t.Fatalf("failure: %+v, attempts %d", failure, attempts.Load())
			}
		})
	}
}

func TestCancellationAndResume(t *testing.T) {
	bodies := make(chan string, 2)
	var attempts atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		bodies <- readBody(t, r)
		if n == 1 {
			<-r.Context().Done()
			return
		}
		fmt.Fprint(w, `{"result":1}`)
	}))
	defer s.Close()
	m := invoke(t, testClient(t, s.URL), "saved")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.Do(ctx); done <- err }()
	original := <-bodies
	cancel()
	err := <-done
	var failure *Error
	if !errors.As(err, &failure) || !errors.Is(err, context.Canceled) || !failure.UnknownOutcome || failure.RequestID != "saved" {
		t.Fatalf("in flight: %v", err)
	}
	_, err = m.Do(ctx)
	if !errors.As(err, &failure) || !errors.Is(err, context.Canceled) || failure.UnknownOutcome {
		t.Fatalf("already canceled: %v", err)
	}
	result, err := m.Do(context.Background())
	if err != nil || string(result.Result) != "1" || <-bodies != original || attempts.Load() != 2 {
		t.Fatalf("resumed: %+v, %v", result, err)
	}
}

func TestBodyLimitsAndInvalidResponses(t *testing.T) {
	responses := make(chan string, 3)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, <-responses) }))
	defer s.Close()
	client := testClient(t, s.URL)
	_, err := client.PrepareCreate(CreateRequest{ObjectID: strings.Repeat("x", maxRequest)})
	var failure *Error
	if !errors.As(err, &failure) || failure.Code != "LIMIT_EXCEEDED" || failure.UnknownOutcome {
		t.Fatalf("large create: %v", err)
	}
	_, err = client.RegisterType(context.Background(), Manifest{}, make([]byte, maxPackage))
	if !errors.As(err, &failure) || failure.Code != "LIMIT_EXCEEDED" {
		t.Fatalf("large package: %v", err)
	}
	responses <- strings.Repeat("x", maxResponse+1)
	if _, err = client.Read(context.Background(), "x"); err == nil {
		t.Fatal("accepted large response")
	}
	responses <- `{}`
	if _, err = invoke(t, client, "x").Do(context.Background()); !errors.As(err, &failure) || !failure.UnknownOutcome {
		t.Fatalf("missing result: %v", err)
	}
	responses <- `{"result":1}{"result":2}`
	if _, err = invoke(t, client, "x").Do(context.Background()); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}

func TestRedirectDoesNotForwardTokenOrMutation(t *testing.T) {
	var forwarded atomic.Bool
	second := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded.Store(true) }))
	defer second.Close()
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, second.URL, http.StatusTemporaryRedirect)
	}))
	defer first.Close()
	_, err := invoke(t, testClient(t, first.URL), "saved").Do(context.Background())
	if err == nil || forwarded.Load() {
		t.Fatalf("redirect: %v, forwarded: %t", err, forwarded.Load())
	}
}
