// Package sdk calls the Duro router's public HTTP/JSON API.
package sdk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

const (
	maxRequest      = 64 << 10
	maxPackage      = 1 << 20
	maxRegistration = 2 << 20
)

type Client struct {
	endpoints []string
	token     string
	http      *http.Client
}

// NewClient accepts router base URLs in failover order.
func NewClient(endpoints []string, token string) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("duro: at least one router URL is required")
	}
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, errors.New("duro: invalid bearer token")
	}
	client := &Client{token: token, http: &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	for _, endpoint := range endpoints {
		u, err := url.Parse(endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" ||
			u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return nil, errors.New("duro: invalid router URL")
		}
		client.endpoints = append(client.endpoints, strings.TrimRight(u.String(), "/"))
	}
	return client, nil
}

type Manifest struct {
	Type    string   `json:"type"`
	Version string   `json:"version"`
	Runtime string   `json:"runtime"`
	Format  string   `json:"format"`
	Methods []string `json:"methods"`
	SHA256  string   `json:"sha256"`
}

type Registration struct {
	Type    string `json:"type"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
}

func (c *Client) RegisterType(ctx context.Context, manifest Manifest, bundle []byte) (Registration, error) {
	var result Registration
	rawManifest, err := json.Marshal(manifest)
	if err != nil {
		return result, err
	}
	if len(rawManifest) > maxPackage || len(bundle) > maxPackage-len(rawManifest) {
		return result, tooLarge("")
	}
	request := struct {
		Manifest json.RawMessage `json:"manifest"`
		Bundle   []byte          `json:"bundle"`
	}{rawManifest, bundle}
	err = c.call(ctx, "registerType", request, maxRegistration, &result)
	if err == nil && (result.Type == "" || result.Version == "" || result.SHA256 == "") {
		err = invalidResponse("registerType", "")
	}
	return result, err
}

type CreateRequest struct {
	ObjectID  string          `json:"object_id"`
	Type      string          `json:"type"`
	Version   string          `json:"version"`
	Input     json.RawMessage `json:"input"`
	RequestID string          `json:"request_id"`
}

type InvokeRequest struct {
	ObjectID  string          `json:"object_id"`
	Method    string          `json:"method"`
	Argument  json.RawMessage `json:"argument"`
	RequestID string          `json:"request_id"`
}

type Result struct {
	ObjectID string          `json:"object_id,omitempty"`
	Result   json.RawMessage `json:"result"`
}

// A Mutation holds the encoded request, including its deduplication key.
type Mutation struct {
	client    *Client
	operation string
	id        string
	body      []byte
}

func (c *Client) PrepareCreate(request CreateRequest) (*Mutation, error) {
	return c.prepare("create", &request.RequestID, &request)
}

func (c *Client) PrepareInvoke(request InvokeRequest) (*Mutation, error) {
	return c.prepare("invoke", &request.RequestID, &request)
}

func (c *Client) prepare(operation string, id *string, request any) (*Mutation, error) {
	if *id == "" {
		var key [16]byte
		if _, err := rand.Read(key[:]); err != nil {
			return nil, err
		}
		*id = hex.EncodeToString(key[:])
	}
	body, err := marshalBody(request, *id, maxRequest)
	if err != nil {
		return nil, err
	}
	return &Mutation{client: c, operation: operation, id: *id, body: body}, nil
}

// RequestID can be persisted before Do sends the first request.
func (m *Mutation) RequestID() string { return m.id }

// Do can be called again with a fresh context after an unknown outcome.
func (m *Mutation) Do(ctx context.Context) (Result, error) {
	var result Result
	err := m.client.send(ctx, m.operation, m.id, m.body, &result)
	if err == nil && (result.Result == nil || len(result.Result) > maxRequest || (m.operation == "create" && result.ObjectID == "")) {
		err = invalidResponse(m.operation, m.id)
	}
	if err != nil {
		return Result{}, err
	}
	return result, nil
}

type Description struct {
	ObjectID string   `json:"object_id"`
	Type     string   `json:"type"`
	Version  string   `json:"version"`
	Methods  []string `json:"methods"`
}

type objectRequest struct {
	ObjectID string `json:"object_id"`
}

func (c *Client) Describe(ctx context.Context, objectID string) (Description, error) {
	var result Description
	err := c.call(ctx, "describe", objectRequest{objectID}, maxRequest, &result)
	if err == nil && (result.ObjectID == "" || result.Type == "" || result.Version == "" || result.Methods == nil) {
		err = invalidResponse("describe", "")
	}
	return result, err
}

func (c *Client) Read(ctx context.Context, objectID string) (map[string]json.RawMessage, error) {
	var result struct {
		KV map[string]json.RawMessage `json:"kv"`
	}
	err := c.call(ctx, "read", objectRequest{objectID}, maxRequest, &result)
	if err == nil && result.KV == nil {
		err = invalidResponse("read", "")
	}
	return result.KV, err
}

func (c *Client) call(ctx context.Context, operation string, request any, limit int, result any) error {
	body, err := marshalBody(request, "", limit)
	if err != nil {
		return err
	}
	return c.send(ctx, operation, "", body, result)
}

func marshalBody(request any, id string, limit int) ([]byte, error) {
	body, err := json.Marshal(request)
	if err == nil && len(body) > limit {
		return nil, tooLarge(id)
	}
	return body, err
}
