package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type SchemaType int

const (
	SchemaTypeAvro SchemaType = iota
	SchemaTypeProtobuf
	SchemaTypeJSON
)

func (s SchemaType) String() string {
	switch s {
	case SchemaTypeAvro:
		return "AVRO"
	case SchemaTypeProtobuf:
		return "PROTOBUF"
	case SchemaTypeJSON:
		return "JSON"
	default:
		return "UNKNOWN"
	}
}

type SchemaMetadata struct {
	ID         int
	Version    int
	Schema     string
	Type       SchemaType
	Subject    string
	References []SchemaReference
}

type SchemaReference struct {
	Name    string
	Subject string
	Version int
}

type SchemaRegistry interface {
	Register(ctx context.Context, subject string, schema string, schemaType SchemaType) (int, error)
	Fetch(ctx context.Context, id int) (*SchemaMetadata, error)
	FetchBySubject(ctx context.Context, subject string, version int) (*SchemaMetadata, error)
}

type SchemaSerde[T any] interface {
	Serialize(ctx context.Context, msg Message[T], schemaID int) (Message[[]byte], error)
	Deserialize(ctx context.Context, msg Message[[]byte]) (Message[T], *SchemaMetadata, error)
}

type ConfluentSchemaRegistry struct {
	baseURL     string
	client      *http.Client
	basicUser   string
	basicPass   string
	bearerToken string
	mu          sync.RWMutex
	schemaCache map[int]*SchemaMetadata
}

type SchemaRegistryOption func(*ConfluentSchemaRegistry)

func NewConfluentSchemaRegistry(baseURL string) *ConfluentSchemaRegistry {
	return NewConfluentSchemaRegistryWithOptions(baseURL)
}

func NewConfluentSchemaRegistryWithOptions(baseURL string, opts ...SchemaRegistryOption) *ConfluentSchemaRegistry {
	client := &http.Client{Timeout: 10 * time.Second}
	registry := &ConfluentSchemaRegistry{
		baseURL:     strings.TrimRight(baseURL, "/"),
		client:      client,
		schemaCache: make(map[int]*SchemaMetadata),
	}
	for _, opt := range opts {
		opt(registry)
	}
	if registry.client == nil {
		registry.client = client
	}
	return registry
}

func WithSchemaRegistryHTTPClient(client *http.Client) SchemaRegistryOption {
	return func(c *ConfluentSchemaRegistry) {
		if client != nil {
			c.client = client
		}
	}
}

func WithSchemaRegistryBasicAuth(username, password string) SchemaRegistryOption {
	return func(c *ConfluentSchemaRegistry) {
		c.basicUser = username
		c.basicPass = password
	}
}

func WithSchemaRegistryBearerToken(token string) SchemaRegistryOption {
	return func(c *ConfluentSchemaRegistry) {
		c.bearerToken = token
	}
}

const maxSchemaRegistryErrorBody = 4096

func (c *ConfluentSchemaRegistry) endpoint(parts ...string) (string, error) {
	if c.baseURL == "" {
		return "", fmt.Errorf("schema registry: empty base URL")
	}
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return "", fmt.Errorf("schema registry base URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("schema registry: unsupported URL scheme %q", u.Scheme)
	}
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	escaped := make([]string, 0, len(parts))
	for _, part := range parts {
		escaped = append(escaped, url.PathEscape(part))
	}
	return strings.TrimRight(u.String(), "/") + "/" + strings.Join(escaped, "/"), nil
}

func (c *ConfluentSchemaRegistry) applyAuth(req *http.Request) {
	if c.basicUser != "" || c.basicPass != "" {
		req.SetBasicAuth(c.basicUser, c.basicPass)
	}
	if c.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	}
}

func NewConfluentSchemaRegistryWithClient(baseURL string, client *http.Client) *ConfluentSchemaRegistry {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &ConfluentSchemaRegistry{
		baseURL:     strings.TrimRight(baseURL, "/"),
		client:      client,
		schemaCache: make(map[int]*SchemaMetadata),
	}
}

type confluentRegisterRequest struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType,omitempty"`
}

type confluentRegisterResponse struct {
	ID int `json:"id"`
}

type confluentFetchResponse struct {
	ID         int    `json:"id"`
	Version    int    `json:"version"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType,omitempty"`
	Subject    string `json:"subject"`
}

func (c *ConfluentSchemaRegistry) Register(ctx context.Context, subject, schema string, schemaType SchemaType) (int, error) {
	req := confluentRegisterRequest{
		Schema: schema,
	}
	if schemaType != SchemaTypeJSON {
		req.SchemaType = schemaType.String()
	}

	body, err := json.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("schema registry marshal: %w", err)
	}

	endpoint, err := c.endpoint("subjects", subject, "versions")
	if err != nil {
		return 0, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("schema registry request: %w", err)
	}
	c.applyAuth(httpReq)
	httpReq.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("schema registry post: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxSchemaRegistryErrorBody))
		return 0, fmt.Errorf("schema registry: %s: %s", resp.Status, string(respBody))
	}

	var regResp confluentRegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return 0, fmt.Errorf("schema registry decode: %w", err)
	}

	return regResp.ID, nil
}

func (c *ConfluentSchemaRegistry) Fetch(ctx context.Context, id int) (*SchemaMetadata, error) {
	c.mu.RLock()
	if cached, ok := c.schemaCache[id]; ok {
		c.mu.RUnlock()
		return cached, nil
	}
	c.mu.RUnlock()

	endpoint, err := c.endpoint("schemas", "ids", fmt.Sprintf("%d", id))
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("schema registry request: %w", err)
	}
	c.applyAuth(httpReq)

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("schema registry get: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry: %s", resp.Status)
	}

	var fetchResp confluentFetchResponse
	if err := json.NewDecoder(resp.Body).Decode(&fetchResp); err != nil {
		return nil, fmt.Errorf("schema registry decode: %w", err)
	}

	meta := &SchemaMetadata{
		ID:     fetchResp.ID,
		Schema: fetchResp.Schema,
		Type:   parseSchemaType(fetchResp.SchemaType),
	}

	c.mu.Lock()
	c.schemaCache[id] = meta
	c.mu.Unlock()

	return meta, nil
}

func (c *ConfluentSchemaRegistry) FetchBySubject(ctx context.Context, subject string, version int) (*SchemaMetadata, error) {
	versionPart := fmt.Sprintf("%d", version)
	if version == -1 {
		versionPart = "latest"
	}
	endpoint, err := c.endpoint("subjects", subject, "versions", versionPart)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("schema registry request: %w", err)
	}
	c.applyAuth(httpReq)

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("schema registry get: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry: %s", resp.Status)
	}

	var fetchResp confluentFetchResponse
	if err := json.NewDecoder(resp.Body).Decode(&fetchResp); err != nil {
		return nil, fmt.Errorf("schema registry decode: %w", err)
	}

	return &SchemaMetadata{
		ID:      fetchResp.ID,
		Version: fetchResp.Version,
		Schema:  fetchResp.Schema,
		Type:    parseSchemaType(fetchResp.SchemaType),
		Subject: fetchResp.Subject,
	}, nil
}

func parseSchemaType(s string) SchemaType {
	switch s {
	case "AVRO":
		return SchemaTypeAvro
	case "PROTOBUF":
		return SchemaTypeProtobuf
	case "JSON":
		return SchemaTypeJSON
	default:
		return SchemaTypeJSON
	}
}

type AvroSerde[T any] struct {
	registry SchemaRegistry
}

func NewAvroSerde[T any](registry SchemaRegistry) *AvroSerde[T] {
	return &AvroSerde[T]{registry: registry}
}

func (a *AvroSerde[T]) Serialize(ctx context.Context, msg Message[T], schemaID int) (Message[[]byte], error) {
	data, err := json.Marshal(msg.Value)
	if err != nil {
		return Message[[]byte]{}, fmt.Errorf("avro serialize: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteByte(0x00)
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(schemaID))
	buf.Write(idBytes)
	buf.Write(data)

	return Message[[]byte]{
		Key:       msg.Key,
		Value:     buf.Bytes(),
		Headers:   msg.Headers,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
	}, nil
}

func (a *AvroSerde[T]) Deserialize(ctx context.Context, msg Message[[]byte]) (Message[T], *SchemaMetadata, error) {
	if len(msg.Value) < 5 {
		return Message[T]{}, nil, fmt.Errorf("avro: message too short")
	}

	schemaID := int(binary.BigEndian.Uint32(msg.Value[1:5]))
	payload := msg.Value[5:]

	meta, err := a.registry.Fetch(ctx, schemaID)
	if err != nil {
		return Message[T]{}, nil, fmt.Errorf("avro fetch schema: %w", err)
	}

	var val T
	if err := json.Unmarshal(payload, &val); err != nil {
		return Message[T]{}, nil, fmt.Errorf("avro deserialize: %w", err)
	}

	result := Message[T]{
		Key:       msg.Key,
		Value:     val,
		Headers:   msg.Headers,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
	}

	return result, meta, nil
}
