package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
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
	ID       int
	Version  int
	Schema   string
	Type     SchemaType
	Subject  string
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
	baseURL    string
	client     *http.Client
	mu         sync.RWMutex
	schemaCache map[int]*SchemaMetadata
}

func NewConfluentSchemaRegistry(baseURL string) *ConfluentSchemaRegistry {
	return &ConfluentSchemaRegistry{
		baseURL:     baseURL,
		client:      &http.Client{},
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/subjects/"+subject+"/versions", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("schema registry request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return 0, fmt.Errorf("schema registry post: %w", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id), nil)
	if err != nil {
		return nil, fmt.Errorf("schema registry request: %w", err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("schema registry get: %w", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
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
	url := fmt.Sprintf("%s/subjects/%s/versions/%d", c.baseURL, subject, version)
	if version == -1 {
		url = fmt.Sprintf("%s/subjects/%s/versions/latest", c.baseURL, subject)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("schema registry request: %w", err)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("schema registry get: %w", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
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
