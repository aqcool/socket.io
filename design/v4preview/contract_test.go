package v4preview

import (
	"context"
	"testing"
)

type previewSubscription struct{}

func (previewSubscription) Close() error { return nil }

type previewEndpoint struct{}

func (*previewEndpoint) Emit(string, ...any) error { return nil }

func (*previewEndpoint) EmitAck(_ context.Context, _ string, _ ...any) ([]any, error) {
	return []any{previewResponse{ID: "ok"}}, nil
}

func (*previewEndpoint) On(string, Listener) Subscription   { return previewSubscription{} }
func (*previewEndpoint) Once(string, Listener) Subscription { return previewSubscription{} }
func (*previewEndpoint) DecodeValue(any, any) error         { return nil }

type previewRequest struct {
	Text string
}

type previewResponse struct {
	ID string
}

func TestTypedContractCompiles(t *testing.T) {
	endpoint := &previewEndpoint{}
	event := NewEvent[previewRequest, previewResponse]("message:send")

	if err := Emit(endpoint, event, previewRequest{Text: "hello"}); err != nil {
		t.Fatal(err)
	}

	response, err := EmitAck(context.Background(), endpoint, event, previewRequest{Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if response.ID != "ok" {
		t.Fatalf("unexpected response: %#v", response)
	}

	sub := Handle(endpoint, event, func(context.Context, previewRequest) (previewResponse, error) {
		return previewResponse{ID: "handled"}, nil
	})
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOptionContract(t *testing.T) {
	cfg := DefaultConfig()
	for _, option := range []Option{
		WithPath("/realtime/"),
		WithClientServing(false),
		WithConnectTimeout(cfg.ConnectTimeout),
		WithQueue(QueueOptions{MaxPending: 1024, Overflow: OverflowReject}),
	} {
		if err := option(&cfg); err != nil {
			t.Fatal(err)
		}
	}
	if cfg.Path != "/realtime" || cfg.Queue.MaxPending != 1024 {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}
