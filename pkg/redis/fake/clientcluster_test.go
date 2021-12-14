package fake

import (
	"context"
	"testing"
)

func TestDoCmd(t *testing.T) {
	mock := NewClientCluster()
	ctx := context.Background()
	mock.Resps["CLUSTER NODES"] = "someanswer"
	var resp interface{}
	err := mock.DoCmd(ctx, &resp, "CLUSTER", "NODES")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if resp != "someanswer" {
		t.Errorf("Expected '%s', got '%s'", "someanswer", resp)
	}
}
