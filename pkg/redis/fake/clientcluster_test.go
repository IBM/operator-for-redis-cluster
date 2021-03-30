package fake

import (
	"github.com/mediocregopher/radix/v3/resp/resp2"
	"testing"
)

func TestCmd(t *testing.T) {
	mock := NewClientCluster()
	mock.Resps["CLUSTER NODES"] = &resp2.Any{I: "someanswer"}
	resp := mock.Cmd("CLUSTER", "NODES")
	val, ok := resp.I.(string)
	if !ok {
		t.Fail()
	}
	if val != "someanswer" {
		t.Errorf("Expected '%s', got '%s'", "someanswer", val)
	}
}
