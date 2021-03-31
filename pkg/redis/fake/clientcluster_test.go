package fake

import (
	"testing"
)

func TestCmd(t *testing.T) {
	mock := NewClientCluster()
	mock.Resps["CLUSTER NODES"] = "someanswer"
	resp := mock.Cmd("CLUSTER", "NODES")
	val, ok := resp.(string)
	if !ok {
		t.Fail()
	}
	if val != "someanswer" {
		t.Errorf("Expected '%s', got '%s'", "someanswer", val)
	}
}
