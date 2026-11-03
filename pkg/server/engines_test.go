package server

import (
	"bytes"
	"testing"
)

// TestStoreKeyForEngine: the switch to authenticated encryption marks each
// engine's registry under the key that seals it (QA on #247). A rotation
// interrupted between the two engines leaves the raft engine under its
// own key; marking it under the state engine's would fail with "encryption
// key does not match store" at every attempt, and the raft engine would
// never switch.
func TestStoreKeyForEngine(t *testing.T) {
	state, raft := []byte("state-key"), []byte("raft-key")
	if k, name := storeKeyForEngine(false, state, raft); !bytes.Equal(k, state) || name != "state" {
		t.Fatalf("state engine: %q %q", k, name)
	}
	if k, name := storeKeyForEngine(true, state, raft); !bytes.Equal(k, raft) || name != "raft" {
		t.Fatalf("raft engine under its own key: %q %q", k, name)
	}
	if k, _ := storeKeyForEngine(true, state, nil); !bytes.Equal(k, state) {
		t.Fatalf("raft engine with no key of its own: %q", k)
	}
}
