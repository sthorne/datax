package kvclient

import (
	"errors"
	"math"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/storage"
)

func condFailed(k string) *kvpb.Error {
	e := kvpb.NewErrorf("conditional put failed on %s", k)
	e.ConditionFailed = &kvpb.ConditionFailedError{Key: keys.Key(k)}
	return e
}

// A failed conditional put reports the error it was buffered with — the
// first put of the key's — and any other failure passes through.
func TestWriteBatchConditionFailure(t *testing.T) {
	errA, errB := errors.New("a taken"), errors.New("b taken")
	var b WriteBatch
	b.Put(keys.Key("p"), []byte("v"))
	if b.HasCPuts() {
		t.Fatal("a batch of puts has conditional puts")
	}
	b.CPut(keys.Key("a"), []byte("v"), errA)
	b.Delete(keys.Key("d"))
	b.CPut(keys.Key("b"), []byte("v"), errB)
	if !b.HasCPuts() || len(b.fails) != len(b.reqs) {
		t.Fatalf("fails %d for %d requests", len(b.fails), len(b.reqs))
	}
	for _, c := range []struct {
		kerr *kvpb.Error
		want error
		key  string
	}{
		{condFailed("a"), errA, "a"},
		{condFailed("b"), errB, "b"},
	} {
		key, ok, got := b.conditionFailure(c.kerr)
		if !ok || got != c.want || string(key) != c.key {
			t.Fatalf("%v: got %v %q %v", c.kerr, got, key, ok)
		}
	}
	other := kvpb.NewErrorf("something else")
	if _, ok, got := b.conditionFailure(other); ok || got != error(other) {
		t.Fatalf("an unrelated error: %v %v", got, ok)
	}
	// A key the batch has no conditional put for (a plain put's) is not
	// the batch's failure.
	if _, ok, _ := b.conditionFailure(condFailed("p")); ok {
		t.Fatal("a plain put's key taken for a failed conditional put")
	}

	// Appended to a pipelined batch, the failures keep their requests'
	// positions.
	var deferred WriteBatch
	deferred.Put(keys.Key("x"), nil)
	deferred.appendBatch(&b)
	deferred.Put(keys.Key("y"), nil)
	if len(deferred.fails) != len(deferred.reqs) || deferred.fails[2] != errA || deferred.fails[4] != errB {
		t.Fatalf("appended fails %v", deferred.fails)
	}
	var plain WriteBatch
	plain.Put(keys.Key("z"), nil)
	deferred.appendBatch(&plain)
	if len(deferred.fails) != len(deferred.reqs) {
		t.Fatalf("fails %d for %d requests", len(deferred.fails), len(deferred.reqs))
	}
}

// Among a batch's ranges, the failed conditional put reported is the one
// earliest in the whole batch.
func TestConditionFailedAt(t *testing.T) {
	g := rangeGroup{indices: []int{1, 4, 6}}
	g.reqs = []kvpb.RequestUnion{
		{Put: &kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key("a")}}},
		{CPut: &kvpb.CPutRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key("b")}}},
		{CPut: &kvpb.CPutRequest{RequestHeader: kvpb.RequestHeader{Key: keys.Key("c")}}},
	}
	if at := conditionFailedAt(g, condFailed("b")); at != 4 {
		t.Fatalf("b at %d", at)
	}
	if at := conditionFailedAt(g, condFailed("c")); at != 6 {
		t.Fatalf("c at %d", at)
	}
	if at := conditionFailedAt(g, condFailed("a")); at != math.MaxInt {
		t.Fatalf("a plain put's key at %d", at)
	}
}

// A conflict on a conditional put's key is the read the put replaces,
// and waits for a live owner; any other conflict in the batch is a
// write's, and may abort the owner by priority.
func TestOnlyCPutConflicts(t *testing.T) {
	var b WriteBatch
	b.CPut(keys.Key("a"), []byte("v"), nil)
	b.Put(keys.Key("p"), []byte("v"))
	ba := &kvpb.BatchRequest{Requests: b.reqs}
	at := func(ks ...string) []storage.Intent {
		var out []storage.Intent
		for _, k := range ks {
			out = append(out, storage.Intent{Key: keys.Key(k)})
		}
		return out
	}
	for _, c := range []struct {
		intents []storage.Intent
		want    bool
	}{
		{at("a"), true},
		{at("p"), false},
		{at("a", "p"), false},
		{nil, false},
	} {
		if got := onlyCPutConflicts(ba, c.intents); got != c.want {
			t.Fatalf("%v: got %v, want %v", c.intents, got, c.want)
		}
	}
}
