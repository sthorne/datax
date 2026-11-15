package server

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/util/hlc"
)

// memResume is a resumeStore in memory.
type memResume struct{ raw []byte }

func (m *memResume) load(context.Context) ([]byte, error) { return m.raw, nil }
func (m *memResume) save(_ context.Context, raw []byte) error {
	m.raw = append([]byte(nil), raw...)
	return nil
}

func descRec(t *testing.T, d catalog.TableDescriptor) kvpb.ExportRecord {
	t.Helper()
	raw, err := json.Marshal(&d)
	if err != nil {
		t.Fatal(err)
	}
	return kvpb.ExportRecord{Key: keys.TableDescKey(d.ID), Value: raw}
}

// TestFollowedStatesRoundTrip: what a job keeps decodes from its
// persisted form to an equal state, which encodes alike (#316).
func TestFollowedStatesRoundTrip(t *testing.T) {
	retired := func() followedState {
		s := &rawDescs{keep: func(d *catalog.TableDescriptor) bool { return len(d.RetiredLayouts) > 0 }}
		s.reset()
		return s
	}
	rd := retired()
	rd.apply(descRec(t, catalog.TableDescriptor{ID: 7, Name: "a", RetiredLayouts: []catalog.RetiredLayout{{PrimaryIndexID: 1}}}))
	rd.apply(descRec(t, catalog.TableDescriptor{ID: 9, Name: "b"}))
	rd.apply(descRec(t, catalog.TableDescriptor{ID: 300, Name: "c", RetiredLayouts: []catalog.RetiredLayout{{PrimaryIndexID: 2}}}))
	if got := len(rd.(*rawDescs).byID); got != 2 {
		t.Fatalf("kept %d descriptors, want 2", got)
	}
	if !rd.apply(kvpb.ExportRecord{Key: keys.TableDescKey(300), Deleted: true}) || rd.apply(kvpb.ExportRecord{Key: keys.TableDescKey(9), Deleted: true}) {
		t.Fatal("a removal reported the wrong change")
	}

	st := &statsState{}
	st.reset()
	st.tables[3], st.tables[70000], st.tables[70001] = true, true, true
	st.collected[3] = time.Now().UnixNano()
	st.collected[5] = 0 // corrupt record
	st.collected[70001] = math.MinInt64

	empty := &statsState{}
	empty.reset()

	for name, s := range map[string]followedState{"rawDescs": rd, "stats": st, "empty stats": empty} {
		enc := s.encode()
		var back followedState
		if _, ok := s.(*rawDescs); ok {
			back = retired()
		} else {
			back = &statsState{}
		}
		if err := back.decode(enc); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !reflect.DeepEqual(back, s) && name != "rawDescs" {
			t.Fatalf("%s: decoded %+v, want %+v", name, back, s)
		}
		if string(back.encode()) != string(enc) {
			t.Fatalf("%s: re-encoded differently", name)
		}
		for i := 0; i < len(enc); i++ {
			if back.decode(enc[:i]) == nil {
				t.Fatalf("%s: a record cut to %d of %d bytes decoded", name, i, len(enc))
			}
		}
		if back.decode(append(enc[:len(enc):len(enc)], 0)) == nil {
			t.Fatalf("%s: a record with a trailing byte decoded", name)
		}
	}
}

// TestCatalogFollowerResumeRecord: a record of the job's version is used
// whole; one of another version, one that does not decode, or one too
// far ahead of the clock is not, and the follower starts empty for a
// full read (#316).
func TestCatalogFollowerResumeRecord(t *testing.T) {
	ctx := context.Background()
	clock := hlc.NewClock(func() int64 { return time.Now().UnixNano() }, 500*time.Millisecond)
	n := &Node{clock: clock}
	fresh := func() followedState {
		s := &statsState{}
		s.reset()
		return s
	}
	st := fresh().(*statsState)
	st.tables[42] = true
	since := clock.Now()
	good := encodeResume(3, since, time.Now(), st.encode())

	for _, tc := range []struct {
		name   string
		raw    []byte
		resume bool
	}{
		{"same version", good, true},
		{"other version", encodeResume(4, since, time.Now(), st.encode()), false},
		{"no timestamp", encodeResume(3, hlc.Timestamp{}, time.Now(), st.encode()), false},
		{"truncated", good[:len(good)-1], false},
		{"garbage", []byte{0xff}, false},
		{"ahead of the clock", encodeResume(3, since.AddNanos(time.Hour.Nanoseconds()), time.Now(), st.encode()), false},
	} {
		f := newCatalogFollower("stats", n, nil, fresh, &memResume{raw: tc.raw}, 3, time.Minute)
		f.resume(ctx)
		if f.resumed != tc.resume {
			t.Fatalf("%s: resumed %v, want %v", tc.name, f.resumed, tc.resume)
		}
		if tc.resume && (f.w.since != since || !f.state.(*statsState).tables[42]) {
			t.Fatalf("%s: resumed at %s with %+v", tc.name, f.w.since, f.state)
		}
		if !tc.resume && (!f.w.since.IsEmpty() || len(f.state.(*statsState).tables) != 0) {
			t.Fatalf("%s: not resumed, but at %s with %+v", tc.name, f.w.since, f.state)
		}
	}
	if clock.Now().LessEq(since) {
		t.Fatal("the clock did not move past a resumed timestamp")
	}
}
