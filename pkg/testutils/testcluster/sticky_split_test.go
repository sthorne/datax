package testcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sthorne/datax/pkg/base"
	"github.com/sthorne/datax/pkg/cluster"
	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/version"
)

// Issue #265: a range boundary an operator made on purpose — ALTER TABLE
// ... SPLIT AT ahead of a load, datax debug split — was folded back by
// the merge queue within seconds, because the two halves of a pre-split
// are small and that is all the queue asked. A manual split now records a
// sticky bit on the range it creates; the queue leaves that range's start
// boundary alone until UNSPLIT clears the bit or its expiration passes.

// mergePasses runs the merge queue once on every node, n times over. A
// merge can take several passes (the first may only pull the right-hand
// range's leadership over), so callers that expect NO merge run a
// generous number and callers that expect one poll with mergeUntil.
func mergePasses(ctx context.Context, tc *TestCluster, n int) {
	for i := 0; i < n; i++ {
		for _, node := range tc.Nodes {
			node.Store().RunRangeMergeOnce(ctx)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// boundaries returns the start keys of the ranges that begin strictly
// inside [lo, hi), in key order, and the descriptors that begin there.
func boundaries(t *testing.T, ctx context.Context, tc *TestCluster, lo, hi keys.Key) (starts []string, descs []kvpb.RangeDescriptor) {
	t.Helper()
	all, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range all {
		if d.StartKey.Compare(lo) > 0 && d.StartKey.Compare(hi) < 0 {
			starts = append(starts, strings.TrimPrefix(string(d.StartKey), string(lo)))
			descs = append(descs, d)
		}
	}
	return starts, descs
}

// mergeUntil runs merge passes until the boundaries inside [lo, hi) are
// exactly want, and returns how many passes that took.
func mergeUntil(t *testing.T, ctx context.Context, tc *TestCluster, lo, hi keys.Key, what string, want ...string) int {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for passes := 1; ; passes++ {
		mergePasses(ctx, tc, 1)
		got, _ := boundaries(t, ctx, tc, lo, hi)
		if strings.Join(got, ",") == strings.Join(want, ",") {
			return passes
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: boundaries %q after %d merge passes, want %q", what, got, passes, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// stickyEverywhere checks that every replica of the range holds the same
// sticky bit — it is replicated state, so a replica that disagreed would
// make a different merge decision the day it led the range.
func stickyEverywhere(t *testing.T, tc *TestCluster, id base.RangeID, want int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		held, ok := 0, true
		for _, n := range tc.Nodes {
			if d, has := n.Store().LocalDescriptor(id); has {
				held++
				ok = ok && d.StickyUntil == want
			}
		}
		if ok && held == len(tc.Nodes) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: sticky bit %d is not on all %d replicas", id, want, len(tc.Nodes))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestManualSplitSurvivesTheMergeQueue: two tiny halves made by an
// automatic split merge back; the same two halves made by a manual split
// stay apart through many more passes of the merge queue than the merge
// took; a later automatic split inside the sticky range is not itself
// sticky; and after an unsplit the halves merge like any others.
func TestManualSplitSurvivesTheMergeQueue(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, func(cfg *server.Config) {
		cfg.MergeSizeThreshold = 1 << 20
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	prefix := keys.TableDataPrefix(871)
	lo, hi := prefix, prefix.PrefixEnd()
	key := func(s string) keys.Key { return append(prefix.Clone(), s...) }
	for _, k := range []string{"a", "p", "x"} {
		if err := db.Put(ctx, key(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	// The control: an automatic split of these halves is merged back.
	// This is what the merge queue is for, and it must keep doing it.
	if _, err := db.AdminSplit(ctx, key("m")); err != nil {
		t.Fatal(err)
	}
	if got, _ := boundaries(t, ctx, tc, lo, hi); len(got) != 1 || got[0] != "m" {
		t.Fatalf("after the automatic split: boundaries %q", got)
	}
	took := mergeUntil(t, ctx, tc, lo, hi, "automatic split")

	// The same split, made manually. Give the queue several times the
	// passes the merge above needed.
	sr, err := db.AdminSplitSticky(ctx, key("m"), kvpb.StickyForever)
	if err != nil {
		t.Fatal(err)
	}
	if sr.Left.StickyUntil != 0 || sr.Right.StickyUntil != kvpb.StickyForever {
		t.Fatalf("manual split produced %v / %v: only the right half's start boundary is the manual one", sr.Left, sr.Right)
	}
	stickyEverywhere(t, tc, sr.Right.RangeID, kvpb.StickyForever)
	patience := 5*took + 20
	mergePasses(ctx, tc, patience)
	got, descs := boundaries(t, ctx, tc, lo, hi)
	if len(got) != 1 || got[0] != "m" {
		t.Fatalf("manual split merged away: boundaries %q after %d passes", got, patience)
	}
	// /meta carries the bit too: it is where SHOW-style listings and
	// UNSPLIT ALL read it.
	if descs[0].RangeID != sr.Right.RangeID || descs[0].StickyUntil != kvpb.StickyForever {
		t.Fatalf("/meta holds %v, want the sticky right half %v", descs[0], sr.Right)
	}

	// A manual split of a boundary that exists is not an error and not a
	// second split; it leaves the bit as asked.
	again, err := db.AdminSplitSticky(ctx, key("m"), kvpb.StickyForever)
	if err != nil || again.Left.RangeID != 0 || again.Right.RangeID != sr.Right.RangeID {
		t.Fatalf("manual split of an existing boundary: %+v, %v", again, err)
	}

	// An automatic split inside the sticky range: its right half starts
	// at a boundary nobody asked for, so it is not sticky and merges
	// back, while the left half keeps the bit and the boundary at m.
	auto, err := db.AdminSplit(ctx, key("t"))
	if err != nil {
		t.Fatal(err)
	}
	if auto.Left.StickyUntil != kvpb.StickyForever || auto.Right.StickyUntil != 0 {
		t.Fatalf("automatic split inside a sticky range produced %v / %v", auto.Left, auto.Right)
	}
	mergeUntil(t, ctx, tc, lo, hi, "automatic split inside the sticky range", "m")
	// The merged range starts at m and is still the sticky one.
	stickyEverywhere(t, tc, sr.Right.RangeID, kvpb.StickyForever)
	mergePasses(ctx, tc, patience)
	if got, _ := boundaries(t, ctx, tc, lo, hi); len(got) != 1 || got[0] != "m" {
		t.Fatalf("sticky boundary lost after a merge to its right: boundaries %q", got)
	}

	// Unsplit: the bit goes, on every replica, and the next passes merge.
	if _, err := db.AdminUnsplit(ctx, key("q")); err == nil || !strings.Contains(err.Error(), "not a range boundary") {
		t.Fatalf("unsplit at a key that is no boundary: %v", err)
	}
	ur, err := db.AdminUnsplit(ctx, key("m"))
	if err != nil {
		t.Fatal(err)
	}
	if ur.Desc.RangeID != sr.Right.RangeID || ur.Desc.StickyUntil != 0 {
		t.Fatalf("unsplit left %v", ur.Desc)
	}
	stickyEverywhere(t, tc, sr.Right.RangeID, 0)
	mergeUntil(t, ctx, tc, lo, hi, "after the unsplit")
	for _, k := range []string{"a", "p", "x"} {
		if v, err := tc.Nodes[1].DB().Get(ctx, key(k)); err != nil || string(v) != "v" {
			t.Fatalf("read %q after the merges: %q, %v", k, v, err)
		}
	}
}

// TestManualSplitExpires: a sticky bit with an expiration holds the
// boundary until then and no longer.
func TestManualSplitExpires(t *testing.T) {
	tc, _ := StartWithEngines(t, 3, func(cfg *server.Config) {
		cfg.MergeSizeThreshold = 1 << 20
	})
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	db := tc.Nodes[0].DB()

	prefix := keys.TableDataPrefix(873)
	lo, hi := prefix, prefix.PrefixEnd()
	if err := db.Put(ctx, append(prefix.Clone(), "a"...), []byte("v")); err != nil {
		t.Fatal(err)
	}
	until := tc.Nodes[0].Clock().Now().WallTime + int64(3*time.Second)
	sr, err := db.AdminSplitSticky(ctx, append(prefix.Clone(), "m"...), until)
	if err != nil {
		t.Fatal(err)
	}
	stickyEverywhere(t, tc, sr.Right.RangeID, until)
	// Held while the expiration is ahead. (The check is skipped on a
	// machine so slow that the passes themselves outlast it.)
	mergePasses(ctx, tc, 10)
	stillHeld := tc.Nodes[0].Clock().Now().WallTime < until
	if got, _ := boundaries(t, ctx, tc, lo, hi); stillHeld && len(got) != 1 {
		t.Fatalf("boundary merged before its expiration: %q", got)
	}
	// Past it, the queue merges the halves without anyone unsplitting.
	mergeUntil(t, ctx, tc, lo, hi, "after the expiration")
	if now := tc.Nodes[0].Clock().Now().WallTime; now < until {
		t.Fatalf("merged %s before the expiration", time.Duration(until-now))
	}
}

// TestSplitAtIsSticky: the SQL surface. ALTER TABLE ... SPLIT AT makes
// boundaries the merge queue keeps (they are the pre-split of an empty
// table: exactly what it would otherwise fold back), WITH EXPIRATION
// bounds one, UNSPLIT AT releases the ones named and UNSPLIT ALL the
// rest, and datax debug split / unsplit do the same for a raw key.
func TestSplitAtIsSticky(t *testing.T) {
	tc := StartWithOptions(t, 3, func(cfg *server.Config) { cfg.MergeSizeThreshold = 1 << 20 })
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := leasedSession(t, tc, 0, 3*time.Second)
	waitForDatabases(t, ctx, s)

	execSQL(t, ctx, s, `CREATE TABLE st (a INT8 PRIMARY KEY, v INT8)`)
	execSQL(t, ctx, s, `INSERT INTO st VALUES (5, 1), (15, 2), (25, 3), (35, 4)`)
	res := execSQL(t, ctx, s, `ALTER TABLE st SPLIT AT VALUES (10), (20), (30)`)
	if res.Tag != "ALTER TABLE 3" {
		t.Fatalf("split at: %+v", res)
	}
	// The boundaries live under the table's primary index; the longest
	// prefix the first and last of them share is inside it and covers
	// all three, which is all the checks below need.
	all, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var first, last keys.Key
	for _, d := range all {
		// Named the way the statement names them: the node's table-name
		// cache may not have caught up with a table this new.
		id, _ := keys.TableIDOf(d.StartKey)
		switch keys.PrettyForTable(d.StartKey, id, "st") {
		case res.Rows[0][0].Text():
			first = d.StartKey
		case res.Rows[2][0].Text():
			last = d.StartKey
		}
	}
	if first == nil || last == nil {
		t.Fatalf("no ranges start at %s and %s: %v", res.Rows[0][0].Text(), res.Rows[2][0].Text(), all)
	}
	n := 0
	for n < len(first) && n < len(last) && first[n] == last[n] {
		n++
	}
	lo := first[:n].Clone()
	hi := lo.PrefixEnd()
	count := func() int {
		got, _ := boundaries(t, ctx, tc, lo, hi)
		return len(got)
	}
	// mergeTo runs the merge queue until that many boundaries are left.
	mergeTo := func(want int, what string) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for count() != want {
			if time.Now().After(deadline) {
				t.Fatalf("%s: %d boundaries, want %d", what, count(), want)
			}
			mergePasses(ctx, tc, 1)
			time.Sleep(100 * time.Millisecond)
		}
	}

	mergePasses(ctx, tc, 40)
	if _, descs := boundaries(t, ctx, tc, lo, hi); len(descs) != 3 {
		t.Fatalf("SPLIT AT boundaries merged away: %d left of 3", len(descs))
	} else {
		for _, d := range descs {
			if d.StickyUntil != kvpb.StickyForever {
				t.Fatalf("SPLIT AT boundary %v is not sticky", d)
			}
		}
	}

	// An expiration: an interval from now, or a moment; never the past.
	before := tc.Nodes[0].Clock().Now().WallTime
	execSQL(t, ctx, s, `ALTER TABLE st SPLIT AT VALUES (30) WITH EXPIRATION INTERVAL '1 hour'`)
	_, descs := boundaries(t, ctx, tc, lo, hi)
	if len(descs) != 3 {
		t.Fatalf("re-splitting an existing boundary changed the ranges: %v", descs)
	}
	if got := descs[2].StickyUntil - before; got < int64(time.Hour) || got > int64(time.Hour+time.Minute) {
		t.Fatalf("WITH EXPIRATION INTERVAL '1 hour' holds the boundary for %s", time.Duration(got))
	}
	execSQL(t, ctx, s, `ALTER TABLE st SPLIT AT VALUES (30) WITH EXPIRATION '2200-01-01 00:00:00+00'`)
	expectCode := func(q, code string) {
		t.Helper()
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != code {
			t.Fatalf("%s: want %s, got %v", q, code, serr)
		}
	}
	expectCode(`ALTER TABLE st SPLIT AT VALUES (30) WITH EXPIRATION '2001-01-01 00:00:00+00'`, sql.CodeInvalidParameterValue)
	expectCode(`ALTER TABLE st SPLIT AT VALUES (30) WITH EXPIRATION 7`, sql.CodeInvalidParameterValue)
	expectCode(`ALTER TABLE st SPLIT AT VALUES (30) WITH EXPIRATION NULL`, sql.CodeInvalidParameterValue)

	// UNSPLIT AT releases what it names; the others stay.
	if _, serr := trySQL(ctx, s, `ALTER TABLE st UNSPLIT AT VALUES (11)`); serr == nil || !strings.Contains(serr.Msg, "not a range boundary") {
		t.Fatalf("UNSPLIT AT a key that is no boundary: %v", serr)
	}
	if res := execSQL(t, ctx, s, `ALTER TABLE st UNSPLIT AT VALUES (10)`); res.Tag != "ALTER TABLE 1" || !strings.HasSuffix(res.Rows[0][0].Text(), "/10") {
		t.Fatalf("unsplit at: %+v", res)
	}
	mergeTo(2, "after UNSPLIT AT (10)")
	mergePasses(ctx, tc, 40)
	if count() != 2 {
		t.Fatalf("UNSPLIT AT (10) released other boundaries too: %d left of 2", count())
	}

	// UNSPLIT ALL releases the rest, and reports them.
	if res := execSQL(t, ctx, s, `ALTER TABLE st UNSPLIT ALL`); res.Tag != "ALTER TABLE 2" {
		t.Fatalf("unsplit all: %+v", res)
	}
	mergeTo(0, "after UNSPLIT ALL")
	if res := execSQL(t, ctx, s, `ALTER TABLE st UNSPLIT ALL`); res.Tag != "ALTER TABLE 0" {
		t.Fatalf("unsplit all with nothing to release: %+v", res)
	}
	if r := execSQL(t, ctx, s, `SELECT count(*), sum(v) FROM st`); r.Rows[0][0].I != 4 || r.Rows[0][1].I != 10 {
		t.Fatalf("rows after the merges: %+v", r.Rows)
	}
	execSQL(t, ctx, s, `ALTER TABLE IF EXISTS nope UNSPLIT ALL`)
	execSQL(t, ctx, s, `BEGIN`)
	expectCode(`ALTER TABLE st UNSPLIT ALL`, sql.CodeActiveTransaction)
	execSQL(t, ctx, s, `ROLLBACK`)

	// TRUNCATE and DROP TABLE release what the table still holds: its old
	// key space is empty for good, and no statement could name a boundary
	// there afterwards.
	execSQL(t, ctx, s, `ALTER TABLE st SPLIT AT VALUES (10), (20)`)
	mergePasses(ctx, tc, 20)
	if count() != 2 {
		t.Fatalf("re-split boundaries: %d, want 2", count())
	}
	execSQL(t, ctx, s, `TRUNCATE st`)
	mergeTo(0, "after TRUNCATE")
	execSQL(t, ctx, s, `INSERT INTO st VALUES (5, 1), (15, 2)`)
	res = execSQL(t, ctx, s, `ALTER TABLE st SPLIT AT VALUES (10)`)
	held := func() *kvpb.RangeDescriptor {
		all, err := tc.ranges(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range all {
			if keys.Pretty(d.StartKey) == res.Rows[0][0].Text() {
				return &d
			}
		}
		return nil
	}
	mergePasses(ctx, tc, 20)
	if d := held(); d == nil || d.StickyUntil != kvpb.StickyForever {
		t.Fatalf("boundary in the truncated table's new key space: %v", d)
	}
	execSQL(t, ctx, s, `DROP TABLE st`)
	deadline := time.Now().Add(60 * time.Second)
	for held() != nil {
		if time.Now().After(deadline) {
			t.Fatalf("DROP TABLE left its manual boundary: %v", held())
		}
		mergePasses(ctx, tc, 1)
		time.Sleep(100 * time.Millisecond)
	}

	// The operator's split of a raw key is manual too, and has its own
	// way back.
	raw := append(lo.Clone(), 0xf0)
	if resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "split", Key: raw}); resp.Error != "" || resp.Right == nil || resp.Right.StickyUntil != kvpb.StickyForever {
		t.Fatalf("debug split: %+v", resp)
	}
	mergePasses(ctx, tc, 40)
	if count() != 1 {
		t.Fatalf("debug split merged away: %d boundaries", count())
	}
	if resp := adminCall(t, ctx, tc.Nodes[0].Addr(), cluster.AdminRequest{Op: "unsplit", Key: raw}); resp.Error != "" || len(resp.Ranges) != 1 || resp.Ranges[0].StickyUntil != 0 {
		t.Fatalf("debug unsplit: %+v", resp)
	}
	mergeTo(0, "after debug unsplit")
}

// TestStickySplitNeedsV20: on a cluster that has not finalized v20 a
// replica may be running a binary that drops the sticky bit, so nothing
// writes one. SPLIT AT still splits — as it did before the bit existed —
// and the statements that could not keep their promise are refused.
func TestStickySplitNeedsV20(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V19 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execSQL(t, ctx, s, `CREATE TABLE old (a INT8 PRIMARY KEY)`)
	res := execSQL(t, ctx, s, `ALTER TABLE old SPLIT AT VALUES (10)`)
	if res.Tag != "ALTER TABLE 1" {
		t.Fatalf("split at v19: %+v", res)
	}
	all, err := tc.ranges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range all {
		if keys.Pretty(d.StartKey) == res.Rows[0][0].Text() {
			found = true
			if d.StickyUntil != 0 {
				t.Fatalf("a sticky bit was written at v19: %v", d)
			}
		}
	}
	if !found {
		t.Fatalf("no range starts at %s", res.Rows[0][0].Text())
	}
	for _, q := range []string{
		`ALTER TABLE old SPLIT AT VALUES (20) WITH EXPIRATION INTERVAL '1 hour'`,
		`ALTER TABLE old UNSPLIT AT VALUES (10)`,
		`ALTER TABLE old UNSPLIT ALL`,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != sql.CodeFeatureNotSupported {
			t.Fatalf("%s at v19: %v, want 0A000", q, serr)
		}
	}
	// A request that reaches the leader anyway (a newer gateway, a race
	// with finalize) is held to the same rule there.
	sr, err := tc.Nodes[0].DB().AdminSplitSticky(ctx, keys.TableDataPrefix(879), kvpb.StickyForever)
	if err != nil || sr.Right.StickyUntil != 0 {
		t.Fatalf("manual KV split at v19: %+v, %v", sr, err)
	}
	if _, err := tc.Nodes[0].DB().AdminUnsplit(ctx, keys.TableDataPrefix(879)); err == nil {
		t.Fatal("unsplit accepted at v19")
	}
}

// TestStickyBitUpdateBuiltBeforeASplitIsRebuilt: marking an existing
// boundary sticky is a descriptor change, and like a split it is built
// from the descriptor the leader holds and proposed afterwards. One built
// before a split of the same range and applied after it would write the
// pre-split descriptor back — the right half's span claimed by two
// ranges. It must be refused at apply and rebuilt from the split range,
// and only the bit may change.
func TestStickyBitUpdateBuiltBeforeASplitIsRebuilt(t *testing.T) {
	hold := newDescriptorHold()
	tc := StartWithOptions(t, 3, func(c *server.Config) { c.TestingKnobs.BeforeProposeDescriptorChange = hold.knob })
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newRaceFixture(t, ctx, tc, 875)

	hold.arm(f.rhs.RangeID, "sticky")
	lead := tc.LeaderIndex(f.rhs.RangeID)
	type result struct {
		resp *kvpb.AdminSplitResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := tc.Nodes[lead].DB().AdminSplitSticky(ctx, f.rhs.StartKey, kvpb.StickyForever)
		done <- result{resp, err}
	}()
	hold.waitArrived(t, "the sticky-bit update")

	split, err := tc.Nodes[lead].DB().AdminSplit(ctx, append(f.prefix.Clone(), "z07"...))
	if err != nil {
		t.Fatalf("split under the built sticky-bit update: %v", err)
	}
	close(hold.release)

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("sticky-bit update built before the split: %v, want it rebuilt", res.err)
		}
		want := split.Left
		want.Generation++
		if got := res.resp.Right; !sameRangeDescriptor(got, want) || got.StickyUntil != kvpb.StickyForever {
			t.Fatalf("sticky-bit update produced %v, want %v with the bit set", got, want)
		}
		waitDescriptorEverywhere(t, f, want)
		stickyEverywhere(t, tc, want.RangeID, kvpb.StickyForever)
	case <-time.After(60 * time.Second):
		t.Fatal("the held sticky-bit update never returned")
	}
	// The split's right half is untouched and not sticky.
	waitDescriptorEverywhere(t, f, split.Right)
	stickyEverywhere(t, tc, split.Right.RangeID, 0)
	f.everyKeyReadable(t, ctx, "after the rebuilt sticky-bit update")
}
