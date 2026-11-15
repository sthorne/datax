package sql

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/parser"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
)

// loTestTables builds the three descriptors as CREATE TABLE would: the
// columns of the DDL in order, IDs from 1.
func loTestTables(t *testing.T) *loTables {
	t.Helper()
	out := &loTables{}
	for i, dst := range []**catalog.TableDescriptor{&out.meta, &out.pages, &out.pending} {
		stmts, err := parser.Parse(largeObjectDDL[i])
		if err != nil {
			t.Fatal(err)
		}
		ct := stmts[0].(*parser.CreateTable)
		id, ok := catalog.SystemTableID(ct.Name)
		if !ok {
			t.Fatalf("%s is not a reserved table", ct.Name)
		}
		d := &catalog.TableDescriptor{ID: id, Name: ct.Name}
		for j, c := range ct.Columns {
			d.Columns = append(d.Columns, catalog.Column{ID: catalog.ColumnID(j + 1), Name: c.Name, Type: c.Type, NotNull: c.NotNull})
		}
		for _, name := range ct.PrimaryKey {
			c, ok := d.Col(name)
			if !ok {
				t.Fatalf("%s: primary key column %q", ct.Name, name)
			}
			d.PrimaryKey = append(d.PrimaryKey, c.ID)
		}
		*dst = d
	}
	return out
}

// TestLargeObjectColumnIDs: the code reads and writes the tables' rows
// by column ID, and the IDs are positions in the DDL. Moving a column
// would silently swap two of them.
func TestLargeObjectColumnIDs(t *testing.T) {
	tabs := loTestTables(t)
	for _, c := range []struct {
		desc *catalog.TableDescriptor
		id   catalog.ColumnID
		name string
		fam  types.Family
	}{
		{tabs.meta, loMetaColCID, "cid", types.Int},
		{tabs.meta, loMetaColOwner, "owner", types.String},
		{tabs.meta, loMetaColCreated, "created_at", types.Timestamp},
		{tabs.pages, loPageColData, "data", types.Bytes},
		{tabs.pending, loPendingColLoid, "loid", types.Int},
		{tabs.pending, loPendingColKind, "kind", types.String},
		{tabs.pending, loPendingColNode, "node", types.Int},
		{tabs.pending, loPendingColAt, "at", types.Timestamp},
	} {
		col, ok := c.desc.ColByID(c.id)
		if !ok || col.Name != c.name || col.Type != c.fam {
			t.Errorf("%s: column %d is %q (%v), the code takes it for %q (%v)", c.desc.Name, c.id, col.Name, col.Type, c.name, c.fam)
		}
	}
	for _, d := range []*catalog.TableDescriptor{tabs.meta, tabs.pages, tabs.pending} {
		if !catalog.IsSealedSystemTable(d.Name) {
			t.Errorf("%s is not sealed", d.Name)
		}
	}
}

// TestLargeObjectPageKeys: a content's pages are one contiguous run of
// keys in page order, under a prefix no other content's pages share —
// what lets a read be one scan, the size be the last key of the run,
// and the sweeper delete a content by its prefix. Content ids are
// unique_rowid values, large and sometimes adjacent.
func TestLargeObjectPageKeys(t *testing.T) {
	tabs := loTestTables(t)
	cids := []int64{1, 2, 255, 256, 1 << 40, 1<<40 + 1, 1<<62 + 12345, 1<<62 + 12346}
	pages := []int64{0, 1, 2, 255, 256, 65535, 65536, 1 << 22}
	var prev keys.Key
	for i, cid := range cids {
		prefix := pagePrefix(tabs, cid)
		end := prefix.PrefixEnd()
		// The end of one content's run may be exactly where the next
		// content's begins (adjacent ids); it may not be past it.
		if prev != nil && bytes.Compare(prev, prefix) > 0 {
			t.Fatalf("content %d's pages do not sort after content %d's", cid, cids[i-1])
		}
		prev = nil
		for _, p := range pages {
			k, err := tabs.pageKey(cid, p)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(k, prefix) || bytes.Compare(k, end) >= 0 {
				t.Fatalf("page %d of content %d is outside the content's prefix", p, cid)
			}
			if prev != nil && bytes.Compare(prev, k) >= 0 {
				t.Fatalf("page %d of content %d does not sort after the page before it", p, cid)
			}
			prev = k
			pk, err := rowenc.DecodePK(tabs.pages, k)
			if err != nil || pk[0].I != cid || pk[1].I != p {
				t.Fatalf("page key of (%d, %d) decodes to %v, %v", cid, p, pk, err)
			}
		}
		prev = end
	}
}

// TestLargeObjectPageValue: a page of every length up to the page size
// survives the row encoding byte for byte, zeros and all, and the
// encoding's overhead is a few bytes.
func TestLargeObjectPageValue(t *testing.T) {
	tabs := loTestTables(t)
	for _, n := range []int{0, 1, 127, 128, 16383, 16384, LargeObjectPageSize - 1, LargeObjectPageSize} {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(i * 7)
		}
		raw, err := rowenc.EncodeValue(tabs.pages, map[catalog.ColumnID]types.Datum{loPageColData: types.NewBytes(data)})
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > n+8 {
			t.Fatalf("a %d-byte page encodes to %d bytes", n, len(raw))
		}
		row, err := rowenc.DecodeValue(tabs.pages, raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := row[loPageColData]; got.Null || got.Fam != types.Bytes || got.S != string(data) {
			t.Fatalf("a %d-byte page decodes to %d bytes (null %v)", n, len(got.S), got.Null)
		}
	}
}

// TestWriteTooLargeIsProgramLimit: the gateway's refusal of a write no
// replica could replicate reaches the client as 54000 with a message
// that says what to do, not as an internal error.
func TestWriteTooLargeIsProgramLimit(t *testing.T) {
	kerr := kvpb.NewErrorf("a write of 70000000 bytes to r7 is larger than a single replicated write can be")
	kerr.TooLarge = &kvpb.WriteTooLargeError{Bytes: 70_000_000, Limit: kvclient.MaxWriteBatchBytes}
	for _, err := range []error{kerr, fmt.Errorf("insert: %w", kerr)} {
		serr := ToSQLError(err)
		if serr.Code != CodeProgramLimitExceeded || !strings.Contains(serr.Msg, "67 MiB") || !strings.Contains(serr.Msg, "60 MiB") || !strings.Contains(serr.Msg, "large object") {
			t.Fatalf("ToSQLError(%v) = %s %q", err, serr.Code, serr.Msg)
		}
	}
	if serr := ToSQLError(kvpb.NewErrorf("something else")); serr.Code != CodeInternal {
		t.Fatalf("an untyped KV error maps to %s", serr.Code)
	}
}
