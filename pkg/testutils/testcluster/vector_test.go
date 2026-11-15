package testcluster

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/kvclient"
	"github.com/sthorne/datax/pkg/server"
	"github.com/sthorne/datax/pkg/sql"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/rowenc"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/sql/vector"
	"github.com/sthorne/datax/pkg/version"
)

// ---------------------------------------------------------------------
// What the tests compute for themselves. Nothing here calls the
// server's own distance code: the reference a result is held to is
// worked out from the formulas.

type vecRow struct {
	id  int64
	grp int64
	v   []float32 // nil: a NULL vector
}

func refDistance(op string, a, b []float32) float64 {
	var dot, na, nb, l2 float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
		l2 += (x - y) * (x - y)
	}
	switch op {
	case "<->":
		return math.Sqrt(l2)
	case "<#>":
		return -dot
	}
	sim := dot / math.Sqrt(na*nb)
	if math.IsNaN(sim) {
		return math.NaN()
	}
	return 1 - math.Max(-1, math.Min(1, sim))
}

// refKNN is the true answer to ORDER BY v <op> q LIMIT k over the rows
// keep admits: nearest first, NaN after every number, ties by id (the
// primary key, which is how the scan breaks them), NULL vectors last.
func refKNN(rows []vecRow, op string, q []float32, k int, keep func(vecRow) bool) []int64 {
	type cand struct {
		id   int64
		d    float64
		null bool
	}
	var cs []cand
	for _, r := range rows {
		if keep != nil && !keep(r) {
			continue
		}
		if r.v == nil {
			cs = append(cs, cand{id: r.id, null: true})
			continue
		}
		cs = append(cs, cand{id: r.id, d: refDistance(op, r.v, q)})
	}
	sort.Slice(cs, func(i, j int) bool {
		a, b := cs[i], cs[j]
		if a.null != b.null {
			return b.null
		}
		an, bn := math.IsNaN(a.d), math.IsNaN(b.d)
		if !a.null && (an != bn || (!an && a.d != b.d)) {
			if an != bn {
				return bn
			}
			return a.d < b.d
		}
		return a.id < b.id
	})
	if len(cs) > k {
		cs = cs[:k]
	}
	out := make([]int64, len(cs))
	for i, c := range cs {
		out[i] = c.id
	}
	return out
}

// clusteredVectors draws n vectors of dims elements around `clusters`
// Gaussian centres: the shape embeddings of a corpus with topics have,
// and the one an IVF index is built for.
func clusteredVectors(r *rand.Rand, n, clusters, dims int, spread float64) [][]float32 {
	centres := make([][]float64, clusters)
	for i := range centres {
		centres[i] = make([]float64, dims)
		for j := range centres[i] {
			centres[i][j] = r.NormFloat64()
		}
	}
	out := make([][]float32, n)
	for i := range out {
		c := centres[r.Intn(clusters)]
		out[i] = make([]float32, dims)
		for j := range out[i] {
			out[i][j] = float32(c[j] + spread*r.NormFloat64())
		}
	}
	return out
}

func vecLit(v []float32) string {
	if v == nil {
		return "NULL"
	}
	return "'" + vector.Format(v) + "'"
}

// insertVecRows loads rows into (id, grp, v) in multi-row statements.
func insertVecRows(t testing.TB, ctx context.Context, s *sql.Session, table string, rows []vecRow) {
	t.Helper()
	const batch = 250
	for lo := 0; lo < len(rows); lo += batch {
		hi := lo + batch
		if hi > len(rows) {
			hi = len(rows)
		}
		vals := make([]string, 0, hi-lo)
		for _, r := range rows[lo:hi] {
			vals = append(vals, fmt.Sprintf("(%d, %d, %s)", r.id, r.grp, vecLit(r.v)))
		}
		if _, serr := trySQL(ctx, s, "INSERT INTO "+table+" VALUES "+strings.Join(vals, ", ")); serr != nil {
			t.Fatalf("loading %s: %v", table, serr)
		}
	}
}

// idsInOrder runs a query whose first column is id and returns the ids
// as returned.
func idsInOrder(t testing.TB, ctx context.Context, s *sql.Session, query string, params ...types.Datum) []int64 {
	t.Helper()
	res, serr := trySQL(ctx, s, query, params...)
	if serr != nil {
		t.Fatalf("sql %q: [%s] %s", query, serr.Code, serr.Msg)
	}
	out := make([]int64, len(res.Rows))
	for i, r := range res.Rows {
		out[i] = r[0].I
	}
	return out
}

// vectorIndexEntries reads a vector index raw and returns its entries
// (key and value), and the entries the table's rows say it should hold,
// both in one transaction.
func vectorIndexEntries(t testing.TB, ctx context.Context, db *kvclient.DB, table, index string) (got, want []string, idx catalog.IndexDescriptor) {
	t.Helper()
	// A lookup through a session loads the index's centres, which the
	// row encoder needs to say where a row belongs.
	execVec(t, ctx, sql.NewSession(db, catalog.NewAccessor()), "SELECT count(*) FROM "+table)
	err := db.RunTxn(ctx, "test-vector-entries", func(ctx context.Context, txn *kvclient.Txn) error {
		got, want = nil, nil
		desc, err := catalog.NewAccessor().LookupFresh(ctx, txn, table)
		if err != nil {
			return err
		}
		var ix *catalog.IndexDescriptor
		for i := range desc.Indexes {
			if desc.Indexes[i].Name == index {
				ix = &desc.Indexes[i]
			}
		}
		if ix == nil {
			return fmt.Errorf("table %s has no index %s", table, index)
		}
		idx = *ix
		lo, hi := keys.TableIndexSpan(desc.ID, ix.ID)
		kvs, err := txn.Scan(ctx, lo, hi, 0)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			got = append(got, packEntry(kv.Key, kv.Value))
		}
		plo, phi := rowenc.PrimarySpanFor(desc)
		rows, err := txn.Scan(ctx, plo, phi, 0)
		if err != nil {
			return err
		}
		for _, kv := range rows {
			pk, err := rowenc.DecodePK(desc, kv.Key)
			if err != nil {
				return err
			}
			row, err := rowenc.DecodeValue(desc, kv.Value)
			if err != nil {
				return err
			}
			for i, id := range desc.PrimaryKey {
				row[id] = pk[i]
			}
			entries, err := rowenc.IndexEntries(desc, ix, row)
			if err != nil {
				return err
			}
			for _, e := range entries {
				want = append(want, packEntry(e.Key, e.Value))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reading index %s: %v", index, err)
	}
	sort.Strings(want)
	return got, want, idx
}

// packEntry holds an index entry as one string: the key's length, the
// key, the value. Entries of one index have keys of one length (the
// primary keys here are integers), so the strings sort as the keys do.
func packEntry(key keys.Key, value []byte) string {
	return fmt.Sprintf("%06d", len(key)) + string(key) + string(value)
}

// entryKey is a packed entry's key.
func entryKey(e string) keys.Key {
	var n int
	fmt.Sscanf(e[:6], "%d", &n)
	return keys.Key(e[6 : 6+n])
}

// checkVectorIndex fails unless the index holds exactly the entries its
// table's rows imply — none missing, none left behind, each carrying
// its row's current vector — and returns how many that is.
func checkVectorIndex(t testing.TB, ctx context.Context, db *kvclient.DB, table, index string) int {
	t.Helper()
	got, want, _ := vectorIndexEntries(t, ctx, db, table, index)
	if len(got) != len(want) {
		t.Fatalf("index %s holds %d entries, its table's rows imply %d", index, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("index %s entry %d is not the one its rows imply (%s)", index, i,
				keys.Pretty(entryKey(got[i])))
		}
	}
	return len(got)
}

func execVec(t testing.TB, ctx context.Context, s *sql.Session, q string, params ...types.Datum) *sql.Result {
	t.Helper()
	res, serr := trySQL(ctx, s, q, params...)
	if serr != nil {
		t.Fatalf("sql %q: [%s] %s", q, serr.Code, serr.Msg)
	}
	return res
}

func planOf(t testing.TB, ctx context.Context, s *sql.Session, q string) string {
	t.Helper()
	res := execVec(t, ctx, s, "EXPLAIN "+q)
	return res.Rows[0][0].S
}

// indexedAndExact runs a nearest-neighbour query through the vector
// index and by scanning, and returns both id lists in result order.
func indexedAndExact(t testing.TB, ctx context.Context, s *sql.Session, q string) (indexed, exact []int64) {
	t.Helper()
	execVec(t, ctx, s, `SET vector_index_scan = on`)
	if p := planOf(t, ctx, s, q); !strings.HasPrefix(p, "vector scan of index") {
		t.Fatalf("%s: not served by the vector index: %s", q, p)
	}
	indexed = idsInOrder(t, ctx, s, q)
	execVec(t, ctx, s, `SET vector_index_scan = off`)
	if p := planOf(t, ctx, s, q); !strings.Contains(p, "top-k heap") {
		t.Fatalf("%s: vector_index_scan = off does not plan the exact top-k scan: %s", q, p)
	}
	exact = idsInOrder(t, ctx, s, q)
	execVec(t, ctx, s, `RESET vector_index_scan`)
	return indexed, exact
}

func recallOf(indexed, exact []int64) float64 {
	if len(exact) == 0 {
		return 1
	}
	in := map[int64]bool{}
	for _, id := range exact {
		in[id] = true
	}
	hit := 0
	for _, id := range indexed {
		if in[id] {
			hit++
		}
	}
	return float64(hit) / float64(len(exact))
}

// ---------------------------------------------------------------------
// A pgx codec for the vector type, as a client library registers one
// (pgvector-go's does the same): []float32 in pgvector's binary form.

type testVectorCodec struct{}

func (testVectorCodec) FormatSupported(f int16) bool { return f == pgtype.BinaryFormatCode }
func (testVectorCodec) PreferredFormat() int16       { return pgtype.BinaryFormatCode }

type testVectorEncode struct{}

func (testVectorEncode) Encode(value any, buf []byte) ([]byte, error) {
	v := value.([]float32)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(v)))
	buf = binary.BigEndian.AppendUint16(buf, 0)
	for _, x := range v {
		buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(x))
	}
	return buf, nil
}

type testVectorScan struct{}

func decodeTestVector(src []byte) ([]float32, error) {
	if len(src) < 4 {
		return nil, errors.New("short vector")
	}
	n := int(binary.BigEndian.Uint16(src))
	if len(src) != 4+4*n {
		return nil, fmt.Errorf("vector of %d dimensions in %d bytes", n, len(src))
	}
	out := make([]float32, n)
	for i := range out {
		out[i] = math.Float32frombits(binary.BigEndian.Uint32(src[4+4*i:]))
	}
	return out, nil
}

func (testVectorScan) Scan(src []byte, target any) error {
	p := target.(*[]float32)
	if src == nil {
		*p = nil
		return nil
	}
	v, err := decodeTestVector(src)
	*p = v
	return err
}

func (testVectorCodec) PlanEncode(_ *pgtype.Map, _ uint32, format int16, value any) pgtype.EncodePlan {
	if _, ok := value.([]float32); ok && format == pgtype.BinaryFormatCode {
		return testVectorEncode{}
	}
	return nil
}

func (testVectorCodec) PlanScan(_ *pgtype.Map, _ uint32, format int16, target any) pgtype.ScanPlan {
	if _, ok := target.(*[]float32); ok && format == pgtype.BinaryFormatCode {
		return testVectorScan{}
	}
	return nil
}

func (testVectorCodec) DecodeDatabaseSQLValue(_ *pgtype.Map, _ uint32, _ int16, src []byte) (driver.Value, error) {
	return string(src), nil
}

func (testVectorCodec) DecodeValue(_ *pgtype.Map, _ uint32, _ int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	return decodeTestVector(src)
}

// ---------------------------------------------------------------------

// TestVectorTypeOverPgx: the type as a stock client meets it on a
// three-node cluster — the extension statement, the OID lookup, values
// in and out as text and (once the client has registered the type) as
// binary, and everything that is refused.
func TestVectorTypeOverPgx(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	waitForDatabases(t, ctx, sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor()))
	conn, err := pgx.Connect(ctx, pgURL(tc, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	refused := func(code, q string, args ...any) string {
		t.Helper()
		_, err := conn.Exec(ctx, q, args...)
		if err == nil || pgCode(err) != code {
			t.Fatalf("%s: got %v, want SQLSTATE %s", q, err, code)
		}
		return err.Error()
	}

	// A schema written for PostgreSQL with pgvector starts like this.
	exec(`CREATE EXTENSION IF NOT EXISTS vector`)
	exec(`CREATE EXTENSION vector`)
	refused("0A000", `CREATE EXTENSION pg_trgm`)
	exec(`CREATE TABLE items (id INT8 PRIMARY KEY, embedding vector(3), free vector)`)

	// The OID, found the two ways clients look for it.
	var oid, fromCatalog uint32
	if err := conn.QueryRow(ctx, `SELECT to_regtype('vector')::oid`).Scan(&oid); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT oid FROM pg_type WHERE typname = 'vector'`).Scan(&fromCatalog); err != nil {
		t.Fatal(err)
	}
	if oid != vector.OID || fromCatalog != oid {
		t.Fatalf("the vector type's OID: to_regtype %d, pg_type %d, want %d", oid, fromCatalog, vector.OID)
	}
	// There is no array of vectors, so no _vector row (which would take
	// another array type's OID).
	var arrays int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM pg_type WHERE typname = '_vector' OR typelem = $1`, oid).Scan(&arrays); err != nil || arrays != 0 {
		t.Fatalf("pg_type lists %d array types of vector (%v)", arrays, err)
	}
	var missing *uint32
	if err := conn.QueryRow(ctx, `SELECT to_regtype('halfvec')::oid`).Scan(&missing); err != nil || missing != nil {
		t.Fatalf("to_regtype of a type that does not exist: %v, %v", missing, err)
	}

	// Text parameters and literals, before the client knows the type.
	exec(`INSERT INTO items VALUES ($1, $2, $3)`, 1, "[1,2,3]", "[1,2]")
	exec(`INSERT INTO items VALUES (2, '[4, 6, 3]', NULL), (3, ARRAY[0,0,1], '[9]'), (4, NULL, NULL)`)
	var text, freeText string
	if err := conn.QueryRow(ctx, `SELECT embedding::text, free::text FROM items WHERE id = 1`).Scan(&text, &freeText); err != nil {
		t.Fatal(err)
	}
	if text != "[1,2,3]" || freeText != "[1,2]" {
		t.Fatalf("read back %s and %s", text, freeText)
	}
	// Unregistered, the value still arrives as its text.
	if err := conn.QueryRow(ctx, `SELECT embedding FROM items WHERE id = 3`, pgx.QueryExecModeSimpleProtocol).Scan(&text); err != nil || text != "[0,0,1]" {
		t.Fatalf("an array literal stored as %q (%v)", text, err)
	}

	// The statement describes its parameters and columns as vectors.
	sd, err := conn.Prepare(ctx, "ins", `INSERT INTO items (id, embedding) VALUES ($1, $2)`)
	if err != nil || sd.ParamOIDs[1] != oid {
		t.Fatalf("INSERT parameter OIDs %v (%v), want the vector OID second", sd.ParamOIDs, err)
	}
	sd, err = conn.Prepare(ctx, "knn", `SELECT id, embedding FROM items ORDER BY embedding <-> $1 LIMIT 3`)
	if err != nil || sd.ParamOIDs[0] != oid || sd.Fields[1].DataTypeOID != oid {
		t.Fatalf("k-NN statement: parameter OIDs %v, column OID %d (%v)", sd.ParamOIDs, sd.Fields[1].DataTypeOID, err)
	}
	var typmod int32
	if err := conn.QueryRow(ctx, `SELECT atttypmod FROM pg_attribute WHERE attname = 'embedding' AND attrelid = 'items'::regclass`).Scan(&typmod); err != nil || typmod != 3 {
		t.Fatalf("atttypmod %d (%v), want the dimension", typmod, err)
	}

	// A second connection registers the type, as pgvector-go does, and
	// from then on sends and receives it in binary.
	bconn, err := pgx.Connect(ctx, pgURL(tc, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer bconn.Close(ctx)
	bconn.TypeMap().RegisterType(&pgtype.Type{Name: "vector", OID: oid, Codec: testVectorCodec{}})
	if _, err := bconn.Exec(ctx, `INSERT INTO items (id, embedding) VALUES ($1, $2)`, 5, []float32{0.5, -1.25, 3e10}); err != nil {
		t.Fatalf("binary insert: %v", err)
	}
	var got []float32
	if err := bconn.QueryRow(ctx, `SELECT embedding FROM items WHERE id = 5`).Scan(&got); err != nil {
		t.Fatalf("binary read: %v", err)
	}
	if len(got) != 3 || got[0] != 0.5 || got[1] != -1.25 || got[2] != 3e10 {
		t.Fatalf("binary round trip gave %v", got)
	}
	if err := bconn.QueryRow(ctx, `SELECT embedding FROM items WHERE id = 4`).Scan(&got); err != nil || got != nil {
		t.Fatalf("a NULL vector read as %v (%v)", got, err)
	}
	rows, err := bconn.Query(ctx, `SELECT id, embedding <-> $1 FROM items WHERE id < 5 ORDER BY embedding <-> $1 LIMIT 10`, []float32{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for rows.Next() {
		var id int64
		var d *float64
		if err := rows.Scan(&id, &d); err != nil {
			t.Fatal(err)
		}
		if d == nil {
			order = append(order, fmt.Sprintf("%d:NULL", id))
		} else {
			order = append(order, fmt.Sprintf("%d:%.4f", id, *d))
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	// |(3,4,0)| = 5; |(1,2,2)| = 3; the NULL vector's distance is NULL
	// and sorts last.
	if want := "1:0.0000 3:3.0000 2:5.0000 4:NULL"; strings.Join(order, " ") != want {
		t.Fatalf("k-NN with a binary parameter: %v, want %s", order, want)
	}
	// The three operators and the functions, against hand-worked values.
	var l2, cos, ip, fl2, fip, norm float64
	var dims int64
	if err := conn.QueryRow(ctx, `SELECT embedding <-> '[4,6,3]', embedding <=> '[4,6,3]', embedding <#> '[4,6,3]',
		l2_distance(embedding, '[4,6,3]'), inner_product(embedding, '[4,6,3]'), vector_norm(embedding), vector_dims(embedding)
		FROM items WHERE id = 1`).Scan(&l2, &cos, &ip, &fl2, &fip, &norm, &dims); err != nil {
		t.Fatal(err)
	}
	if l2 != 5 || ip != -25 || fl2 != 5 || fip != 25 || dims != 3 || math.Abs(norm-math.Sqrt(14)) > 1e-12 ||
		math.Abs(cos-(1-25/(math.Sqrt(14)*math.Sqrt(61)))) > 1e-12 {
		t.Fatalf("operators and functions: <-> %v, <=> %v, <#> %v, l2_distance %v, inner_product %v, norm %v, dims %d", l2, cos, ip, fl2, fip, norm, dims)
	}

	// Equality and the element-wise order.
	var n int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM items WHERE embedding = '[1, 2, 3]'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("equality matched %d rows (%v)", n, err)
	}
	if err := conn.QueryRow(ctx, `SELECT id FROM items WHERE embedding IS NOT NULL ORDER BY embedding LIMIT 1`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("ORDER BY a vector column put %d first (%v), want 3 ([0,0,1])", n, err)
	}

	// What is refused.
	if msg := refused("22000", `INSERT INTO items VALUES (10, '[1,2]', NULL)`); !strings.Contains(msg, "expected 3 dimensions, not 2") {
		t.Fatalf("wrong dimension: %s", msg)
	}
	refused("22000", `INSERT INTO items VALUES ($1, $2, NULL)`, 10, "[1,2,3,4]")
	refused("22000", `UPDATE items SET embedding = '[1]' WHERE id = 1`)
	for _, bad := range []string{"[1,NaN,3]", "[1,Infinity,3]", "[]", "1,2,3", "[1,2,x]", "[1e40,0,0]"} {
		_, err := conn.Exec(ctx, `INSERT INTO items VALUES (10, $1, NULL)`, bad)
		if c := pgCode(err); c != "22P02" && c != "22000" {
			t.Fatalf("inserting %q: %v", bad, err)
		}
	}
	if _, err := bconn.Exec(ctx, `INSERT INTO items (id, embedding) VALUES ($1, $2)`, 11, []float32{1, float32(math.NaN()), 3}); pgCode(err) != "22P03" {
		t.Fatalf("a binary NaN: %v", err)
	}
	if _, err := bconn.Exec(ctx, `INSERT INTO items (id, embedding) VALUES ($1, $2)`, 11, []float32{1, 2}); pgCode(err) != "22000" {
		t.Fatalf("a binary vector of the wrong dimension: %v", err)
	}
	if msg := refused("22000", `SELECT embedding <-> '[1,2]' FROM items WHERE id = 1`); !strings.Contains(msg, "different vector dimensions 3 and 2") {
		t.Fatalf("distance across dimensions: %s", msg)
	}
	refused("42601", `CREATE TABLE bad (id INT8 PRIMARY KEY, v vector(16001))`)
	refused("42601", `CREATE TABLE bad (id INT8 PRIMARY KEY, v vector(3)[])`)
	// No ordered key encoding: not a primary key, not an ordinary index.
	if _, err := conn.Exec(ctx, `CREATE TABLE bad (v vector(2) PRIMARY KEY)`); err == nil {
		t.Fatal("a vector primary key was accepted")
	}
	if msg := refused("0A000", `CREATE INDEX items_btree ON items (embedding)`); !strings.Contains(msg, "ivfflat") {
		t.Fatalf("an ordinary index on a vector: %s", msg)
	}
	if msg := refused("0A000", `CREATE INDEX items_hnsw ON items USING hnsw (embedding vector_l2_ops)`); !strings.Contains(msg, "ivfflat") {
		t.Fatalf("USING hnsw: %s", msg)
	}
	if msg := refused("0A000", `CREATE INDEX items_free ON items USING ivfflat (free)`); !strings.Contains(msg, "does not have dimensions") {
		t.Fatalf("an index on a column without dimensions: %s", msg)
	}
	refused("0A000", `CREATE UNIQUE INDEX items_u ON items USING ivfflat (embedding)`)
	refused("0A000", `CREATE INDEX items_id ON items USING ivfflat (id)`)
	refused("42704", `CREATE INDEX items_x ON items USING ivfflat (embedding vector_l1_ops)`)
	refused("22023", `CREATE INDEX items_x ON items USING ivfflat (embedding) WITH (lists = 0)`)
	refused("22023", `CREATE INDEX items_x ON items USING ivfflat (embedding) WITH (lists = 40000)`)
	refused("22023", `CREATE INDEX items_x ON items USING ivfflat (embedding) WITH (m = 16)`)
	refused("0A000", `CREATE INDEX items_x ON items (id) WITH (lists = 4)`)
	exec(`CREATE TABLE wide (id INT8 PRIMARY KEY, v vector(2001))`)
	refused("54000", `CREATE INDEX wide_ivf ON wide USING ivfflat (v)`)
	refused("22023", `SET ivfflat.probes = 0`)

	// The definitions read back as written.
	exec(`CREATE INDEX items_ivf ON items USING ivfflat (embedding vector_cosine_ops) WITH (lists = 2)`)
	var def string
	if err := conn.QueryRow(ctx, `SELECT indexdef FROM pg_indexes WHERE indexname = 'items_ivf'`).Scan(&def); err != nil ||
		!strings.Contains(def, "USING ivfflat (embedding vector_cosine_ops) WITH (lists='2')") {
		t.Fatalf("pg_indexes: %q (%v)", def, err)
	}
	var name, create string
	if err := conn.QueryRow(ctx, `SHOW CREATE TABLE items`).Scan(&name, &create); err != nil || !strings.Contains(create, "embedding VECTOR(3)") || !strings.Contains(create, "free VECTOR,") {
		t.Fatalf("SHOW CREATE TABLE: %q (%v)", create, err)
	}
	if err := conn.QueryRow(ctx, `SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attname = 'embedding' AND attrelid = 'items'::regclass`).Scan(&def); err != nil || def != "vector(3)" {
		t.Fatalf("format_type: %q (%v)", def, err)
	}
	// A text column whose values are vectors converts.
	exec(`CREATE TABLE texts (id INT8 PRIMARY KEY, v TEXT)`)
	exec(`INSERT INTO texts VALUES (1, '[3,4]'), (2, '[0, 1]'), (3, NULL)`)
	exec(`ALTER TABLE texts ALTER COLUMN v TYPE vector(2)`)
	if err := conn.QueryRow(ctx, `SELECT id FROM texts ORDER BY v <-> '[3,3]' LIMIT 1`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("after ALTER COLUMN TYPE vector(2): nearest is %d (%v)", n, err)
	}
	refused("22000", `INSERT INTO texts VALUES (4, '[1,2,3]')`)
	// The probes setting, for the session and for one transaction.
	var probes string
	exec(`SET ivfflat.probes = 7`)
	if err := conn.QueryRow(ctx, `SHOW ivfflat.probes`).Scan(&probes); err != nil || probes != "7" {
		t.Fatalf("SHOW ivfflat.probes: %q (%v)", probes, err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ivfflat.probes = 2`); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, `SHOW ivfflat.probes`).Scan(&probes); err != nil || probes != "2" {
		t.Fatalf("SET LOCAL ivfflat.probes: %q (%v)", probes, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	exec(`RESET ivfflat.probes`)
	if err := conn.QueryRow(ctx, `SHOW ivfflat.probes`).Scan(&probes); err != nil || probes != "auto" {
		t.Fatalf("RESET ivfflat.probes: %q (%v)", probes, err)
	}
	// A column that is indexed keeps its type and its place.
	refused("0A000", `ALTER TABLE items ALTER COLUMN embedding TYPE vector(4)`)
	refused("0A000", `ALTER TABLE items DROP COLUMN embedding`)
}

// TestVectorExactKNN: without an index, ORDER BY distance LIMIT k is
// the true k nearest for each of the three distances — against a
// reference the test computes itself — with filters, ties, NULLs,
// OFFSET, and a zero vector under cosine; the plan is the top-k heap,
// and it runs in memory a full sort of the table does not fit in.
func TestVectorExactKNN(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const n, dims = 1500, 12
	r := rand.New(rand.NewSource(42))
	vs := clusteredVectors(r, n, 20, dims, 0.4)
	rows := make([]vecRow, n)
	for i := range rows {
		rows[i] = vecRow{id: int64(i), grp: int64(i % 7), v: vs[i]}
		switch {
		case i%10 == 9:
			// Ties: an exact copy of an earlier row's vector.
			rows[i].v = rows[i-5].v
		case i%97 == 0:
			rows[i].v = nil
		}
	}
	rows[3].v = make([]float32, dims) // the zero vector: its cosine distance is NaN
	execVec(t, ctx, s, fmt.Sprintf(`CREATE TABLE pts (id INT8 PRIMARY KEY, grp INT8, v vector(%d))`, dims))
	execVec(t, ctx, s, `CREATE INDEX pts_grp ON pts (grp)`)
	insertVecRows(t, ctx, s, "pts", rows)
	// Spread over ranges, so the scan crosses boundaries.
	for _, at := range []int{400, 900, 1300} {
		execVec(t, ctx, s, fmt.Sprintf(`ALTER TABLE pts SPLIT AT VALUES (%d)`, at))
	}

	same := func(what string, got, want []int64) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s:\n got %v\nwant %v", what, got, want)
		}
	}
	nonNull := func(r vecRow) bool { return r.v != nil }
	for _, op := range []string{"<->", "<=>", "<#>"} {
		for qi := 0; qi < 12; qi++ {
			q := vs[r.Intn(n)]
			if qi%3 == 0 {
				q = clusteredVectors(r, 1, 1, dims, 1)[0]
			}
			lit := vecLit(q)
			base := fmt.Sprintf(`SELECT id FROM pts ORDER BY v %s %s`, op, lit)
			same(op+" k=10", idsInOrder(t, ctx, s, base+` LIMIT 10`), refKNN(rows, op, q, 10, nil))
			same(op+" k=1", idsInOrder(t, ctx, s, base+` LIMIT 1`), refKNN(rows, op, q, 1, nil))
			// As a parameter, and with the operands the other way round.
			same(op+" parameter", idsInOrder(t, ctx, s, fmt.Sprintf(`SELECT id FROM pts ORDER BY $1 %s v LIMIT 10`, op), types.NewString(vector.Format(q))),
				refKNN(rows, op, q, 10, nil))
			// OFFSET pages through the same order.
			if want := refKNN(rows, op, q, 25, nil); true {
				same(op+" offset", idsInOrder(t, ctx, s, base+` LIMIT 10 OFFSET 15`), want[15:])
			}
			// A filter the scan evaluates, and one an index serves (the
			// k nearest among the rows it selects).
			same(op+" filter", idsInOrder(t, ctx, s, fmt.Sprintf(`SELECT id FROM pts WHERE id %% 3 = 1 AND v IS NOT NULL ORDER BY v %s %s LIMIT 10`, op, lit)),
				refKNN(rows, op, q, 10, func(r vecRow) bool { return r.id%3 == 1 && r.v != nil }))
			same(op+" indexed filter", idsInOrder(t, ctx, s, fmt.Sprintf(`SELECT id FROM pts WHERE grp = 2 ORDER BY v %s %s LIMIT 10`, op, lit)),
				refKNN(rows, op, q, 10, func(r vecRow) bool { return r.grp == 2 }))
			same(op+" range filter", idsInOrder(t, ctx, s, fmt.Sprintf(`SELECT id FROM pts WHERE id >= 300 AND id < 1100 ORDER BY v %s %s LIMIT 10`, op, lit)),
				refKNN(rows, op, q, 10, func(r vecRow) bool { return r.id >= 300 && r.id < 1100 }))
			// A distance threshold: fewer than k may pass.
			if op == "<->" {
				same(op+" threshold", idsInOrder(t, ctx, s, fmt.Sprintf(`SELECT id FROM pts WHERE v <-> %s < 0.9 ORDER BY v <-> %s LIMIT 10`, lit, lit)),
					refKNN(rows, op, q, 10, func(r vecRow) bool { return r.v != nil && refDistance(op, r.v, q) < 0.9 }))
			}
			// The whole table in order: every NULL after every vector.
			if qi == 0 {
				same(op+" everything", idsInOrder(t, ctx, s, base+fmt.Sprintf(` LIMIT %d`, n+5)), refKNN(rows, op, q, n+5, nil))
				same(op+" no limit", idsInOrder(t, ctx, s, `SELECT id FROM pts WHERE v IS NOT NULL ORDER BY v `+op+` `+lit), refKNN(rows, op, q, n, nonNull))
			}
		}
	}
	// The returned distances are the true ones, in order.
	q := vs[7]
	res := execVec(t, ctx, s, fmt.Sprintf(`SELECT id, v <=> %s AS d FROM pts WHERE v IS NOT NULL AND id <> 3 ORDER BY d LIMIT 20`, vecLit(q)))
	prev := math.Inf(-1)
	for _, row := range res.Rows {
		want := refDistance("<=>", rows[row[0].I].v, q)
		if math.Abs(row[1].F-want) > 1e-12 || row[1].F < prev {
			t.Fatalf("row %d: distance %v, want %v (previous %v)", row[0].I, row[1].F, want, prev)
		}
		prev = row[1].F
	}

	// The plans: a top-k heap over the scan, over a range, and — where
	// an index serves the filter — the ordinary sort of what it selects.
	for q, want := range map[string]string{
		`SELECT id FROM pts ORDER BY v <-> '` + vector.Format(q) + `' LIMIT 10`:                "full table scan; top-k heap: the first 10 rows",
		`SELECT id FROM pts ORDER BY v <-> '` + vector.Format(q) + `' LIMIT 10 OFFSET 5`:       "top-k heap: the first 15 rows",
		`SELECT id FROM pts WHERE id >= 300 ORDER BY v <=> '` + vector.Format(q) + `' LIMIT 3`: "range scan of primary key (id >= 300); top-k heap: the first 3 rows",
		`SELECT id FROM pts WHERE grp = 2 ORDER BY v <-> '` + vector.Format(q) + `' LIMIT 10`:  `scan of index "pts_grp"`,
		`SELECT id FROM pts ORDER BY v <-> '` + vector.Format(q) + `'`:                         "in-memory sort",
		`SELECT id FROM pts ORDER BY grp DESC, id LIMIT 4`:                                     "top-k heap: the first 4 rows",
		`SELECT DISTINCT grp FROM pts ORDER BY grp LIMIT 4`:                                    "full table scan",
	} {
		if p := planOf(t, ctx, s, q); !strings.Contains(p, want) {
			t.Fatalf("EXPLAIN %s:\n got %s\nwant it to contain %q", q, p, want)
		}
	}
	// The heap is any ORDER BY ... LIMIT's, not only a distance's.
	same("ORDER BY grp DESC, id LIMIT 4", idsInOrder(t, ctx, s, `SELECT id FROM pts ORDER BY grp DESC, id LIMIT 4`), []int64{6, 13, 20, 27})
	// Ties leave the heap in scan order, as the stable sort leaves them.
	same("ORDER BY grp DESC LIMIT 4", idsInOrder(t, ctx, s, `SELECT id FROM pts ORDER BY grp DESC LIMIT 4`), []int64{6, 13, 20, 27})
	same("ORDER BY grp LIMIT 3 OFFSET 2", idsInOrder(t, ctx, s, `SELECT id FROM pts ORDER BY grp LIMIT 3 OFFSET 2`), []int64{14, 21, 28})
	if a := strings.Join(rowsText(t, ctx, s, `EXPLAIN ANALYZE SELECT id FROM pts ORDER BY v <-> '`+vector.Format(q)+`' LIMIT 10`), "\n"); !strings.Contains(a, fmt.Sprintf("top-k: %d rows scanned, %d matched, 10 kept in a heap of 10", n, n)) {
		t.Fatalf("EXPLAIN ANALYZE:\n%s", a)
	}

	// Bounded memory: wider rows than the statement may hold all at
	// once. The top-k query holds k of them; the same order without a
	// limit has to hold them all and is refused.
	const wideN, wideDims = 600, 512
	wide := clusteredVectors(r, wideN, 5, wideDims, 0.5)
	wrows := make([]vecRow, wideN)
	for i := range wrows {
		wrows[i] = vecRow{id: int64(i), v: wide[i]}
	}
	execVec(t, ctx, s, fmt.Sprintf(`CREATE TABLE wide (id INT8 PRIMARY KEY, grp INT8, v vector(%d))`, wideDims))
	insertVecRows(t, ctx, s, "wide", wrows)
	execVec(t, ctx, s, `SET statement_memory_limit = '512kB'`) // the table is ~1.3 MB of vectors
	wq := `SELECT id FROM wide ORDER BY v <-> ` + vecLit(wide[11])
	same("top-k under a memory limit", idsInOrder(t, ctx, s, wq+` LIMIT 5`), refKNN(wrows, "<->", wide[11], 5, nil))
	if _, serr := trySQL(ctx, s, wq); serr == nil || serr.Code != sql.CodeOutOfMemory {
		t.Fatalf("a full sort under the same limit: %v", serr)
	}
}

// vectorRecall measures recall@k of the index against the exact scan's
// answers over the given queries at one probes setting, checking as it goes
// that what the index returns are real rows in true distance order.
func vectorRecall(t testing.TB, ctx context.Context, s *sql.Session, table, op string, byID map[int64][]float32, queries [][]float32, exact [][]int64, k int, probes string) (recall float64, perQuery time.Duration) {
	t.Helper()
	execVec(t, ctx, s, `SET ivfflat.probes = `+probes)
	defer execVec(t, ctx, s, `RESET ivfflat.probes`)
	var sum float64
	var took time.Duration
	for qi, q := range queries {
		stmt := fmt.Sprintf(`SELECT id, v %s %s FROM %s ORDER BY v %s %s LIMIT %d`, op, vecLit(q), table, op, vecLit(q), k)
		start := time.Now()
		res := execVec(t, ctx, s, stmt)
		took += time.Since(start)
		indexed := make([]int64, len(res.Rows))
		prev := math.Inf(-1)
		seen := map[int64]bool{}
		for i, row := range res.Rows {
			id, d := row[0].I, row[1].F
			v, ok := byID[id]
			if !ok || seen[id] {
				t.Fatalf("the index returned id %d, which is not a row of the table (or twice)", id)
			}
			seen[id] = true
			if want := refDistance(op, v, q); math.Abs(d-want) > 1e-9*(1+math.Abs(want)) {
				t.Fatalf("row %d: the returned distance %v is not its true distance %v", id, d, want)
			}
			if d < prev {
				t.Fatalf("the index's rows are not in distance order: %v after %v", d, prev)
			}
			prev = d
			indexed[i] = id
		}
		sum += recallOf(indexed, exact[qi])
	}
	return sum / float64(len(queries)), took / time.Duration(len(queries))
}

// exactKNN answers each query by the exact scan, and says how long one
// took.
func exactKNN(t testing.TB, ctx context.Context, s *sql.Session, table, op string, queries [][]float32, k int) ([][]int64, time.Duration) {
	t.Helper()
	execVec(t, ctx, s, `SET vector_index_scan = off`)
	defer execVec(t, ctx, s, `RESET vector_index_scan`)
	out := make([][]int64, len(queries))
	start := time.Now()
	for i, q := range queries {
		out[i] = idsInOrder(t, ctx, s, fmt.Sprintf(`SELECT id FROM %s ORDER BY v %s %s LIMIT %d`, table, op, vecLit(q), k))
	}
	return out, time.Since(start) / time.Duration(len(queries))
}

// TestVectorIndexRecall: an ivfflat index over 20,000 clustered vectors
// of 32 dimensions on a three-node cluster. Recall@10 is measured at
// several probes settings and logged; probing every list is exact; the
// default setting reaches 0.95.
func TestVectorIndexRecall(t *testing.T) {
	n := 20000
	if testing.Short() {
		n = 4000
	}
	const dims, lists, k = 32, 100, 10
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	r := rand.New(rand.NewSource(304))
	vs := clusteredVectors(r, n, 400, dims, 0.6)
	rows := make([]vecRow, n)
	byID := make(map[int64][]float32, n)
	for i := range rows {
		rows[i] = vecRow{id: int64(i), grp: int64(i % 10), v: vs[i]}
		byID[int64(i)] = vs[i]
	}
	execVec(t, ctx, s, fmt.Sprintf(`CREATE TABLE corpus (id INT8 PRIMARY KEY, grp INT8, v vector(%d))`, dims))
	start := time.Now()
	insertVecRows(t, ctx, s, "corpus", rows)
	loaded := time.Since(start)
	start = time.Now()
	execVec(t, ctx, s, fmt.Sprintf(`CREATE INDEX corpus_ivf ON corpus USING ivfflat (v vector_l2_ops) WITH (lists = %d)`, lists))
	built := time.Since(start)
	t.Logf("%d vectors × %d dims on 3 nodes: loaded in %s, index (lists = %d) built in %s", n, dims, loaded.Round(time.Millisecond), lists, built.Round(time.Millisecond))
	if got := checkVectorIndex(t, ctx, tc.Nodes[0].DB(), "corpus", "corpus_ivf"); got != n {
		t.Fatalf("the index holds %d entries for %d rows", got, n)
	}

	// Queries: half near stored vectors, half fresh draws from the same
	// distribution.
	var queries [][]float32
	for i := 0; i < 50; i++ {
		q := append([]float32(nil), vs[r.Intn(n)]...)
		for j := range q {
			q[j] += float32(0.05 * r.NormFloat64())
		}
		queries = append(queries, q)
	}
	queries = append(queries, clusteredVectors(rand.New(rand.NewSource(304)), 2000, 400, dims, 0.6)[1950:]...)

	// Another gateway, which has to find the centres for itself.
	s2 := sql.NewSession(tc.Nodes[2].DB(), catalog.NewAccessor())
	if p := planOf(t, ctx, s2, `SELECT id FROM corpus ORDER BY v <-> `+vecLit(queries[0])+` LIMIT 10`); !strings.Contains(p, `vector scan of index "corpus_ivf" (ivfflat, <->: the nearest 10 of 100 lists probed, approximate)`) {
		t.Fatalf("the default plan: %s", p)
	}
	exact, exactPer := exactKNN(t, ctx, s2, "corpus", "<->", queries, k)
	// The exact scan is itself held to the reference.
	for i := 0; i < 5; i++ {
		if want := refKNN(rows, "<->", queries[i], k, nil); fmt.Sprint(exact[i]) != fmt.Sprint(want) {
			t.Fatalf("exact scan: %v, reference %v", exact[i], want)
		}
	}
	t.Logf("exact scan (top-k heap): %s per query", exactPer.Round(10*time.Microsecond))
	recalls := map[string]float64{}
	for _, probes := range []string{"1", "3", "auto", "20", "100"} {
		rec, per := vectorRecall(t, ctx, s2, "corpus", "<->", byID, queries, exact, k, probes)
		recalls[probes] = rec
		t.Logf("probes = %-4s recall@%d = %.3f, %s per query", probes, k, rec, per.Round(10*time.Microsecond))
	}
	if recalls["100"] != 1 {
		t.Fatalf("probing every list is not exact: recall %.4f", recalls["100"])
	}
	if recalls["auto"] < 0.95 {
		t.Fatalf("recall@10 at the default probes is %.3f, want at least 0.95", recalls["auto"])
	}
	if recalls["1"] > recalls["auto"] || recalls["3"] > recalls["20"]+1e-9 {
		t.Fatalf("recall does not grow with probes: %v", recalls)
	}

	// A filter is applied to the candidates nearest first, so the answer
	// is the k nearest matching rows of the probed lists, not whatever
	// is left of the k nearest overall.
	execVec(t, ctx, s2, `SET ivfflat.probes = 100`)
	for i := 0; i < 5; i++ {
		q := fmt.Sprintf(`SELECT id FROM corpus WHERE grp = 3 ORDER BY v <-> %s LIMIT 10`, vecLit(queries[i]))
		indexed, exact := indexedAndExact(t, ctx, s2, q)
		if len(indexed) != 10 || fmt.Sprint(indexed) != fmt.Sprint(exact) {
			t.Fatalf("filtered k-NN through the index: %v, exact %v", indexed, exact)
		}
	}
	execVec(t, ctx, s2, `RESET ivfflat.probes`)
	// What the index is not used for: another distance, a descending
	// order, no limit, a second sort term.
	for _, q := range []string{
		`SELECT id FROM corpus ORDER BY v <=> ` + vecLit(queries[0]) + ` LIMIT 10`,
		`SELECT id FROM corpus ORDER BY v <-> ` + vecLit(queries[0]) + ` DESC LIMIT 10`,
		`SELECT id FROM corpus ORDER BY v <-> ` + vecLit(queries[0]),
		`SELECT id FROM corpus ORDER BY v <-> ` + vecLit(queries[0]) + `, id LIMIT 10`,
		`SELECT id FROM corpus WHERE id < 50 ORDER BY v <-> ` + vecLit(queries[0]) + ` LIMIT 10`,
	} {
		if p := planOf(t, ctx, s2, q); strings.Contains(p, "vector scan") {
			t.Fatalf("%s is planned through the index: %s", q, p)
		}
	}
}

// TestVectorIndexMaintenance: three indexes (one per distance) on one
// table stay exactly what the rows imply through inserts, updates that
// move a vector to another list, updates to and from NULL, and deletes,
// from concurrent sessions on every node; then through TRUNCATE; and
// DROP INDEX removes the entries and the centres.
func TestVectorIndexMaintenance(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const seed, dims, lists = 1200, 8, 12
	r := rand.New(rand.NewSource(9))
	vs := clusteredVectors(r, seed, 30, dims, 0.5)
	rows := make([]vecRow, seed)
	for i := range rows {
		rows[i] = vecRow{id: int64(i), grp: int64(i % 4), v: vs[i]}
	}
	execVec(t, ctx, s, fmt.Sprintf(`CREATE TABLE live (id INT8 PRIMARY KEY, grp INT8, v vector(%d))`, dims))
	insertVecRows(t, ctx, s, "live", rows)
	indexes := map[string]string{"<->": "live_l2", "<=>": "live_cos", "<#>": "live_ip"}
	for op, name := range indexes {
		m, _ := vector.MetricForOperator(op)
		execVec(t, ctx, s, fmt.Sprintf(`CREATE INDEX %s ON live USING ivfflat (v %s) WITH (lists = %d)`, name, m.OpClass(), lists))
		if got := checkVectorIndex(t, ctx, tc.Nodes[0].DB(), "live", name); got != seed {
			t.Fatalf("%s holds %d entries after the build, want %d", name, got, seed)
		}
	}

	var stop atomic.Bool
	var wg sync.WaitGroup
	var writes, failures, reads atomic.Int64
	for w := 0; w < 3; w++ {
		ws := sql.NewSession(tc.Nodes[w].DB(), catalog.NewAccessor())
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			wr := rand.New(rand.NewSource(int64(w) + 100))
			next := seed + w*1_000_000
			for !stop.Load() {
				v := vecLit(clusteredVectors(wr, 1, 1, dims, 1.5)[0])
				var q string
				switch wr.Intn(8) {
				case 0, 1:
					q = fmt.Sprintf(`INSERT INTO live VALUES (%d, %d, %s)`, next, wr.Intn(4), v)
					next++
				case 2:
					q = fmt.Sprintf(`DELETE FROM live WHERE id = %d`, wr.Intn(seed/3))
				case 3:
					q = fmt.Sprintf(`UPDATE live SET v = NULL WHERE id = %d`, seed/3+wr.Intn(seed/3))
				case 4:
					q = fmt.Sprintf(`UPDATE live SET grp = grp + 1 WHERE id = %d`, seed/3+wr.Intn(2*seed/3))
				default:
					// A new vector, usually under another list; often to a
					// row another writer set to NULL.
					q = fmt.Sprintf(`UPDATE live SET v = %s WHERE id = %d`, v, seed/3+wr.Intn(2*seed/3))
				}
				if _, serr := trySQL(ctx, ws, q); serr != nil {
					failures.Add(1)
				} else {
					writes.Add(1)
				}
			}
		}(w)
	}
	// A reader through the indexes while they change: whatever it sees
	// is a set of distinct rows in distance order.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rs := sql.NewSession(tc.Nodes[1].DB(), catalog.NewAccessor())
		rr := rand.New(rand.NewSource(77))
		for !stop.Load() {
			for op := range indexes {
				q := vecLit(vs[rr.Intn(seed)])
				res, serr := trySQL(ctx, rs, fmt.Sprintf(`SELECT id, v %s %s FROM live ORDER BY v %s %s LIMIT 10`, op, q, op, q))
				if serr != nil {
					// A retryable conflict with a writer is fine; an entry
					// that points at no row, or any other error, is not.
					if serr.Code != sql.CodeSerializationFailure {
						t.Errorf("a concurrent read through %s failed: [%s] %s", indexes[op], serr.Code, serr.Msg)
						return
					}
					continue
				}
				prev, seen := math.Inf(-1), map[int64]bool{}
				for _, row := range res.Rows {
					if seen[row[0].I] || row[1].Null || row[1].F < prev {
						t.Errorf("a concurrent read through %s returned %v", indexes[op], res.Rows)
						return
					}
					seen[row[0].I], prev = true, row[1].F
				}
				reads.Add(1)
			}
		}
	}()
	time.Sleep(4 * time.Second)
	stop.Store(true)
	wg.Wait()
	t.Logf("%d writes committed and %d failed (conflicts) under %d index reads", writes.Load(), failures.Load(), reads.Load())
	if writes.Load() < 100 || reads.Load() == 0 {
		t.Fatalf("too little happened: %d writes, %d reads", writes.Load(), reads.Load())
	}

	for op, name := range indexes {
		entries := checkVectorIndex(t, ctx, tc.Nodes[0].DB(), "live", name)
		var nonNull int64
		res := execVec(t, ctx, s, `SELECT count(*) FROM live WHERE v IS NOT NULL`)
		nonNull = res.Rows[0][0].I
		if int64(entries) != nonNull {
			t.Fatalf("%s holds %d entries for %d rows with a vector", name, entries, nonNull)
		}
		// With every list probed the index is exact.
		execVec(t, ctx, s, fmt.Sprintf(`SET ivfflat.probes = %d`, lists))
		for i := 0; i < 10; i++ {
			q := fmt.Sprintf(`SELECT id FROM live ORDER BY v %s %s LIMIT 10`, op, vecLit(vs[r.Intn(seed)]))
			indexed, exact := indexedAndExact(t, ctx, s, q)
			if fmt.Sprint(indexed) != fmt.Sprint(exact) {
				t.Fatalf("%s after the writes:\n index %v\n exact %v", q, indexed, exact)
			}
		}
		execVec(t, ctx, s, `RESET ivfflat.probes`)
	}

	// Rows with a NULL vector are in no list: the exact scan returns
	// them after every vector, the index does not return them at all.
	execVec(t, ctx, s, `CREATE TABLE few (id INT8 PRIMARY KEY, grp INT8, v vector(2))`)
	execVec(t, ctx, s, `INSERT INTO few VALUES (1, 0, '[1,1]'), (2, 0, NULL), (3, 0, '[5,5]')`)
	execVec(t, ctx, s, `CREATE INDEX few_ivf ON few USING ivfflat (v)`)
	indexed, exact := indexedAndExact(t, ctx, s, `SELECT id FROM few ORDER BY v <-> '[0,0]' LIMIT 5`)
	if fmt.Sprint(indexed) != "[1 3]" || fmt.Sprint(exact) != "[1 3 2]" {
		t.Fatalf("NULL vectors: index %v, exact %v", indexed, exact)
	}

	// TRUNCATE moves the indexes to fresh IDs; the centres stay, and new
	// rows are filed and found.
	_, _, before := vectorIndexEntries(t, ctx, tc.Nodes[0].DB(), "live", "live_l2")
	execVec(t, ctx, s, `TRUNCATE TABLE live`)
	insertVecRows(t, ctx, s, "live", rows[:300])
	_, _, after := vectorIndexEntries(t, ctx, tc.Nodes[0].DB(), "live", "live_l2")
	if after.ID == before.ID || after.Vector.Build != before.Vector.Build {
		t.Fatalf("TRUNCATE: index ID %d → %d, build %s → %s", before.ID, after.ID, before.Vector.Build, after.Vector.Build)
	}
	for op, name := range indexes {
		if got := checkVectorIndex(t, ctx, tc.Nodes[0].DB(), "live", name); got != 300 {
			t.Fatalf("%s holds %d entries after TRUNCATE and 300 inserts", name, got)
		}
		execVec(t, ctx, s, fmt.Sprintf(`SET ivfflat.probes = %d`, lists))
		q := fmt.Sprintf(`SELECT id FROM live ORDER BY v %s %s LIMIT 10`, op, vecLit(vs[5]))
		if indexed, exact := indexedAndExact(t, ctx, s, q); fmt.Sprint(indexed) != fmt.Sprint(exact) || len(indexed) != 10 {
			t.Fatalf("%s after TRUNCATE:\n index %v\n exact %v", q, indexed, exact)
		}
		execVec(t, ctx, s, `RESET ivfflat.probes`)
	}

	// DROP INDEX: the plan goes back to the scan, and the centres go.
	desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "live")
	prefix := keys.VectorCentroidPrefix(desc.ID, after.Vector.Build)
	if kvs, err := tc.Nodes[0].DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0); err != nil || len(kvs) != after.Vector.Lists {
		t.Fatalf("%d centres stored for %d lists (%v)", len(kvs), after.Vector.Lists, err)
	}
	execVec(t, ctx, s, `DROP INDEX live_l2`)
	if p := planOf(t, ctx, s, `SELECT id FROM live ORDER BY v <-> `+vecLit(vs[5])+` LIMIT 10`); !strings.Contains(p, "top-k heap") {
		t.Fatalf("after DROP INDEX: %s", p)
	}
	if kvs, err := tc.Nodes[0].DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0); err != nil || len(kvs) != 0 {
		t.Fatalf("%d centres left after DROP INDEX (%v)", len(kvs), err)
	}
	execVec(t, ctx, s, `INSERT INTO live VALUES (999999, 0, `+vecLit(vs[0])+`)`)
	for _, name := range []string{"live_cos", "live_ip"} {
		checkVectorIndex(t, ctx, tc.Nodes[0].DB(), "live", name)
	}
}

// TestVectorIndexBackfillUnderWrites: CREATE INDEX ... USING ivfflat on
// a table under concurrent inserts, updates and deletes from leased
// sessions on every node. When it returns, the index is exactly what
// the rows imply.
func TestVectorIndexBackfillUnderWrites(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	const ttl = time.Second
	s := leasedSession(t, tc, 0, ttl)
	waitForDatabases(t, ctx, s)
	const seed, dims, lists = 1500, 8, 10
	r := rand.New(rand.NewSource(5))
	vs := clusteredVectors(r, seed, 25, dims, 0.5)
	rows := make([]vecRow, seed)
	for i := range rows {
		rows[i] = vecRow{id: int64(i), grp: int64(i % 4), v: vs[i]}
	}
	execVec(t, ctx, s, fmt.Sprintf(`CREATE TABLE busy (id INT8 PRIMARY KEY, grp INT8, v vector(%d))`, dims))
	insertVecRows(t, ctx, s, "busy", rows)

	var stop atomic.Bool
	var wg sync.WaitGroup
	var writes, failures atomic.Int64
	for w := 0; w < 3; w++ {
		ws := leasedSession(t, tc, w%3, ttl)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			wr := rand.New(rand.NewSource(int64(w) + 1))
			next := seed + w*1_000_000
			for !stop.Load() {
				v := vecLit(clusteredVectors(wr, 1, 1, dims, 1.5)[0])
				var q string
				switch wr.Intn(4) {
				case 0:
					q = fmt.Sprintf(`INSERT INTO busy VALUES (%d, 0, %s)`, next, v)
					next++
				case 1:
					q = fmt.Sprintf(`DELETE FROM busy WHERE id = %d`, wr.Intn(seed/2))
				default:
					q = fmt.Sprintf(`UPDATE busy SET v = %s WHERE id = %d`, v, seed/2+wr.Intn(seed/2))
				}
				if _, serr := trySQL(ctx, ws, q); serr != nil {
					failures.Add(1)
				} else {
					writes.Add(1)
				}
			}
		}(w)
	}
	time.Sleep(300 * time.Millisecond)
	execVec(t, ctx, s, fmt.Sprintf(`CREATE INDEX busy_ivf ON busy USING ivfflat (v vector_l2_ops) WITH (lists = %d)`, lists))
	during := writes.Load()
	time.Sleep(300 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	if during == 0 {
		t.Fatal("no write committed while the index was being built")
	}
	t.Logf("%d writes committed during the build, %d after, %d failed", during, writes.Load()-during, failures.Load())

	entries := checkVectorIndex(t, ctx, tc.Nodes[0].DB(), "busy", "busy_ivf")
	if rowsN := execVec(t, ctx, s, `SELECT count(*) FROM busy`).Rows[0][0].I; int64(entries) != rowsN {
		t.Fatalf("the index holds %d entries for %d rows", entries, rowsN)
	}
	execVec(t, ctx, s, fmt.Sprintf(`SET ivfflat.probes = %d`, lists))
	for i := 0; i < 20; i++ {
		q := fmt.Sprintf(`SELECT id FROM busy ORDER BY v <-> %s LIMIT 10`, vecLit(vs[r.Intn(seed)]))
		if indexed, exact := indexedAndExact(t, ctx, s, q); fmt.Sprint(indexed) != fmt.Sprint(exact) {
			t.Fatalf("%s:\n index %v\n exact %v", q, indexed, exact)
		}
	}
	// IF NOT EXISTS on the index that now exists trains nothing and
	// leaves no centres behind.
	execVec(t, ctx, s, `CREATE INDEX IF NOT EXISTS busy_ivf ON busy USING ivfflat (v vector_l2_ops) WITH (lists = 3)`)
	desc := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "busy")
	aux := keys.TableIndexPrefix(desc.ID, keys.TableAuxIndexID)
	kvs, err := tc.Nodes[0].DB().Scan(ctx, aux, aux.PrefixEnd(), 0)
	if err != nil || len(kvs) != desc.Indexes[0].Vector.Lists {
		t.Fatalf("%d centre keys under the table for an index of %d lists (%v)", len(kvs), desc.Indexes[0].Vector.Lists, err)
	}
}

// TestVectorIndexSplitsMergesAndRestart: the index's lists are split
// across ranges and merged back, the process forgets the centres, and
// every node restarts in turn; queries, the centres' reload from the
// store, and maintenance keep working through each node.
func TestVectorIndexSplitsMergesAndRestart(t *testing.T) {
	tc, engines := StartWithEngines(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const n, dims, lists = 1200, 8, 10
	r := rand.New(rand.NewSource(11))
	vs := clusteredVectors(r, n, 25, dims, 0.5)
	rows := make([]vecRow, n)
	for i := range rows {
		rows[i] = vecRow{id: int64(i), v: vs[i]}
	}
	execVec(t, ctx, s, fmt.Sprintf(`CREATE TABLE kept (id INT8 PRIMARY KEY, grp INT8, v vector(%d))`, dims))
	insertVecRows(t, ctx, s, "kept", rows)
	execVec(t, ctx, s, fmt.Sprintf(`CREATE INDEX kept_ivf ON kept USING ivfflat (v vector_cosine_ops) WITH (lists = %d)`, lists))
	queries := make([]string, 8)
	for i := range queries {
		queries[i] = fmt.Sprintf(`SELECT id FROM kept ORDER BY v <=> %s LIMIT 10`, vecLit(vs[r.Intn(n)]))
	}
	check := func(s *sql.Session, where string) {
		t.Helper()
		execVec(t, ctx, s, fmt.Sprintf(`SET ivfflat.probes = %d`, lists))
		for _, q := range queries {
			indexed, exact := indexedAndExact(t, ctx, s, q)
			if len(indexed) != 10 || fmt.Sprint(indexed) != fmt.Sprint(exact) {
				t.Fatalf("%s: %s\n index %v\n exact %v", where, q, indexed, exact)
			}
		}
		execVec(t, ctx, s, `RESET ivfflat.probes`)
	}
	check(s, "after the build")

	// Split inside the index: within a list and between lists, so one
	// probe and one query both cross range boundaries. The boundaries
	// are sticky, or the merge queue would take them away again before
	// anything had read across them.
	got, _, idx := vectorIndexEntries(t, ctx, tc.Nodes[0].DB(), "kept", "kept_ivf")
	db := tc.Nodes[0].DB()
	isBoundary := func(k keys.Key) bool {
		t.Helper()
		d, ok, err := db.LookupMetaRecord(ctx, k)
		if err != nil || !ok {
			t.Fatalf("no range holds %s (%v)", keys.Pretty(k), err)
		}
		return d.StartKey.Equal(k)
	}
	var splits []keys.Key
	for _, at := range []int{len(got) / 5, len(got) / 2, 4 * len(got) / 5} {
		k := entryKey(got[at])
		if _, err := db.AdminSplitSticky(ctx, k, time.Now().Add(time.Hour).UnixNano()); err != nil {
			t.Fatalf("split inside the index: %v", err)
		}
		if !isBoundary(k) {
			t.Fatalf("the split at %s left no boundary", keys.Pretty(k))
		}
		splits = append(splits, k)
	}
	check(s, "after the splits")
	execVec(t, ctx, s, `UPDATE kept SET v = `+vecLit(vs[1])+` WHERE id = 0`)
	execVec(t, ctx, s, `DELETE FROM kept WHERE id = 2`)
	checkVectorIndex(t, ctx, db, "kept", "kept_ivf")
	for _, k := range splits {
		if !isBoundary(k) {
			t.Fatalf("the boundary at %s went before the index was read across it", keys.Pretty(k))
		}
	}
	// And merge the pieces back, right to left: the range that holds a
	// key absorbs its right-hand neighbour.
	first := entryKey(got[0])
	for i := len(splits) - 1; i >= 0; i-- {
		if _, err := db.AdminUnsplit(ctx, splits[i]); err != nil {
			t.Fatalf("releasing the boundary at %s: %v", keys.Pretty(splits[i]), err)
		}
		left := first
		if i > 0 {
			left = splits[i-1]
		}
		deadline := time.Now().Add(60 * time.Second)
		for isBoundary(splits[i]) {
			if _, err := db.AdminMerge(ctx, left); err != nil && time.Now().After(deadline) {
				t.Fatalf("merging across %s: %v", keys.Pretty(splits[i]), err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	check(s, "after the merges")
	checkVectorIndex(t, ctx, db, "kept", "kept_ivf")

	for i := 0; i < 3; i++ {
		tc.StopNode(i)
		// The nodes of a test cluster share one process, and so one
		// registry of centres: forgetting the build is what a real
		// restart does to it.
		vector.Forget(idx.Vector.Build)
		node := tc.RestartNode(i, engines[i])
		si := sql.NewSession(node.DB(), catalog.NewAccessor())
		check(si, fmt.Sprintf("through node %d after its restart", i+1))
		if _, ok := vector.Lookup(idx.Vector.Build); !ok {
			t.Fatalf("node %d answered without loading the centres", i+1)
		}
		execVec(t, ctx, si, fmt.Sprintf(`INSERT INTO kept VALUES (%d, 0, %s)`, 5000+i, vecLit(vs[10+i])))
		execVec(t, ctx, si, fmt.Sprintf(`UPDATE kept SET v = %s WHERE id = %d`, vecLit(vs[20+i]), 100+i))
	}
	sn := sql.NewSession(tc.Node(0).DB(), catalog.NewAccessor())
	check(sn, "after every node restarted")
	if got := checkVectorIndex(t, ctx, tc.Node(0).DB(), "kept", "kept_ivf"); got != n-1+3 {
		t.Fatalf("the index holds %d entries, want %d", got, n-1+3)
	}
}

// TestVectorIndexBackupRestore: a table with a vector index restores,
// full and incremental, into another cluster with the index usable,
// exact over what it probes, and maintained; and one table restores
// into the running cluster under a new name and table ID with its
// index and centres re-keyed along with its rows.
func TestVectorIndexBackupRestore(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const n, dims, lists = 800, 8, 8
	r := rand.New(rand.NewSource(21))
	vs := clusteredVectors(r, n+200, 20, dims, 0.5)
	rows := make([]vecRow, n)
	for i := range rows {
		rows[i] = vecRow{id: int64(i), grp: int64(i % 3), v: vs[i]}
	}
	execVec(t, ctx, s, fmt.Sprintf(`CREATE TABLE saved (id INT8 PRIMARY KEY, grp INT8, v vector(%d))`, dims))
	insertVecRows(t, ctx, s, "saved", rows)
	execVec(t, ctx, s, fmt.Sprintf(`CREATE INDEX saved_ivf ON saved USING ivfflat (v vector_ip_ops) WITH (lists = %d)`, lists))
	dirFull := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirFull, "", false, false); err != nil {
		t.Fatalf("full backup: %v", err)
	}
	// The incremental carries moved and removed entries and new ones.
	for i := 0; i < 50; i++ {
		execVec(t, ctx, s, fmt.Sprintf(`UPDATE saved SET v = %s WHERE id = %d`, vecLit(vs[n+i]), i))
	}
	execVec(t, ctx, s, `DELETE FROM saved WHERE id >= 700`)
	extra := make([]vecRow, 100)
	for i := range extra {
		extra[i] = vecRow{id: int64(2000 + i), v: vs[n+50+i]}
	}
	insertVecRows(t, ctx, s, "saved", extra)
	dirInc := t.TempDir()
	if _, err := tc.Nodes[0].RunBackup(ctx, dirInc, dirFull, false, false); err != nil {
		t.Fatalf("incremental backup: %v", err)
	}
	queries := make([]string, 8)
	for i := range queries {
		queries[i] = fmt.Sprintf(`SELECT id FROM saved ORDER BY v <#> %s LIMIT 10`, vecLit(vs[r.Intn(n)]))
	}
	execVec(t, ctx, s, fmt.Sprintf(`SET ivfflat.probes = %d`, lists))
	var want [][]int64
	for _, q := range queries {
		indexed, exact := indexedAndExact(t, ctx, s, q)
		if fmt.Sprint(indexed) != fmt.Sprint(exact) {
			t.Fatalf("source: %s\n index %v\n exact %v", q, indexed, exact)
		}
		want = append(want, indexed)
	}
	_, _, srcIdx := vectorIndexEntries(t, ctx, tc.Nodes[0].DB(), "saved", "saved_ivf")

	// Into the running cluster, as another table.
	if _, err := tc.Nodes[1].RunRestoreOpts(ctx, server.RestoreOptions{
		Srcs: []string{dirFull, dirInc}, Tables: []server.RestoreTable{{Name: "saved", Into: "saved_back"}},
	}); err != nil {
		t.Fatalf("table restore: %v", err)
	}
	rs := sql.NewSession(tc.Nodes[2].DB(), catalog.NewAccessor())
	execVec(t, ctx, rs, fmt.Sprintf(`SET ivfflat.probes = %d`, lists))
	for i, q := range queries {
		q = strings.Replace(q, "FROM saved", "FROM saved_back", 1)
		indexed, exact := indexedAndExact(t, ctx, rs, q)
		if fmt.Sprint(indexed) != fmt.Sprint(want[i]) || fmt.Sprint(exact) != fmt.Sprint(want[i]) {
			t.Fatalf("restored table: %s\n index %v\n exact %v\n source %v", q, indexed, exact, want[i])
		}
	}
	if got := checkVectorIndex(t, ctx, tc.Nodes[2].DB(), "saved_back", "saved_ivf"); got != 800 {
		t.Fatalf("the restored table's index holds %d entries, want 800", got)
	}
	execVec(t, ctx, rs, `INSERT INTO saved_back VALUES (9000, 0, `+vecLit(vs[3])+`)`)
	execVec(t, ctx, rs, `DELETE FROM saved_back WHERE id = 1`)
	checkVectorIndex(t, ctx, tc.Nodes[2].DB(), "saved_back", "saved_ivf")
	if def := strings.Join(rowsText(t, ctx, rs, `SELECT indexdef FROM pg_indexes WHERE tablename = 'saved_back'`), "\n"); !strings.Contains(def, "USING ivfflat (v vector_ip_ops)") {
		t.Fatalf("the restored table's index definition:\n%s", def)
	}
	// Its centres were re-keyed under the new table.
	back := lookupDescriptor(t, ctx, tc.Nodes[0].DB(), "saved_back")
	prefix := keys.VectorCentroidPrefix(back.ID, srcIdx.Vector.Build)
	if kvs, err := tc.Nodes[0].DB().Scan(ctx, prefix, prefix.PrefixEnd(), 0); err != nil || len(kvs) != srcIdx.Vector.Lists {
		t.Fatalf("%d centres under the restored table, want %d (%v)", len(kvs), srcIdx.Vector.Lists, err)
	}

	// Into an empty cluster, which has to read the centres from what
	// was restored.
	tc2 := Start(t, 1)
	if _, err := tc2.Nodes[0].RunRestore(ctx, []string{dirFull, dirInc}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	vector.Forget(srcIdx.Vector.Build)
	s2 := sql.NewSession(tc2.Nodes[0].DB(), catalog.NewAccessor())
	execVec(t, ctx, s2, fmt.Sprintf(`SET ivfflat.probes = %d`, lists))
	for i, q := range queries {
		indexed, exact := indexedAndExact(t, ctx, s2, q)
		if fmt.Sprint(indexed) != fmt.Sprint(want[i]) || fmt.Sprint(exact) != fmt.Sprint(want[i]) {
			t.Fatalf("restored cluster: %s\n index %v\n exact %v\n source %v", q, indexed, exact, want[i])
		}
	}
	if got := checkVectorIndex(t, ctx, tc2.Nodes[0].DB(), "saved", "saved_ivf"); got != 800 {
		t.Fatalf("the restored index holds %d entries, want 800", got)
	}
	execVec(t, ctx, s2, `INSERT INTO saved VALUES (9000, 0, `+vecLit(vs[3])+`)`)
	checkVectorIndex(t, ctx, tc2.Nodes[0].DB(), "saved", "saved_ivf")
}

// TestVectorIndexReshard: a re-shard rebuilds every index at a shadow
// ID. A vector index's entries move with it, filed under the same
// lists — the centres are the table's, not the index ID's — and the
// writes made after it are maintained.
func TestVectorIndexReshard(t *testing.T) {
	n, _ := startGCNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s := sql.NewSession(n.DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	execVec(t, ctx, s, `CREATE TABLE ev (series INT8, ts TIMESTAMPTZ, v vector(4), PRIMARY KEY (series, ts))
		WITH (timeseries = true, shards = 2)`)
	r := rand.New(rand.NewSource(8))
	vs := clusteredVectors(r, 80, 6, 4, 0.3)
	for i := 0; i < 60; i++ {
		execVec(t, ctx, s, `INSERT INTO ev VALUES ($1, $2, $3)`, types.NewInt(int64(i)), mustTS(t, 2026, 8, 30, i), types.NewString(vector.Format(vs[i])))
	}
	execVec(t, ctx, s, `CREATE INDEX ev_ivf ON ev USING ivfflat (v) WITH (lists = 4)`)
	if got := checkVectorIndex(t, ctx, n.DB(), "ev", "ev_ivf"); got != 60 {
		t.Fatalf("%d entries before the re-shard", got)
	}
	_, _, before := vectorIndexEntries(t, ctx, n.DB(), "ev", "ev_ivf")
	q := `SELECT series FROM ev ORDER BY v <-> ` + vecLit(vs[70]) + ` LIMIT 5`
	execVec(t, ctx, s, `SET ivfflat.probes = 4`)
	indexed, exact := indexedAndExact(t, ctx, s, q)
	if len(indexed) != 5 || fmt.Sprint(indexed) != fmt.Sprint(exact) {
		t.Fatalf("before the re-shard: index %v, exact %v", indexed, exact)
	}

	execVec(t, ctx, s, `ALTER TABLE ev SET (shards = 4)`)
	_, _, after := vectorIndexEntries(t, ctx, n.DB(), "ev", "ev_ivf")
	if after.ID == before.ID || after.Vector.Build != before.Vector.Build {
		t.Fatalf("re-shard: index ID %d → %d, build %s → %s", before.ID, after.ID, before.Vector.Build, after.Vector.Build)
	}
	if got := checkVectorIndex(t, ctx, n.DB(), "ev", "ev_ivf"); got != 60 {
		t.Fatalf("%d entries after the re-shard", got)
	}
	if again, exactAgain := indexedAndExact(t, ctx, s, q); fmt.Sprint(again) != fmt.Sprint(indexed) || fmt.Sprint(exactAgain) != fmt.Sprint(exact) {
		t.Fatalf("after the re-shard: index %v, exact %v, before %v", again, exactAgain, indexed)
	}
	execVec(t, ctx, s, `UPDATE ev SET v = `+vecLit(vs[71])+` WHERE series = 0`)
	execVec(t, ctx, s, `DELETE FROM ev WHERE series = 1`)
	execVec(t, ctx, s, `INSERT INTO ev VALUES (100, $1, `+vecLit(vs[72])+`)`, mustTS(t, 2026, 8, 30, 50))
	if got := checkVectorIndex(t, ctx, n.DB(), "ev", "ev_ivf"); got != 60 {
		t.Fatalf("%d entries after writes to the re-sharded table", got)
	}
	if indexed, exact := indexedAndExact(t, ctx, s, q); len(indexed) != 5 || fmt.Sprint(indexed) != fmt.Sprint(exact) {
		t.Fatalf("after writes to the re-sharded table: index %v, exact %v", indexed, exact)
	}
}

// TestVectorRowSecurity: nearest-neighbour queries — through the
// vector index and by the exact top-k scan — read rows through the
// ordinary row path, so a row-security policy filters them as it
// filters any read: a confined role sees only its tenant's rows, and
// the nearest of those, never a nearer row of another tenant. The
// pgvector setting and a policy's custom setting live side by side.
func TestVectorRowSecurity(t *testing.T) {
	tc := Start(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	const n, dims, lists = 900, 8, 6
	r := rand.New(rand.NewSource(294))
	vs := clusteredVectors(r, n, 12, dims, 0.5)
	rows := make([]vecRow, n)
	for i := range rows {
		rows[i] = vecRow{id: int64(i), grp: int64(i % 3), v: vs[i]}
	}
	execVec(t, ctx, s, fmt.Sprintf(`CREATE TABLE emb (id INT8 PRIMARY KEY, tenant INT8, v vector(%d))`, dims))
	insertVecRows(t, ctx, s, "emb", rows)
	execVec(t, ctx, s, fmt.Sprintf(`CREATE INDEX emb_ivf ON emb USING ivfflat (v) WITH (lists = %d)`, lists))
	execVec(t, ctx, s, `CREATE USER app PASSWORD 'app-pw-123'`)
	execVec(t, ctx, s, `GRANT SELECT ON emb TO app`)
	execVec(t, ctx, s, `ALTER TABLE emb ENABLE ROW LEVEL SECURITY`)
	execVec(t, ctx, s, `CREATE POLICY tenant_rows ON emb USING (tenant = current_setting('app.tenant')::int8)`)

	app := sql.NewSessionForUser(tc.Nodes[1].DB(), catalog.NewAccessor(), "app")
	execVec(t, ctx, app, `SET app.tenant = '1'`)
	execVec(t, ctx, app, fmt.Sprintf(`SET ivfflat.probes = %d`, lists))
	if got := rowsText(t, ctx, app, `SHOW ivfflat.probes`); fmt.Sprint(got) != fmt.Sprintf("[%d]", lists) {
		t.Fatalf("SHOW ivfflat.probes: %v", got)
	}
	if got := rowsText(t, ctx, app, `SELECT current_setting('app.tenant')`); fmt.Sprint(got) != "[1]" {
		t.Fatalf("the custom setting reads %v", got)
	}
	// ivfflat.probes is a setting of datax's own, checked as one, not a
	// custom setting that takes any text.
	if _, serr := trySQL(ctx, app, `SET ivfflat.probes = 'many'`); serr == nil || serr.Code != sql.CodeInvalidParameterValue {
		t.Fatalf("SET ivfflat.probes = 'many': %v", serr)
	}
	mine := func(r vecRow) bool { return r.grp == 1 }
	for i := 0; i < 10; i++ {
		qv := vs[r.Intn(n)] // often another tenant's row: the nearest is then not visible
		q := `SELECT id FROM emb ORDER BY v <-> ` + vecLit(qv) + ` LIMIT 10`
		if p := planOf(t, ctx, app, q); !strings.Contains(p, "row-level security policy applied") {
			t.Fatalf("the plan does not show the policy: %s", p)
		}
		indexed, exact := indexedAndExact(t, ctx, app, q)
		want := refKNN(rows, "<->", qv, 10, mine)
		if fmt.Sprint(indexed) != fmt.Sprint(want) || fmt.Sprint(exact) != fmt.Sprint(want) {
			t.Fatalf("%s as the confined role:\n index %v\n exact %v\n want  %v", q, indexed, exact, want)
		}
	}
	// Another tenant, the same session: the index answers for it.
	execVec(t, ctx, app, `SET app.tenant = '2'`)
	q := `SELECT id FROM emb ORDER BY v <-> ` + vecLit(vs[0]) + ` LIMIT 10`
	indexed, exact := indexedAndExact(t, ctx, app, q)
	want := refKNN(rows, "<->", vs[0], 10, func(r vecRow) bool { return r.grp == 2 })
	if fmt.Sprint(indexed) != fmt.Sprint(want) || fmt.Sprint(exact) != fmt.Sprint(want) {
		t.Fatalf("tenant 2:\n index %v\n exact %v\n want  %v", indexed, exact, want)
	}
}

// TestVectorNeedsV21: until the cluster has finalized v21 a node may be
// running a binary that cannot decode a vector's row value or would
// maintain the index as an ordinary one, so neither is created.
func TestVectorNeedsV21(t *testing.T) {
	tc := StartWithOptions(t, 1, func(c *server.Config) { c.BinaryVersionOverride = version.V20 })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	waitForDatabases(t, ctx, s)
	for _, q := range []string{
		`CREATE TABLE old (id INT8 PRIMARY KEY, v vector(3))`,
		`CREATE TABLE old (id INT8 PRIMARY KEY, v vector)`,
	} {
		if _, serr := trySQL(ctx, s, q); serr == nil || serr.Code != sql.CodeFeatureNotSupported || !strings.Contains(serr.Msg, "v21") {
			t.Fatalf("%s at v20: %v", q, serr)
		}
	}
	execVec(t, ctx, s, `CREATE TABLE old (id INT8 PRIMARY KEY, note TEXT)`)
	if _, serr := trySQL(ctx, s, `ALTER TABLE old ADD COLUMN v vector(3)`); serr == nil || !strings.Contains(serr.Msg, "v21") {
		t.Fatalf("ADD COLUMN vector at v20: %v", serr)
	}
	if _, serr := trySQL(ctx, s, `CREATE INDEX old_ivf ON old USING ivfflat (note)`); serr == nil || serr.Code != sql.CodeFeatureNotSupported || !strings.Contains(serr.Msg, "v21") {
		t.Fatalf("CREATE INDEX ... USING ivfflat at v20: %v", serr)
	}
	// The operators and functions compute on values, which need no
	// stored format.
	if got := rowsText(t, ctx, s, `SELECT '[1,2,3]'::vector <-> '[4,6,3]', vector_dims('[1,2]'::vector)`); fmt.Sprint(got) != "[5|2]" {
		t.Fatalf("vector expressions at v20: %v", got)
	}
}

// BenchmarkVectorKNN compares ORDER BY distance LIMIT 10 through an
// ivfflat index (at the default probes and at 1) with the exact top-k
// scan, over 20,000 vectors of 64 dimensions on one node:
//
//	go test ./pkg/testutils/testcluster -run - -bench VectorKNN -benchtime 50x
func BenchmarkVectorKNN(b *testing.B) {
	const n, dims, lists = 20000, 64, 100
	tc := Start(b, 1)
	ctx := context.Background()
	s := sql.NewSession(tc.Nodes[0].DB(), catalog.NewAccessor())
	r := rand.New(rand.NewSource(1))
	vs := clusteredVectors(r, n, 400, dims, 0.6)
	rows := make([]vecRow, n)
	byID := make(map[int64][]float32, n)
	for i := range rows {
		rows[i] = vecRow{id: int64(i), v: vs[i]}
		byID[int64(i)] = vs[i]
	}
	execVec(b, ctx, s, fmt.Sprintf(`CREATE TABLE bench_vec (id INT8 PRIMARY KEY, grp INT8, v vector(%d))`, dims))
	insertVecRows(b, ctx, s, "bench_vec", rows)
	start := time.Now()
	execVec(b, ctx, s, fmt.Sprintf(`CREATE INDEX bench_vec_ivf ON bench_vec USING ivfflat (v vector_l2_ops) WITH (lists = %d)`, lists))
	b.Logf("index over %d × %d built in %s", n, dims, time.Since(start).Round(time.Millisecond))
	queries := make([][]float32, 50)
	for i := range queries {
		queries[i] = clusteredVectors(r, 1, 1, dims, 0.6)[0]
		copy(queries[i], vs[r.Intn(n)])
		queries[i][0] += 0.05
	}
	exact, _ := exactKNN(b, ctx, s, "bench_vec", "<->", queries, 10)
	for _, probes := range []string{"1", "auto", "30"} {
		rec, _ := vectorRecall(b, ctx, s, "bench_vec", "<->", byID, queries, exact, 10, probes)
		b.Logf("probes = %s: recall@10 = %.3f", probes, rec)
	}
	for _, mode := range []struct{ name, set string }{
		{"indexed-probes-1", `SET vector_index_scan = on; SET ivfflat.probes = 1`},
		{"indexed-probes-auto", `SET vector_index_scan = on; RESET ivfflat.probes`},
		{"indexed-probes-30", `SET vector_index_scan = on; SET ivfflat.probes = 30`},
		{"exact", `SET vector_index_scan = off`},
	} {
		b.Run(mode.name, func(b *testing.B) {
			execVec(b, ctx, s, mode.set)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				q := queries[i%len(queries)]
				if got := idsInOrder(b, ctx, s, `SELECT id FROM bench_vec ORDER BY v <-> `+vecLit(q)+` LIMIT 10`); len(got) != 10 {
					b.Fatalf("%d rows", len(got))
				}
			}
		})
	}
}
