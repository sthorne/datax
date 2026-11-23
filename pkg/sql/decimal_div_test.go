package sql

import (
	"math"
	"testing"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// decWithScale is a DECIMAL datum carrying a declared display scale, as
// a value read from a NUMERIC(p,s) column does.
func decWithScale(s string, scale int32) types.Datum {
	d := types.NewDecimal(s)
	d.Dscale = scale
	return d
}

// TestDecimalDivisionScale: numeric `/` picks PostgreSQL's result scale
// (select_div_scale: at least 16 significant digits, at least either
// operand's display scale) instead of a fixed 6 places, so a tiny
// quotient is not rounded to 0. The expected texts are PostgreSQL 16's.
func TestDecimalDivisionScale(t *testing.T) {
	for _, c := range []struct {
		l, r types.Datum
		want string
	}{
		{types.NewDecimal("0.0000005"), types.NewInt(1), "0.000000500000000000000000"},
		{types.NewDecimal("0.0000001"), types.NewInt(3), "0.000000033333333333333333"},
		{types.NewDecimal("1"), types.NewInt(3), "0.33333333333333333333"},
		{types.NewInt(7), types.NewDecimal("2"), "3.5000000000000000"},
		{types.NewDecimal("-2"), types.NewDecimal("3"), "-0.66666666666666666667"},
		// The declared scale of a NUMERIC(30,2) operand is kept.
		{decWithScale("12345678901234567890.1", 2), types.NewInt(1), "12345678901234567890.10"},
	} {
		got, err := applyArithIn(c.l, "/", c.r, nil)
		if err != nil {
			t.Fatalf("%s / %s: %v", c.l.Text(), c.r.Text(), err)
		}
		if got.Fam != types.Decimal || got.Text() != c.want {
			t.Errorf("%s / %s: got %s %q, want %q", c.l.Text(), c.r.Text(), got.Fam, got.Text(), c.want)
		}
	}
	for _, r := range []types.Datum{types.NewInt(0), types.NewDecimal("0")} {
		_, err := applyArithIn(types.NewDecimal("1.5"), "/", r, nil)
		if e, ok := err.(*Error); !ok || e.Code != CodeDivisionByZero {
			t.Fatalf("1.5 / %s: got %v, want SQLSTATE %s", r.Text(), err, CodeDivisionByZero)
		}
	}
}

// TestAvgDecimalScale: avg over DECIMAL/INT is sum / count through the
// same numeric division (PostgreSQL's numeric_avg), not a 6-place
// quantize.
func TestAvgDecimalScale(t *testing.T) {
	col := catalog.Column{ID: 1, Name: "x", Type: types.Decimal}
	icol := catalog.Column{ID: 1, Name: "x", Type: types.Int}
	for _, c := range []struct {
		col  catalog.Column
		in   []types.Datum
		want string
	}{
		{col, []types.Datum{types.NewDecimal("0.0000001"), types.NewDecimal("0.0000002")}, "0.000000150000000000000000"},
		{col, []types.Datum{types.NewDecimal("1"), types.NewDecimal("2"), types.NewDecimal("2")}, "1.6666666666666667"},
		{icol, []types.Datum{types.NewInt(1), types.NewInt(2), types.NewInt(2)}, "1.6666666666666667"},
		// An input's declared scale carries into the result's.
		{col, []types.Datum{decWithScale("12345678901234567890.1", 3), types.NewDecimal("12345678901234567890.2")}, "12345678901234567890.150"},
	} {
		desc := &catalog.TableDescriptor{Name: "t", Columns: []catalog.Column{c.col}}
		specs := []aggSpec{{fn: "AVG", col: c.col, argType: c.col.Type, name: "avg"}}
		st := newAggState(1)
		for _, d := range c.in {
			if err := st.accumulate(specs, desc, map[catalog.ColumnID]types.Datum{1: d}, nil); err != nil {
				t.Fatal(err)
			}
		}
		out, err := st.finish(specs, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := out[0].Text(); got != c.want {
			t.Errorf("avg%v: got %q, want %q", c.in, got, c.want)
		}
	}
}

// TestAvgInt8DoesNotOverflow: avg over int8 totals in numeric, as
// PostgreSQL's int8_avg_accum does, so a total beyond int8 is no error.
func TestAvgInt8DoesNotOverflow(t *testing.T) {
	icol := catalog.Column{ID: 1, Name: "x", Type: types.Int}
	desc := &catalog.TableDescriptor{Name: "t", Columns: []catalog.Column{icol}}
	in := []types.Datum{types.NewInt(math.MaxInt64), types.NewInt(math.MaxInt64), types.NewInt(1)}
	specs := []aggSpec{{fn: "AVG", col: icol, argType: types.Int, name: "avg"}}
	st := newAggState(1)
	for _, d := range in {
		if err := st.accumulate(specs, desc, map[catalog.ColumnID]types.Datum{1: d}, nil); err != nil {
			t.Fatalf("avg: %v", err)
		}
	}
	out, err := st.finish(specs, nil)
	if err != nil {
		t.Fatalf("avg: %v", err)
	}
	// (2^64 - 1) / 3, exact; at 19 integer digits select_div_scale
	// asks for no fraction.
	if want := "6148914691236517205"; out[0].Fam != types.Decimal || out[0].Text() != want {
		t.Errorf("avg: got %s %q, want DECIMAL %q", out[0].Fam, out[0].Text(), want)
	}
}
