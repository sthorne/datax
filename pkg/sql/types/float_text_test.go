package types

import (
	"math"
	"strings"
	"testing"
)

// TestFormatFloat8 checks float8 text against PostgreSQL 16's float8out
// at four extra_float_digits settings (issue #365: COPY TO and the wire
// render floats byte for byte as PostgreSQL does).
func TestFormatFloat8(t *testing.T) {
	vals := []float64{0, math.Copysign(0, -1), 1.5, 0.1, 123456789.125, 1e15, 1e14, 123456789012345.6, 0.0001,
		0.00001234, 1e100, -2.5e-300, math.NaN(), math.Inf(1), math.Inf(-1), 1.0 / 3, 2.0 / 3 * 1e20, 100}
	want := map[int]string{
		1:   "0 -0 1.5 0.1 123456789.125 1e+15 100000000000000 123456789012345.6 0.0001 1.234e-05 1e+100 -2.5e-300 NaN Infinity -Infinity 0.3333333333333333 6.666666666666666e+19 100",
		0:   "0 -0 1.5 0.1 123456789.125 1e+15 100000000000000 123456789012346 0.0001 1.234e-05 1e+100 -2.5e-300 NaN Infinity -Infinity 0.333333333333333 6.66666666666667e+19 100",
		-3:  "0 -0 1.5 0.1 123456789.125 1e+15 1e+14 1.23456789012e+14 0.0001 1.234e-05 1e+100 -2.5e-300 NaN Infinity -Infinity 0.333333333333 6.66666666667e+19 100",
		-15: "0 -0 2 0.1 1e+08 1e+15 1e+14 1e+14 0.0001 1e-05 1e+100 -2e-300 NaN Infinity -Infinity 0.3 7e+19 1e+02",
	}
	for extra, w := range want {
		var got []string
		for _, v := range vals {
			if extra > 0 {
				got = append(got, FormatFloat8(v))
				if s := NewFloat(v).Text(); s != got[len(got)-1] {
					t.Fatalf("Text(%v) = %q, FormatFloat8 %q", v, s, got[len(got)-1])
				}
			} else {
				got = append(got, FormatFloat8Digits(v, extra))
			}
		}
		if g := strings.Join(got, " "); g != w {
			t.Errorf("extra_float_digits %d:\n got %s\nwant %s", extra, g, w)
		}
	}
}
