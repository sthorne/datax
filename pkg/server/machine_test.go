package server

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sthorne/datax/pkg/kvpb"
	"github.com/sthorne/datax/pkg/util/sysstats"
)

// TestMachineSummaryProjectsTheSample (issue #146): the heartbeat's
// kvpb.MachineSummary is a projection of the node's sysstats.Sample —
// the console reads one shape for both — so every summary field must
// exist in the sample under the same JSON name (uptime is the one
// documented exception: the sample says process_uptime_seconds), and
// machineSummary must copy every field.
func TestMachineSummaryProjectsTheSample(t *testing.T) {
	jsonNames := func(v any) map[string]bool {
		out := map[string]bool{}
		rt := reflect.TypeOf(v)
		for i := 0; i < rt.NumField(); i++ {
			tag := rt.Field(i).Tag.Get("json")
			if name, _, _ := strings.Cut(tag, ","); name != "" && name != "-" {
				out[name] = true
			}
		}
		return out
	}
	sample := jsonNames(sysstats.Sample{})
	renamed := map[string]string{"uptime_seconds": "process_uptime_seconds"}
	for name := range jsonNames(kvpb.MachineSummary{}) {
		if r, ok := renamed[name]; ok {
			name = r
		}
		if !sample[name] {
			t.Fatalf("MachineSummary field %q has no Sample field of that JSON name", name)
		}
	}
	// Every field the summary has, the projection copies.
	n := &Node{sys: sysstats.New("")}
	s := n.sys.Sample()
	_ = s
	got := n.machineSummary()
	if got == nil {
		t.Fatal("no summary from a sampled node")
	}
	rv := reflect.ValueOf(*got)
	latest := n.sys.Latest()
	lv := reflect.ValueOf(latest)
	for i := 0; i < rv.NumField(); i++ {
		f := rv.Type().Field(i)
		src := f.Name
		if f.Name == "UptimeSeconds" {
			src = "ProcessUp"
		}
		want := lv.FieldByName(src)
		if !want.IsValid() {
			t.Fatalf("MachineSummary.%s has no Sample counterpart", f.Name)
		}
		if !reflect.DeepEqual(rv.Field(i).Interface(), want.Interface()) {
			t.Fatalf("MachineSummary.%s = %v, Sample.%s = %v: the projection does not copy it", f.Name, rv.Field(i).Interface(), src, want.Interface())
		}
	}
}
