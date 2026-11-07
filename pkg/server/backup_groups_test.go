package server

import (
	"reflect"
	"strings"
	"testing"
)

// TestManifestKeyGroupsCoverTheManifest (issue #242): restore holds each
// of the manifest's raw-record groups to the spans a backup collects it
// from (#238), and the groups live in two places — the manifest's
// []backupKV fields, which RunBackup fills, and manifestKeyGroups, which
// checkManifestKeys walks. A sixth group added to the struct and
// collected, but not named in the table, would be restored unchecked
// with no test failing. So: every []backupKV field of backupManifest is
// named in the table, under its JSON tag; every table entry names a
// field; and each entry's accessor returns that very field. The join is
// the JSON tag, which is also what a manifest on disk is keyed by. A raw
// group is any field whose type is a slice of backupKV, named or not;
// a group held one level down — inside a sub-struct, or as a map of
// slices — is beyond what this test sees, so the manifest keeps its raw
// groups as top-level fields.
func TestManifestKeyGroupsCoverTheManifest(t *testing.T) {
	kvElem := reflect.TypeOf(backupKV{})
	mt := reflect.TypeOf(backupManifest{})
	fields := map[string]int{} // json name -> field index
	for i := 0; i < mt.NumField(); i++ {
		f := mt.Field(i)
		if f.Type.Kind() != reflect.Slice || f.Type.Elem() != kvElem {
			continue
		}
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			t.Fatalf("backupManifest.%s is a raw-record group with no JSON name", f.Name)
		}
		fields[tag] = i
	}
	if len(fields) == 0 {
		t.Fatal("backupManifest has no slice-of-backupKV field; the test is looking at the wrong type")
	}
	seen := map[string]bool{}
	for _, g := range manifestKeyGroups {
		idx, ok := fields[g.name]
		if !ok {
			t.Errorf("manifestKeyGroups names %q, which is no slice-of-backupKV field of backupManifest", g.name)
			continue
		}
		if seen[g.name] {
			t.Errorf("manifestKeyGroups names %q twice", g.name)
		}
		seen[g.name] = true
		// The accessor returns the field it is named for: set a sentinel
		// through reflection and read it back through the accessor.
		var man backupManifest
		sentinel := []backupKV{{Key: []byte("sentinel-" + g.name)}}
		field := reflect.ValueOf(&man).Elem().Field(idx)
		field.Set(reflect.ValueOf(sentinel).Convert(field.Type()))
		got := g.kvs(&man)
		if len(got) != 1 || string(got[0].Key) != "sentinel-"+g.name {
			t.Errorf("manifestKeyGroups[%q].kvs returns something other than backupManifest's %q field", g.name, g.name)
		}
		if len(g.spans()) == 0 {
			t.Errorf("manifestKeyGroups[%q] has no spans: every key in it would be refused", g.name)
		}
	}
	for name := range fields {
		if !seen[name] {
			t.Errorf("backupManifest's %q is a raw-record group that manifestKeyGroups does not hold to any span: restore would write any key it carries", name)
		}
	}
}
