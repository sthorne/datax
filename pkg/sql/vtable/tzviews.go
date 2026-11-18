package vtable

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
)

// pg_timezone_names and pg_timezone_abbrevs (gap PG-C-008), from the
// IANA database Go embeds: each zone's abbreviation, offset and
// daylight-saving state at the statement's time, and the abbreviations
// datax accepts in AT TIME ZONE.

var (
	tzOnce sync.Once
	tzLocs []*time.Location // tzNames' zones, loaded once
	// tzAbbrevDST maps an abbreviation and offset some zone of the
	// database uses to whether it is daylight-saving time there.
	tzAbbrevDST map[tzAbbrev]bool
)

type tzAbbrev struct {
	name   string
	offset int
}

func loadZones() {
	tzOnce.Do(func() {
		tzAbbrevDST = map[tzAbbrev]bool{}
		year := time.Now().UTC().Year()
		for _, name := range tzNames {
			loc, err := time.LoadLocation(name)
			if err != nil {
				continue
			}
			tzLocs = append(tzLocs, loc)
			for _, month := range []time.Month{time.January, time.July} {
				t := time.Date(year, month, 15, 12, 0, 0, 0, loc)
				abbr, off := t.Zone()
				key := tzAbbrev{abbr, off}
				tzAbbrevDST[key] = tzAbbrevDST[key] || t.IsDST()
			}
		}
	})
}

// secondsInterval is an interval of whole seconds (utc_offset).
func secondsInterval(secs int) types.Datum {
	return types.NewInterval(types.Interval{Nanos: int64(secs) * int64(time.Second)})
}

func init() {
	pg("pg_timezone_names", []catalog.Column{col("name", types.String), col("abbrev", types.String), col("utc_offset", types.IntervalFam), col("is_dst", types.Bool)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			loadZones()
			now := time.Now()
			if env.Now != 0 {
				now = time.Unix(0, env.Now)
			}
			rows := make([]Row, 0, len(tzLocs))
			for _, loc := range tzLocs {
				t := now.In(loc)
				abbr, off := t.Zone()
				rows = append(rows, Row{str(loc.String()), str(abbr), secondsInterval(off), boolean(t.IsDST())})
			}
			return rows, nil
		})
	pg("pg_timezone_abbrevs", []catalog.Column{col("abbrev", types.String), col("utc_offset", types.IntervalFam), col("is_dst", types.Bool)},
		func(ctx context.Context, env *Env) ([]Row, error) {
			loadZones()
			abbrevs := types.ZoneAbbrevs()
			names := make([]string, 0, len(abbrevs))
			for n := range abbrevs {
				names = append(names, n)
			}
			sort.Strings(names)
			rows := make([]Row, 0, len(names))
			for _, n := range names {
				off := abbrevs[n]
				rows = append(rows, Row{str(n), secondsInterval(off), boolean(tzAbbrevDST[tzAbbrev{n, off}])})
			}
			return rows, nil
		})
}
