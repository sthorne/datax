package rowenc

import (
	"fmt"
	"strings"
	"time"

	"github.com/sthorne/datax/pkg/keys"
	"github.com/sthorne/datax/pkg/sql/catalog"
	"github.com/sthorne/datax/pkg/sql/types"
	"github.com/sthorne/datax/pkg/util/encoding"
)

// PrettyKey renders a key of desc's table by name: /table/<name>/<index
// name>/<typed datums>, the primary index as "primary". Keys outside the
// table, and bytes past what the schema explains, fall back to
// keys.Pretty's shape-based rendering.
func PrettyKey(desc *catalog.TableDescriptor, k keys.Key) string {
	if desc == nil {
		return keys.Pretty(k)
	}
	prefix := keys.TableDataPrefix(desc.ID)
	if !k.HasPrefix(prefix) {
		return keys.Pretty(k)
	}
	rest := []byte(k[len(prefix):])
	var b strings.Builder
	b.WriteString("/table/" + desc.Name)
	if len(rest) == 0 {
		return b.String()
	}
	rest, indexID, err := encoding.DecodeUint64(rest)
	if err != nil {
		return b.String() + keys.PrettyDatums(rest)
	}
	// The column families the key carries under this index, in order.
	var fams []types.Family
	var idx *catalog.IndexDescriptor
	for i := range desc.Indexes {
		if desc.Indexes[i].ID == indexID {
			idx = &desc.Indexes[i]
		}
	}
	switch {
	case indexID == desc.LivePrimaryIndex() || (idx == nil && indexID == PrimaryIndexID):
		b.WriteString("/primary")
		fams = pkFamilies(desc)
	case idx != nil:
		b.WriteString("/" + idx.Name)
		for _, id := range idx.ColumnIDs {
			c, _ := desc.ColByID(id)
			fams = append(fams, c.Type)
		}
		if !idx.Unique {
			fams = append(fams, pkFamilies(desc)...)
		}
	default:
		fmt.Fprintf(&b, "/%d", indexID)
	}
	for _, fam := range fams {
		if len(rest) == 0 {
			break
		}
		tail, text, err := prettyDatum(rest, fam)
		if err != nil {
			break
		}
		b.WriteString("/" + text)
		rest = tail
	}
	if len(rest) > 0 {
		b.WriteString(keys.PrettyDatums(rest))
	}
	return b.String()
}

func pkFamilies(desc *catalog.TableDescriptor) []types.Family {
	fams := make([]types.Family, 0, len(desc.PrimaryKey))
	for _, id := range desc.PrimaryKey {
		c, _ := desc.ColByID(id)
		fams = append(fams, c.Type)
	}
	return fams
}

func prettyDatum(b []byte, fam types.Family) ([]byte, string, error) {
	switch fam {
	case types.Int:
		rest, v, err := encoding.DecodeInt64(b)
		return rest, fmt.Sprintf("%d", v), err
	case types.Timestamp:
		rest, v, err := encoding.DecodeInt64(b)
		return rest, time.Unix(0, v).UTC().Format("2006-01-02T15:04:05.999999999Z"), err
	case types.Date:
		rest, v, err := encoding.DecodeInt64(b)
		return rest, time.Unix(v*86400, 0).UTC().Format("2006-01-02"), err
	case types.Float:
		rest, v, err := encoding.DecodeFloat64(b)
		return rest, fmt.Sprintf("%g", v), err
	case types.String:
		rest, s, err := encoding.DecodeString(b)
		return rest, fmt.Sprintf("%q", s), err
	case types.Bytes, types.Uuid:
		rest, s, err := encoding.DecodeString(b)
		return rest, types.Datum{Fam: fam, S: s}.Text(), err
	case types.Bool:
		rest, v, err := encoding.DecodeBool(b)
		return rest, fmt.Sprintf("%t", v), err
	case types.Decimal:
		rest, d, err := encoding.DecodeDecimal(b)
		return rest, d.String(), err
	}
	return nil, "", fmt.Errorf("unrenderable type %s", fam)
}
