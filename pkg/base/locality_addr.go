package base

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// LocalityAddress is one --locality-advertise entry: the address peers
// that share Tier with this node (the same key AND value in their own
// locality) dial it at, in place of its public --advertise address — a
// private intra-region network, say.
type LocalityAddress struct {
	Tier    Tier   `json:"tier"`
	Address string `json:"address"`
}

func (la LocalityAddress) String() string {
	return la.Tier.Key + "=" + la.Tier.Value + "@" + la.Address
}

// FormatLocalityAddresses renders entries as the flag spells them.
func FormatLocalityAddresses(las []LocalityAddress) string {
	parts := make([]string, len(las))
	for i, la := range las {
		parts[i] = la.String()
	}
	return strings.Join(parts, ",")
}

// ParseLocalityAddresses parses a --locality-advertise value, a
// comma-separated list of key=value@host:port entries, against the node's
// own locality: every entry's tier must be one of loc's tiers, its address
// a host:port, and no tier may be named twice. "" is no entries.
func ParseLocalityAddresses(s string, loc Locality) ([]LocalityAddress, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []LocalityAddress
	seen := map[Tier]bool{}
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		at := strings.LastIndex(entry, "@")
		if at <= 0 || at == len(entry)-1 {
			return nil, fmt.Errorf("invalid entry %q: expected key=value@host:port", entry)
		}
		tl, err := ParseLocality(entry[:at])
		if err != nil || len(tl.Tiers) != 1 {
			return nil, fmt.Errorf("invalid entry %q: expected key=value@host:port", entry)
		}
		tier := tl.Tiers[0]
		if !loc.HasTier(tier) {
			have := loc.String()
			if have == "" {
				have = "none: --locality is not set"
			}
			return nil, fmt.Errorf("entry %q: tier %s is not one of this node's --locality tiers (%s)", entry, tier.Key+"="+tier.Value, have)
		}
		if seen[tier] {
			return nil, fmt.Errorf("entry %q: duplicate tier %s", entry, tier.Key+"="+tier.Value)
		}
		seen[tier] = true
		addr := entry[at+1:]
		if err := checkHostPort(addr); err != nil {
			return nil, fmt.Errorf("entry %q: invalid address %q: %v", entry, addr, err)
		}
		out = append(out, LocalityAddress{Tier: tier, Address: addr})
	}
	return out, nil
}

// checkHostPort accepts host:port with a non-empty host and a numeric port
// in 1..65535: an address another machine can dial.
func checkHostPort(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		return fmt.Errorf("missing host")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("port %q is not a number in 1-65535", port)
	}
	return nil
}

// HasTier reports whether t (key and value) is one of l's tiers.
func (l Locality) HasTier(t Tier) bool {
	for _, have := range l.Tiers {
		if have == t {
			return true
		}
	}
	return false
}

// SelectLocalityAddress picks the address a node with locality local
// should dial a peer at, given the peer's --locality-advertise entries:
// the first entry whose tier (same key and value) is one of local's
// tiers. ok is false when none is, and the peer's public address applies.
func SelectLocalityAddress(entries []LocalityAddress, local Locality) (addr string, ok bool) {
	for _, la := range entries {
		if la.Address != "" && local.HasTier(la.Tier) {
			return la.Address, true
		}
	}
	return "", false
}
