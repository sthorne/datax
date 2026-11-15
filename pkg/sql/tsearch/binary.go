package tsearch

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// PostgreSQL's binary wire formats of the two types (tsvectorsend,
// tsquerysend), so that drivers which ask for binary results or send
// binary parameters see what a PostgreSQL server would send.
//
// A tsvector is an int32 count of lexemes and, for each, its bytes with
// a terminating zero, a uint16 count of positions, and the positions:
// uint16s holding the position in the low 14 bits and the weight in the
// top two (A is 3).
//
// A tsquery is an int32 count of items and the items in prefix order —
// an operator, then its RIGHT operand, then its left. An operand is the
// byte 1, a byte with its weight set (A is bit 3), a byte that is 1 for
// a prefix match, and the lexeme with a terminating zero. An operator
// is the byte 2 and a byte naming it (1 not, 2 and, 3 or, 4 phrase),
// followed for a phrase by its distance as a uint16.

// AppendBinary appends the vector's binary form.
func (v Vector) AppendBinary(b []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
	for _, e := range v {
		b = append(b, e.Lexeme...)
		b = append(b, 0)
		b = binary.BigEndian.AppendUint16(b, uint16(len(e.Pos)))
		for _, p := range e.Pos {
			b = binary.BigEndian.AppendUint16(b, p.P|uint16(p.W)<<14)
		}
	}
	return b
}

// ParseVectorBinary reads a vector's binary form.
func ParseVectorBinary(b []byte) (Vector, error) {
	bad := fmt.Errorf("malformed binary tsvector")
	if len(b) < 4 {
		return nil, bad
	}
	n := int(binary.BigEndian.Uint32(b))
	b = b[4:]
	bld := newBuilder()
	for i := 0; i < n; i++ {
		end := bytes.IndexByte(b, 0)
		if end <= 0 || len(b) < end+3 {
			return nil, bad
		}
		lex := string(b[:end])
		if len(lex) > MaxLexemeLen {
			return nil, fmt.Errorf("word is too long (%d bytes, max %d bytes)", len(lex), MaxLexemeLen)
		}
		npos := int(binary.BigEndian.Uint16(b[end+1:]))
		b = b[end+3:]
		if len(b) < 2*npos {
			return nil, bad
		}
		pos := make([]Pos, npos)
		for j := range pos {
			raw := binary.BigEndian.Uint16(b[2*j:])
			pos[j] = Pos{P: raw & 0x3fff, W: Weight(raw >> 14)}
			if pos[j].P == 0 {
				return nil, bad
			}
		}
		b = b[2*npos:]
		if err := bld.add(lex, pos...); err != nil {
			return nil, err
		}
	}
	if len(b) != 0 {
		return nil, bad
	}
	return bld.vector(), nil
}

const (
	binVal = 1
	binOpr = 2
)

// AppendBinary appends the query's binary form.
func (q Query) AppendBinary(b []byte) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(q.NumNodes()))
	var walk func(n *Node)
	walk = func(n *Node) {
		switch n.Kind {
		case NodeVal:
			prefix := byte(0)
			if n.Prefix {
				prefix = 1
			}
			b = append(b, binVal, n.Weights, prefix)
			b = append(b, n.Lexeme...)
			b = append(b, 0)
		case NodeNot:
			b = append(b, binOpr, 1)
			walk(n.L)
		default:
			op := map[NodeKind]byte{NodeAnd: 2, NodeOr: 3, NodePhrase: 4}[n.Kind]
			b = append(b, binOpr, op)
			if n.Kind == NodePhrase {
				b = binary.BigEndian.AppendUint16(b, uint16(n.Distance))
			}
			walk(n.R)
			walk(n.L)
		}
	}
	if q.Root != nil {
		walk(q.Root)
	}
	return b
}

// ParseQueryBinary reads a query's binary form.
func ParseQueryBinary(b []byte) (Query, error) {
	bad := fmt.Errorf("malformed binary tsquery")
	if len(b) < 4 {
		return Query{}, bad
	}
	left := int(binary.BigEndian.Uint32(b))
	b = b[4:]
	if left == 0 {
		if len(b) != 0 {
			return Query{}, bad
		}
		return Query{}, nil
	}
	var read func(depth int) (*Node, error)
	read = func(depth int) (*Node, error) {
		if left == 0 || len(b) < 2 || depth > maxQueryDepth {
			return nil, bad
		}
		left--
		switch b[0] {
		case binVal:
			if len(b) < 3 {
				return nil, bad
			}
			weights, prefix := b[1], b[2]
			end := bytes.IndexByte(b[3:], 0)
			if end <= 0 || weights > 0xf || prefix > 1 || end > MaxLexemeLen {
				return nil, bad
			}
			n := &Node{Kind: NodeVal, Lexeme: string(b[3 : 3+end]), Prefix: prefix == 1, Weights: weights}
			b = b[3+end+1:]
			return n, nil
		case binOpr:
			op := b[1]
			b = b[2:]
			switch op {
			case 1:
				kid, err := read(depth + 1)
				if err != nil {
					return nil, err
				}
				return &Node{Kind: NodeNot, L: kid}, nil
			case 2, 3, 4:
				n := &Node{Kind: map[byte]NodeKind{2: NodeAnd, 3: NodeOr, 4: NodePhrase}[op]}
				if op == 4 {
					if len(b) < 2 {
						return nil, bad
					}
					n.Distance = int(binary.BigEndian.Uint16(b))
					b = b[2:]
				}
				var err error
				if n.R, err = read(depth + 1); err != nil {
					return nil, err
				}
				if n.L, err = read(depth + 1); err != nil {
					return nil, err
				}
				return n, nil
			}
		}
		return nil, bad
	}
	root, err := read(0)
	if err != nil {
		return Query{}, err
	}
	if left != 0 || len(b) != 0 {
		return Query{}, bad
	}
	return Query{Root: root}, nil
}
