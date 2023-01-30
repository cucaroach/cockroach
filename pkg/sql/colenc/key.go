// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colenc

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// encodeKey is the columnar version of keyside.Encode.
// cases taken from decodeTableKeyToCol
func encodeKey(b []byte, typ *types.T, dir encoding.Direction, vec coldata.Vec, row int) ([]byte, error) {
	if vec == nil || vec.Nulls().NullAt(row) {
		if dir == encoding.Ascending {
			return encoding.EncodeNullAscending(b), nil
		}
		return encoding.EncodeNullDescending(b), nil
	}
	switch typ.Family() {
	case types.BoolFamily:
		bs := vec.Bool()
		var x int64
		if bs[row] {
			x = 1
		}
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, x), nil
		}
		return encoding.EncodeVarintDescending(b, x), nil
	case types.IntFamily, types.DateFamily:
		is := vec.Int64()
		i := is[row]
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, i), nil
		}
		return encoding.EncodeVarintDescending(b, i), nil
	case types.FloatFamily:
		fs := vec.Float64()
		f := fs[row]
		if dir == encoding.Ascending {
			return encoding.EncodeFloatAscending(b, f), nil
		}
		return encoding.EncodeFloatDescending(b, f), nil
	case types.DecimalFamily:
		ds := vec.Decimal()
		d := &ds[row]
		if dir == encoding.Ascending {
			return encoding.EncodeDecimalAscending(b, d), nil
		}
		return encoding.EncodeDecimalDescending(b, d), nil
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		ss := vec.Bytes()
		s := ss.Get(row)
		if dir == encoding.Ascending {
			// TODO: make sure string() isn't allocating...
			return encoding.EncodeStringAscending(b, string(s)), nil
		}
		return encoding.EncodeStringDescending(b, string(s)), nil
	case types.TimestampFamily, types.TimestampTZFamily:
		ts := vec.Timestamp().Get(row)
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, ts), nil
		}
		return encoding.EncodeTimeDescending(b, ts), nil
	case types.IntervalFamily:
		d := vec.Interval().Get(row)
		if dir == encoding.Ascending {
			return encoding.EncodeDurationAscending(b, d)
		}
		return encoding.EncodeDurationDescending(b, d)
	case types.EncodedKeyFamily:
		return append(b, vec.Bytes().Get(row)...), nil
	default:
		return keyside.Encode(b, vec.Datum().Get(row).(tree.Datum), dir)
	}
}

// encodeKeyCols is the vector version of rowenc.EncodeIndexKey
func (b *BatchEncoder) encodeIndexKey(kys []roachpb.Key, keyCols []fetchpb.IndexFetchSpec_KeyColumn,
	pkoffsets []int32, nulls coldata.Nulls) error {
	for _, k := range keyCols {
		if k.IsComposite {
			b.compositeColumnIDs.Add(int(k.ColumnID))
		}
		dir, err := catalogkeys.IndexColumnEncodingDirection(k.Direction)
		if err != nil {
			return err
		}
		col, ok := b.colMap.Get(k.ColumnID)
		var vec coldata.Vec
		if ok {
			vec = b.b.ColVec(col)
		} else {
			nulls.SetNulls()
		}
		for row := 0; row < b.b.Length(); row++ {
			// Elided partial index keys will be nil
			if kys[row] == nil {
				continue
			}
			if kys[row], err = encodeKey(kys[row], k.Type, dir, vec, row); err != nil {
				return err
			}

			if pkoffsets != nil {
				pkoffsets[row] = int32(len(kys[row]))
			}
		}
		if vec.Nulls().MaybeHasNulls() {
			nulls = nulls.Or(*vec.Nulls())
		}
	}
	return nil
}
