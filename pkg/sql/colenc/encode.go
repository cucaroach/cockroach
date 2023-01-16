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
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func InsertBatch(ctx context.Context, rh *row.RowHelper, b coldata.Batch, p row.Putter, colMap catalog.TableColMap) error {
	err := EncodePK(ctx, rh, p, rh.TableDesc, rh.TableDesc.GetPrimaryIndex(), b.ColVecs(), colMap)
	if err != nil {
		return err
	}
	for _, ind := range rh.TableDesc.WritableNonPrimaryIndexes() {
		err = EncodeSecondaryIndex(ctx, rh, p, rh.TableDesc, ind, b.ColVecs(), colMap, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// valuesideEncodeCol is the vector version of valueside.Encode.
func valuesideEncodeCol(appendTo []byte, typ *types.T, colID valueside.ColumnIDDelta, vec coldata.Vec, row int) ([]byte, error) {
	if vec.Nulls().NullAt(row) {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), nil
	}
	switch typ.Family() {
	case types.BoolFamily:
		bs := vec.Bool()
		return encoding.EncodeBoolValue(appendTo, uint32(colID), bs[row]), nil
	case types.IntFamily, types.DateFamily:
		is := vec.Int64()
		return encoding.EncodeIntValue(appendTo, uint32(colID), is[row]), nil
	case types.FloatFamily:
		fs := vec.Float64()
		return encoding.EncodeFloatValue(appendTo, uint32(colID), fs[row]), nil
	case types.DecimalFamily:
		ds := vec.Decimal()
		return encoding.EncodeDecimalValue(appendTo, uint32(colID), &ds[row]), nil
	case types.BytesFamily, types.StringFamily:
		b := vec.Bytes().Get(row)
		return encoding.EncodeBytesValue(appendTo, uint32(colID), b), nil
	case types.JsonFamily:
		j := vec.JSON().Get(row)
		encoded, err := json.EncodeJSON(nil, j)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeJSONValue(appendTo, uint32(colID), encoded), nil
	case types.TimestampFamily, types.TimestampTZFamily:
		t := vec.Timestamp()[row]
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t), nil
	case types.IntervalFamily:
		d := vec.Interval()[row]
		return encoding.EncodeDurationValue(appendTo, uint32(colID), d), nil
	case types.UuidFamily:
		b := vec.Bytes().Get(row)
		u, err := uuid.FromBytes(b)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeUUIDValue(appendTo, uint32(colID), u), nil

	// case types.EncodedKeyFamily:
	// 	so := t.Geometry.SpatialObjectRef()
	// 	spaceCurveIndex, err := t.Geometry.SpaceCurveIndex()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if dir == encoding.Ascending {
	// 		return encoding.EncodeGeoAscending(b, spaceCurveIndex, so)
	// 	}
	// 	return encoding.EncodeGeoDescending(b, spaceCurveIndex, so)
	default:
		//TODO:Must be Datum col?
		return valueside.Encode(appendTo, colID, vec.Datum().Get(row).(tree.Datum), nil)
	}
}

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
func encodeIndexKey(kys []roachpb.Key, count int, vecs []coldata.Vec, keyCols []fetchpb.IndexFetchSpec_KeyColumn, colIDToRowIndex catalog.TableColMap, pkoffsets []int32) error {
	for _, k := range keyCols {
		dir, err := catalogkeys.IndexColumnEncodingDirection(k.Direction)
		if err != nil {
			return err
		}
		col, ok := colIDToRowIndex.Get(k.ColumnID)
		var vec coldata.Vec
		if ok {
			vec = vecs[col]
		}
		for row := 0; row < count; row++ {
			if kys[row], err = encodeKey(kys[row], k.Type, dir, vec, row); err != nil {
				return err
			}
			if pkoffsets != nil {
				pkoffsets[row] = int32(len(kys[row]))
			}
		}
	}
	return nil
}

func EncodePK(ctx context.Context, rh *row.RowHelper, p row.Putter, desc catalog.TableDescriptor,
	ind catalog.Index, vecs []coldata.Vec,
	colMap catalog.TableColMap) error {

	keyAndSuffixCols := desc.IndexFetchSpecKeyAndSuffixColumns(ind)
	keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]
	fetchedCols := desc.PublicColumns()
	count := vecs[0].Length()
	kys := make([]roachpb.Key, count)
	rowBufSize := 5*len(keyCols) + len(rh.PrimaryIndexKeyPrefix)
	buffer := make([]byte, 0, count*rowBufSize)
	families := desc.GetFamilies()
	// TODO: what are key length limits, could this be int16? Is int32 okay?
	var pkoffsets []int32

	// Store the index up to the family id so we can reuse the prefixes
	if len(families) > 0 {
		pkoffsets = make([]int32, count)
	}

	// Initialize count buffers sliced from one big buffer
	for row := 0; row < count; row++ {
		offset := row * rowBufSize
		kys[row] = buffer[offset : offset : rowBufSize+offset]
		kys[row] = append(kys[row], rh.PrimaryIndexKeyPrefix...)
	}

	if err := encodeIndexKey(kys, count, vecs, keyCols, colMap, pkoffsets); err != nil {
		return err
	}

	for i := range families {
		// lastColIDs := make([]catid.ColumnID, count)
		// values := make([]roachpb.Value, count)

		family := &families[i]
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := colMap.Get(colID); ok {
				update = true
				break
			}
		}
		// We can have an empty family.ColumnIDs in the following case:
		// * A table is created with the primary key not in family 0, and another column in family 0.
		// * The column in family 0 is dropped, leaving the 0'th family empty.
		// In this case, we must keep the empty 0'th column family in order to ensure that column family 0
		// is always encoded as the sentinel k/v for a row.
		if !update && len(family.ColumnIDs) != 0 {
			continue
		}

		familySortedColumnIDs, ok := rh.SortedColumnFamily(family.ID)
		if !ok {
			return errors.AssertionFailedf("invalid family sorted column id map")
		}
		var values [][]byte
		var lastColIDs []catid.ColumnID

		// reset keys for new family
		if i > 0 {
			buffer := make([]byte, 0, count*rowBufSize)
			for row := 0; row < count; row++ {
				offset := row * rowBufSize
				newkys := make([]roachpb.Key, count)
				newkys[row] = buffer[offset : offset : rowBufSize+offset]
				newkys[row] = append(newkys[row], kys[row][:pkoffsets[row]]...)
				kys = newkys
			}

		}

		for rowoffset := 0; rowoffset < count; rowoffset++ {
			kys[rowoffset] = keys.MakeFamilyKey(kys[rowoffset], uint32(family.ID))
		}

		// We need to ensure that column family 0 contains extra metadata, like composite primary key values.
		// Additionally, the decoders expect that column family 0 is encoded with a TUPLE value tag, so we
		// don't want to use the untagged value encoding.
		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID && family.ID != 0 {
			// Storage optimization to store DefaultColumnID directly as a value. Also
			// backwards compatible with the original BaseFormatVersion.

			// TODO: valColIDMapping vs. updatedColIDMapping????
			idx, ok := colMap.Get(family.DefaultColumnID)
			if !ok {
				continue
			}

			typ := fetchedCols[idx].GetType()
			vec := vecs[idx]
			for row := 0; row < count; row++ {
				marshaled, err := MarshalLegacy(typ, vec, row)
				if err != nil {
					return err
				}

				if marshaled.RawBytes == nil {
					if false /*overwrite*/ {
						// If the new family contains a NULL value, then we must
						// delete any pre-existing row.
						//insertDelFn(ctx, batch, kvKey, traceKV)
					}
				} else {
					// We only output non-NULL values. Non-existent column keys are
					// considered NULL during scanning and the row sentinel ensures we know
					// the row exists.
					// if err := helper.checkRowSize(ctx, kvKey, &marshaled, family.ID); err != nil {
					// 	return nil, err
					// }
					// TODO(cucaroach): make this use the bulk put operations and push traceKV stuff beneath Putter interface
					if rh.TraceKV {
						log.VEventfDepth(ctx, 1, 2, "CPut %s -> %s", kys[row], marshaled.PrettyPrint())
						p.CPut(&kys[row], &marshaled, nil /* expValue */)
					} else {
						p.CPut(&kys[row], &marshaled, nil /* expValue */)
					}
				}
			}

			continue
		}

		if values == nil {
			values = make([][]byte, count)
		}
		if lastColIDs == nil {
			lastColIDs = make([]catid.ColumnID, count)
		}

		for _, colID := range familySortedColumnIDs {
			idx, ok := colMap.Get(colID)
			if !ok {
				// Column not being updated or inserted.
				continue
			}

			// FIXME: composite datum handling wrong
			if skip, err := rh.SkipColumnNotInPrimaryIndexValue(colID, tree.DNull); err != nil {
				return err
			} else if skip {
				continue
			}

			col := fetchedCols[idx]
			typ := col.GetType()
			vec := vecs[idx]
			for row := 0; row < count; row++ {
				if vec.Nulls().NullAt(row) {
					continue
				}
				if lastColIDs[row] > colID {
					return errors.AssertionFailedf("cannot write column id %d after %d", colID, lastColIDs[row])
				}
				colIDDelta := valueside.MakeColumnIDDelta(lastColIDs[row], colID)
				lastColIDs[row] = colID
				var err error
				values[row], err = valuesideEncodeCol(values[row], typ, colIDDelta, vec, row)
				if err != nil {
					return err
				}
			}
		}

		//TODO: overwrite makes this a plain put
		p.CPutTuples(kys, values)
	}

	return nil
}

type KVS struct {
	Keys   []roachpb.Key
	Values [][]byte
}

var _ sort.Interface = &KVS{}

func (k *KVS) Len() int {
	return len(k.Keys)
}
func (k *KVS) Less(i, j int) bool {
	return bytes.Compare(k.Keys[i], k.Keys[j]) < 0
}
func (k *KVS) Swap(i, j int) {
	k.Keys[i], k.Keys[j] = k.Keys[j], k.Keys[i]
	k.Values[i], k.Values[j] = k.Values[j], k.Values[i]
}

func EncodeSecondaryIndex(ctx context.Context, rh *row.RowHelper, p row.Putter,
	desc catalog.TableDescriptor, ind catalog.Index,
	vecs []coldata.Vec, colMap catalog.TableColMap, srt bool) error {
	var err error
	secondaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(rh.Codec, desc.GetID(), ind.GetID())

	// Use the primary key encoding for covering indexes.
	if ind.GetEncodingType() == catenumpb.PrimaryIndexEncoding {
		return EncodePK(ctx, rh, p, desc, ind, vecs, colMap)
	}

	keyAndSuffixCols := desc.IndexFetchSpecKeyAndSuffixColumns(ind)
	keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]
	// TODO: we should re-use these
	count := vecs[0].Length()
	kys := make([]roachpb.Key, count)
	rowBufSize := 5*len(keyCols) + len(secondaryIndexKeyPrefix)
	buffer := make([]byte, 0, count*rowBufSize)

	for rowoffset := 0; rowoffset < count; rowoffset++ {
		offset := rowoffset * rowBufSize
		kys[rowoffset] = buffer[offset : offset : rowBufSize+offset]
		kys[rowoffset] = append(kys[rowoffset], secondaryIndexKeyPrefix...)
	}

	extraKeys, _, err := encodeColumnsValues(ind.IndexDesc().KeySuffixColumnIDs, nil /*directions*/, colMap, vecs, nil /*keyPrefixs*/)
	if err != nil {
		return err
	}

	var containsNull coldata.Nulls
	if ind.GetType() == descpb.IndexDescriptor_INVERTED {
		// Since the inverted indexes generate multiple keys per row just handle them
		// separately.
		return encodeInvertedSecondaryIndex(kys, ind, vecs, colMap, p, extraKeys)
	} else {
		if err := encodeIndexKey(kys, count, vecs, keyCols, colMap, nil); err != nil {
			return err
		}
	}

	for rowoffset := 0; rowoffset < count; rowoffset++ {
		if !ind.IsUnique() || containsNull.NullAt(rowoffset) {
			kys[rowoffset] = append(kys[rowoffset], extraKeys[rowoffset]...)
		}
		// TODO: secondary index family support
		kys[rowoffset] = keys.MakeFamilyKey(kys[rowoffset], 0)
	}

	if desc.NumFamilies() == 1 || ind.GetVersion() == descpb.BaseIndexFormatVersion {
		if err := encodeSecondaryIndexNoFamilies(ind, p, colMap, kys, extraKeys, vecs); err != nil {
			return err
		}
	} else {

	}
	return nil
}

func encodeSecondaryIndexNoFamilies(ind catalog.Index, p row.Putter,
	colMap catalog.TableColMap, kys []roachpb.Key, extraKeyCols [][]byte, vecs []coldata.Vec) error {
	var values [][]byte
	var err error
	if ind.IsUnique() {
		values = extraKeyCols
	} else {
		// TODO: make preallocated slices out of big slice
		values = make([][]byte, vecs[0].Length())
	}
	cols := rowenc.GetValueColumns(ind)
	values, err = writeColumnValues(values, colMap, vecs, cols)
	if err != nil {
		return err
	}
	if ind.ForcePut() {
		p.PutBytes(kys, values)
	} else {
		p.InitPutBytes(kys, values, false)
	}
	return nil
}

func writeColumnValues(values [][]byte, colMap catalog.TableColMap, vecs []coldata.Vec,
	cols []rowenc.ValueEncodedColumn) ([][]byte, error) {
	var err error
	count := vecs[0].Length()
	lastColIDs := make([]catid.ColumnID, count)

	for _, col := range cols {
		idx, ok := colMap.Get(col.Id)
		if !ok {
			// Column not being updated or inserted.
			continue
		}
		vec := vecs[idx]
		for row := 0; row < count; row++ {
			if vec.Nulls().NullAt(row) {
				continue
			}
			if lastColIDs[row] > col.Id {
				return nil, errors.AssertionFailedf("cannot write column id %d after %d", col.Id, lastColIDs[row])
			}
			colIDDelta := valueside.MakeColumnIDDelta(lastColIDs[row], col.Id)
			lastColIDs[row] = col.Id
			values[row], err = valuesideEncodeCol(values[row], vec.Type(), colIDDelta, vec, row)
			if err != nil {
				return nil, err
			}
		}
	}
	return values, nil
}

// encodeColumns is the vector version of rowenc.EncodeColumns
func encodeColumnsKeys(
	columnIDs []descpb.ColumnID,
	directions []catenumpb.IndexColumn_Direction,
	colMap catalog.TableColMap,
	vecs []coldata.Vec,
	keyPrefixes []roachpb.Key) ([]roachpb.Key, *coldata.Nulls, error) {
	count := vecs[0].Length()
	keys := make([]roachpb.Key, count)
	var nulls coldata.Nulls
	var err error
	for _, id := range columnIDs {
		var vec coldata.Vec
		var typ *types.T
		i, ok := colMap.Get(id)
		if ok {
			vec = vecs[i]
			typ = vec.Type()
			if vec.Nulls().MaybeHasNulls() {
				nulls = nulls.Or(*vec.Nulls())
			}
		}
		for row := 0; row < count; row++ {
			if keys[row], err = encodeKey(keyPrefixes[row], typ, encoding.Ascending, vec, row); err != nil {
				return nil, nil, err
			}
		}
	}
	return keys, &nulls, nil
}

// encodeColumns is the vector version of rowenc.EncodeColumns, TODO: can we use generics to get rid of COPY of above function?
func encodeColumnsValues(
	columnIDs []descpb.ColumnID,
	directions []catenumpb.IndexColumn_Direction,
	colMap catalog.TableColMap,
	vecs []coldata.Vec,
	keyPrefixes [][]byte) ([][]byte, *coldata.Nulls, error) {
	count := vecs[0].Length()
	keys := make([][]byte, count)
	var nulls coldata.Nulls
	var err error
	for _, id := range columnIDs {
		var vec coldata.Vec
		var typ *types.T
		i, ok := colMap.Get(id)
		if ok {
			vec = vecs[i]
			typ = vec.Type()
			if vec.Nulls().MaybeHasNulls() {
				nulls = nulls.Or(*vec.Nulls())
			}
		}
		for row := 0; row < count; row++ {
			var pref []byte
			if keyPrefixes != nil {
				pref = keyPrefixes[row]
			}
			if keys[row], err = encodeKey(pref, typ, encoding.Ascending, vec, row); err != nil {
				return nil, nil, err
			}
		}
	}
	return keys, &nulls, nil
}
