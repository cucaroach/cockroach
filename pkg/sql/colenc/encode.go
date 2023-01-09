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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func valuesideEncodeCol(appendTo []byte, typ *types.T, colID valueside.ColumnIDDelta, vec coldata.Vec, row int) ([]byte, error) {
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
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		b := vec.Bytes().Get(row)
		return encoding.EncodeBytesValue(appendTo, uint32(colID), b), nil
	// case types.TimestampFamily, types.TimestampTZFamily:
	// 	if dir == encoding.Ascending {
	// 		return encoding.EncodeStringAscending(b, string(*t)), nil
	// 	}
	// 	return encoding.EncodeStringDescending(b, string(*t)), nil
	// case types.IntervalFamily:
	// 	return encoding.EncodeVoidAscendingOrDescending(b), nil
	// case types.JsonFamily:
	// 	if dir == encoding.Ascending {
	// 		return encoding.EncodeBox2DAscending(b, t.CartesianBoundingBox.BoundingBox)
	// 	}
	// 	return encoding.EncodeBox2DDescending(b, t.CartesianBoundingBox.BoundingBox)
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
	}

	panic(false)
}

func colencodingEncodeKey(b []byte, typ *types.T, dir encoding.Direction, vec coldata.Vec, row int) []byte {
	// keyside.Encode(key, val, dir); err != nil {
	// cases taken from decodeTableKeyToCol
	switch typ.Family() {
	case types.BoolFamily:
		bs := vec.Bool()
		var x int64
		if bs[row] {
			x = 1
		} else {
			x = 0
		}
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, x)
		}
		return encoding.EncodeVarintDescending(b, x)
	case types.IntFamily, types.DateFamily:
		is := vec.Int64()
		i := is[row]
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, i)
		}
		return encoding.EncodeVarintDescending(b, i)
	case types.FloatFamily:
		fs := vec.Float64()
		f := fs[row]
		if dir == encoding.Ascending {
			return encoding.EncodeFloatAscending(b, f)
		}
		return encoding.EncodeFloatDescending(b, f)
	case types.DecimalFamily:
		ds := vec.Decimal()
		d := &ds[row]
		if dir == encoding.Ascending {
			return encoding.EncodeDecimalAscending(b, d)
		}
		return encoding.EncodeDecimalDescending(b, d)
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		ss := vec.Bytes()
		s := ss.Get(row)
		if dir == encoding.Ascending {
			// TODO: make sure string() isn't allocating...
			return encoding.EncodeStringAscending(b, string(s))
		}
		return encoding.EncodeStringDescending(b, string(s))
	// case types.TimestampFamily, types.TimestampTZFamily:
	// 	if dir == encoding.Ascending {
	// 		return encoding.EncodeStringAscending(b, string(*t)), nil
	// 	}
	// 	return encoding.EncodeStringDescending(b, string(*t)), nil
	// case types.IntervalFamily:
	// 	return encoding.EncodeVoidAscendingOrDescending(b), nil
	// case types.JsonFamily:
	// 	if dir == encoding.Ascending {
	// 		return encoding.EncodeBox2DAscending(b, t.CartesianBoundingBox.BoundingBox)
	// 	}
	// 	return encoding.EncodeBox2DDescending(b, t.CartesianBoundingBox.BoundingBox)
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
	}

	panic(false)
}

func EncodePK(ctx context.Context, rh *row.RowHelper, batch *kv.Batch, desc catalog.TableDescriptor,
	ind catalog.Index, vecs []coldata.Vec,
	colIDToRowIndex catalog.TableColMap) error {

	// EncodeIndexKey
	keyAndSuffixCols := desc.IndexFetchSpecKeyAndSuffixColumns(ind)
	keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]
	fetchedCols := desc.PublicColumns()
	count := vecs[0].Length()
	kys := make([]roachpb.Key, count)
	rowBufSize := 5*len(keyCols) + len(rh.PrimaryIndexKeyPrefix)
	buffer := make([]byte, 0, count*rowBufSize)

	// Initialize count buffers sliced from one big buffer
	for row := 0; row < count; row++ {
		offset := row * rowBufSize
		kys[row] = buffer[offset : offset : rowBufSize+offset]
		kys[row] = append(kys[row], rh.PrimaryIndexKeyPrefix...)
	}

	//func EncodePartialIndexKey(
	for i := range keyCols {
		keyCol := &keyCols[i]
		dir, err := catalogkeys.IndexColumnEncodingDirection(keyCol.Direction)
		if err != nil {
			return err
		}
		col, ok := colIDToRowIndex.Get(keyCol.ColumnID)
		if !ok {
			panic("nulls not handled")
		}
		vec := vecs[col]
		for row := 0; row < count; row++ {
			kys[row] = colencodingEncodeKey(kys[row], keyCol.Type, dir, vec, row)
		}
	}
	families := desc.GetFamilies()
	for i := range families {
		// lastColIDs := make([]catid.ColumnID, count)
		// values := make([]roachpb.Value, count)

		family := &families[i]
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := colIDToRowIndex.Get(colID); ok {
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
		firstTime := true
		for _, colID := range familySortedColumnIDs {
			idx, ok := colIDToRowIndex.Get(colID)
			// FIXME: fix null handling
			if !ok /*|| values[idx] == tree.DNull */ {
				// Column not being updated or inserted.
				continue
			}

			// FIXME: composite datum handling wrong
			if skip, err := rh.SkipColumnNotInPrimaryIndexValue(colID, tree.DNull); err != nil {
				return err
			} else if skip {
				continue
			}

			if firstTime {
				firstTime = false
				// TODO: this is broken for more than 1 family
				for rowoffset := 0; rowoffset < count; rowoffset++ {
					kys[rowoffset] = keys.MakeFamilyKey(kys[rowoffset], uint32(family.ID))
				}
				if values == nil {
					values = make([][]byte, count)
				}
				if lastColIDs == nil {
					lastColIDs = make([]catid.ColumnID, count)
				}
			}

			col := fetchedCols[idx]
			typ := col.GetType()
			vec := vecs[idx]
			for row := 0; row < count; row++ {
				if lastColIDs[row] > colID {
					return errors.AssertionFailedf("cannot write column id %d after %d", colID, lastColIDs[row])
				}
				colIDDelta := valueside.MakeColumnIDDelta(lastColIDs[row], colID)
				lastColIDs[row] = colID
				// check col type and vec type
				var err error
				values[row], err = valuesideEncodeCol(values[row], typ, colIDDelta, vec, row)
				if err != nil {
					return err
				}
			}
		}

		if rh.TraceKV {
			var value roachpb.Value
			for rowoffset := 0; rowoffset < count; rowoffset++ {
				//TODO: overwrite makes this a plain put
				value.SetTuple(values[rowoffset])
				log.VEventfDepth(ctx, 1, 2, "CPut %s -> %s", kys[rowoffset], value.PrettyPrint())
				batch.CPut(&kys[rowoffset], &value, nil /* expValue */)
			}
		} else {
			batch.CPutTuples(kys, values)
		}
	}

	return nil
}

type KVS struct {
	keys   []roachpb.Key
	values [][]byte
}

var _ sort.Interface = &KVS{}

func (k *KVS) Len() int {
	return len(k.keys)
}
func (k *KVS) Less(i, j int) bool {
	return bytes.Compare(k.keys[i], k.keys[j]) < 0
}
func (k *KVS) Swap(i, j int) {
	k.keys[i], k.keys[j] = k.keys[j], k.keys[i]
	k.values[i], k.values[j] = k.values[j], k.values[i]
}

func EncodeSecondaryIndex(ctx context.Context, rh *row.RowHelper, b *kv.Batch,
	desc catalog.TableDescriptor, ind catalog.Index,
	vecs []coldata.Vec, colMap catalog.TableColMap, srt bool) error {
	fetchedCols := desc.PublicColumns()
	//entries, err := rowenc.EncodeSecondaryIndex(rh.Codec, rh.TableDesc, index, colIDtoRowIndex, values, includeEmpty)
	secondaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(rh.Codec, desc.GetID(), ind.GetID())
	_ = secondaryIndexKeyPrefix

	// Use the primary key encoding for covering indexes.
	if ind.GetEncodingType() == descpb.PrimaryIndexEncoding {
		return EncodePK(ctx, rh, b, desc, ind, vecs, colMap)
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
	//func EncodePartialIndexKey(
	for i := range keyCols {
		keyCol := &keyCols[i]
		dir, err := catalogkeys.IndexColumnEncodingDirection(keyCol.Direction)
		if err != nil {
			return err
		}
		rowCol, ok := colMap.Get(keyCol.ColumnID)
		if !ok {
			panic("nulls not handled")
		}
		vec := vecs[rowCol]
		for row := 0; row < count; row++ {
			kys[row] = colencodingEncodeKey(kys[row], keyCol.Type, dir, vec, row)
		}
	}

	//	buffer := make([]byte, 0, count*rowBufSize)
	extraKeys := make([][]byte, count)
	// TODO: bitset?
	containsNull := make([]bool, count)
	//EncodeColumns
	columnIDs := ind.IndexDesc().KeySuffixColumnIDs
	for _, id := range columnIDs {
		i, ok := colMap.Get(id)
		if !ok {
			panic(false)
		}
		typ := fetchedCols[i].GetType()
		vec := vecs[i]
		for row := 0; row < count; row++ {
			// null checking!
			containsNull[row] = false
			extraKeys[row] = colencodingEncodeKey(extraKeys[row], typ, encoding.Ascending, vec, row)
		}
	}

	for rowoffset := 0; rowoffset < count; rowoffset++ {
		if !ind.IsUnique() || containsNull[rowoffset] {
			kys[rowoffset] = append(kys[rowoffset], extraKeys[rowoffset]...)
		}
		// TODO: secondary index family support
		kys[rowoffset] = keys.MakeFamilyKey(kys[rowoffset], 0)
	}

	//encodeSecondaryIndexNoFamilies
	var values [][]byte
	if ind.IsUnique() {
		values = extraKeys
	} else {
		values = make([][]byte, count)
	}
	for i := 0; i < ind.NumSecondaryStoredColumns(); i++ {
		//TODO
	}
	for i := 0; i < ind.NumCompositeColumns(); i++ {
		//TODO
	}
	//values[rowoffset] = writeColumnValues

	if srt {
		kvs := KVS{keys: kys, values: values}
		sort.Sort(&kvs)
		kys = kvs.keys
		values = kvs.values
	}

	if rh.TraceKV {
		var value roachpb.Value
		for rowoffset := 0; rowoffset < count; rowoffset++ {
			value.SetBytes(values[rowoffset])
			if ind.ForcePut() {
				log.VEventfDepth(ctx, 1, 2, "Put %s -> %s", kys[rowoffset], value.PrettyPrint())
				b.Put(&kys[rowoffset], &value)
			} else {
				log.VEventfDepth(ctx, 1, 2, "InitPut %s -> %s", kys[rowoffset], value.PrettyPrint())
				b.InitPut(&kys[rowoffset], &value, false)
			}
		}
	} else {
		if ind.ForcePut() {
			b.PutBytes(kys, values)
		} else {
			b.InitPutBytes(kys, values, false)
		}
	}

	return nil
}
