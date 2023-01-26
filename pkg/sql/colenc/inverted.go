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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func invertedColToDatum(vec coldata.Vec, row int) tree.Datum {
	switch vec.Type().Family() {
	case types.JsonFamily:
		return tree.NewDJSON(vec.JSON().Get(row))
	case types.StringFamily:
		b := vec.Bytes().Get(row)
		s := *(*string)(unsafe.Pointer(&b))
		return tree.NewDString(s)
	}
	// This handles arrays
	return vec.Datum().Get(row).(tree.Datum)
}

func (b *BatchEncoder) encodeInvertedSecondaryIndex(index catalog.Index, kys []roachpb.Key,
	extraKeys [][]byte) error {
	var err error
	if kys, err = encodeInvertedIndexPrefixKeys(kys, index, b.b.ColVecs(), b.colMap); err != nil {
		return err
	}
	var vec coldata.Vec
	if i, ok := b.colMap.Get(index.InvertedColumnID()); ok {
		vec = b.b.ColVecs()[i]
	}
	partialIndexPreds := b.partialIndexes[index.GetID()]
	for row := 0; row < b.b.Length(); row++ {
		if partialIndexPreds != nil && !partialIndexPreds.Get(row) {
			continue
		}
		var keys [][]byte
		val := invertedColToDatum(vec, row)
		indexGeoConfig := index.GetGeoConfig()
		if !indexGeoConfig.IsEmpty() {
			if keys, err = rowenc.EncodeGeoInvertedIndexTableKeys(val, kys[row], indexGeoConfig); err != nil {
				return err
			}
		} else {
			if keys, err = rowenc.EncodeInvertedIndexTableKeys(val, kys[row], index.GetVersion()); err != nil {
				return err
			}
		}
		for _, key := range keys {
			if !index.IsUnique() {
				key = append(key, extraKeys[row]...)
			}
			if err = b.encodeSecondaryIndexNoFamiliesOneRow(index, key, extraKeys[row], row); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BatchEncoder) encodeSecondaryIndexNoFamiliesOneRow(ind catalog.Index, key roachpb.Key, extraKeyCols []byte, row int) error {
	var value []byte
	var err error
	// If we aren't encoding index keys with families, all index keys use the sentinel family 0.
	key = keys.MakeFamilyKey(key, 0)
	if ind.IsUnique() {
		value = extraKeyCols
	} else {
		// TODO: make preallocated slices out of big slice
		value = nil
	}
	cols := rowenc.GetValueColumns(ind)
	value, err = writeColumnValueOneRow(value, b.colMap, b.b.ColVecs(), cols, row)
	if err != nil {
		return err
	}
	var kvValue roachpb.Value
	kvValue.SetBytes(value)
	if ind.ForcePut() {
		b.p.Put(&key, &kvValue)
	} else {
		b.p.InitPut(&key, &kvValue, false)
	}
	return nil
}

func writeColumnValueOneRow(value []byte, colMap catalog.TableColMap, vecs []coldata.Vec,
	cols []rowenc.ValueEncodedColumn, row int) ([]byte, error) {
	var err error
	var lastColID catid.ColumnID

	for _, col := range cols {
		idx, ok := colMap.Get(col.Id)
		if !ok {
			// Column not being updated or inserted.
			continue
		}
		vec := vecs[idx]
		if vec.Nulls().NullAt(row) {
			continue
		}
		if lastColID > col.Id {
			return nil, errors.AssertionFailedf("cannot write column id %d after %d", col.Id, lastColID)
		}
		colIDDelta := valueside.MakeColumnIDDelta(lastColID, col.Id)
		lastColID = col.Id
		value, err = valuesideEncodeCol(value, vec.Type(), colIDDelta, vec, row)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

func encodeInvertedIndexPrefixKeys(kys []roachpb.Key, index catalog.Index, vecs []coldata.Vec, colMap catalog.TableColMap) ([]roachpb.Key, error) {
	numColumns := index.NumKeyColumns()
	var err error
	// If the index is a multi-column inverted index, we encode the non-inverted
	// columns in the key prefix.
	if numColumns > 1 {
		// Do not encode the last column, which is the inverted column, here. It
		// is encoded below this block.
		colIDs := index.IndexDesc().KeyColumnIDs[:numColumns-1]
		dirs := index.IndexDesc().KeyColumnDirections

		kys, _, err = encodeColumnsKeys(colIDs, dirs, colMap, vecs, kys)
		if err != nil {
			return nil, err
		}
	}
	return kys, nil
}
