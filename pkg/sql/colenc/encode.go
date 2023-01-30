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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type BatchEncoder struct {
	rh                 *row.RowHelper
	b                  coldata.Batch
	p                  row.Putter
	colMap             catalog.TableColMap
	partialIndexes     map[descpb.IndexID]coldata.Bools
	familyToColumns    map[descpb.FamilyID][]rowenc.ValueEncodedColumn
	compositeColumnIDs intsets.Fast
}

func MakeEncoder(rh *row.RowHelper,
	b coldata.Batch,
	p row.Putter,
	colMap catalog.TableColMap,
	partialIndexes map[descpb.IndexID]coldata.Bools) BatchEncoder {
	return BatchEncoder{rh: rh, b: b, p: p, colMap: colMap, partialIndexes: partialIndexes}
}

func (b *BatchEncoder) PrepareBatch(ctx context.Context) error {
	err := b.EncodePK(ctx, b.rh.TableDesc.GetPrimaryIndex())
	if err != nil {
		return err
	}
	for _, ind := range b.rh.TableDesc.WritableNonPrimaryIndexes() {
		err = b.EncodeSecondaryIndex(ctx, ind, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *BatchEncoder) EncodePK(ctx context.Context, ind catalog.Index) error {
	desc := b.rh.TableDesc
	count := b.b.Length()
	vecs := b.b.ColVecs()
	keyAndSuffixCols := desc.IndexFetchSpecKeyAndSuffixColumns(ind)
	keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]

	families := desc.GetFamilies()
	// TODO: what are key length limits, could this be int16? Is int32 okay?
	var pkoffsets []int32

	// Store the index up to the family id so we can reuse the prefixes
	if len(families) > 1 {
		pkoffsets = make([]int32, count)
	}
	// Partial index support, we will use this to leave some keys nil.
	var piPreds coldata.Bools
	if b.partialIndexes != nil {
		piPreds = b.partialIndexes[ind.GetID()]
	}
	// Initialize count buffers sliced from one big buffer
	kys := make([]roachpb.Key, count)
	rowBufSize := 5*len(keyCols) + len(b.rh.PrimaryIndexKeyPrefix)
	buffer := make([]byte, 0, count*rowBufSize)
	for row, counter := 0, 0; row < count; row++ {
		if piPreds != nil && !piPreds.Get(row) {
			continue
		}
		offset := counter * rowBufSize
		kys[row] = buffer[offset : offset : rowBufSize+offset]
		kys[row] = append(kys[row], b.rh.PrimaryIndexKeyPrefix...)
		counter++
	}

	var nulls coldata.Nulls
	if err := b.encodeIndexKey(kys, keyCols, pkoffsets, nulls); err != nil {
		return err
	}
	if nulls.MaybeHasNulls() {
		return rowenc.MakeNullPKError(desc, ind, b.colMap, nil)
	}

	for i := range families {
		family := &families[i]
		update := false
		for _, colID := range family.ColumnIDs {
			if _, ok := b.colMap.Get(colID); ok {
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
		familySortedColumnIDs, ok := b.rh.SortedColumnFamily(family.ID)
		if !ok {
			return errors.AssertionFailedf("invalid family sorted column id map")
		}
		var lastColIDs []catid.ColumnID

		// reset keys for new family
		if i > 0 {
			buffer := make([]byte, 0, count*rowBufSize)
			newkys := make([]roachpb.Key, count)
			for row, counter := 0, 0; row < count; row++ {
				// Elided partial index keys will be nil.
				if kys[row] == nil {
					continue
				}
				offset := counter * rowBufSize
				newkys[row] = buffer[offset : offset : rowBufSize+offset]
				newkys[row] = append(newkys[row], kys[row][:pkoffsets[row]]...)
				counter++
			}
			kys = newkys
		}

		for row := 0; row < count; row++ {
			// Elided partial index keys will be nil.
			if kys[row] == nil {
				continue
			}
			kys[row] = keys.MakeFamilyKey(kys[row], uint32(family.ID))
		}

		fetchedCols := desc.PublicColumns()

		// We need to ensure that column family 0 contains extra metadata, like composite primary key values.
		// Additionally, the decoders expect that column family 0 is encoded with a TUPLE value tag, so we
		// don't want to use the untagged value encoding.
		if len(family.ColumnIDs) == 1 && family.ColumnIDs[0] == family.DefaultColumnID && family.ID != 0 {
			// Storage optimization to store DefaultColumnID directly as a value. Also
			// backwards compatible with the original BaseFormatVersion.

			// TODO(cucaroach): valColIDMapping vs. updatedColIDMapping????
			idx, ok := b.colMap.Get(family.DefaultColumnID)
			if !ok {
				continue
			}

			values := make([]roachpb.Value, count)

			typ := fetchedCols[idx].GetType()
			vec := vecs[idx]
			for row := 0; row < count; row++ {
				if piPreds != nil && !piPreds[row] {
					continue
				}
				marshaled, err := MarshalLegacy(typ, vec, row)
				if err != nil {
					return err
				}

				if marshaled.RawBytes == nil {
					// Tell CPutValues to ignore this one.
					kys[row] = nil
					// TODO(cucaroach): update support
					// if overwrite {
					// 	// If the new family contains a NULL value, then we must
					// 	// delete any pre-existing row.
					// 	//insertDelFn(ctx, batch, kvKey, traceKV)
					// }
				} else {
					// We only output non-NULL values. Non-existent column keys are
					// considered NULL during scanning and the row sentinel ensures we know
					// the row exists.
					if err := b.rh.CheckRowSize(ctx, &kys[row], &marshaled, family.ID); err != nil {
						return err
					}
					values[row] = marshaled
				}
			}
			b.p.CPutValues(kys, values)
			continue
		}

		values := make([][]byte, count)
		if lastColIDs == nil {
			lastColIDs = make([]catid.ColumnID, count)
		}

		for _, colID := range familySortedColumnIDs {
			idx, ok := b.colMap.Get(colID)
			if !ok {
				// Column not being updated or inserted.
				continue
			}

			col := fetchedCols[idx]
			typ := col.GetType()
			vec := vecs[idx]
			for row := 0; row < count; row++ {
				if piPreds != nil && !piPreds[row] {
					continue
				}
				if vec.Nulls().NullAt(row) {
					if !col.IsNullable() {
						return sqlerrors.NewNonNullViolationError(col.GetName())
					}
					continue
				}

				// Reuse this function but fake out the value and handle composites ourselves.
				// TODO(cucaroach): this is yucky
				if skip := b.rh.SkipColumnNotInPrimaryIndexValue(colID, tree.DNull); skip {
					if !b.compositeColumnIDs.Contains(int(colID)) {
						continue
					}
					switch vec.CanonicalTypeFamily() {
					case types.FloatFamily:
						f := tree.DFloat(vec.Float64()[row])
						if !f.IsComposite() {
							continue
						}
					case types.DecimalFamily:
						d := tree.DDecimal{vec.Decimal()[row]}
						if !d.IsComposite() {
							continue
						}
					default:
						d := vec.Datum().Get(row)
						if cdatum, ok := d.(tree.CompositeDatum); ok {
							// Composite columns are encoded in both the key and the value.
							if !cdatum.IsComposite() {
								continue
							}
						}
					}
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

		//TODO(cucuroach): for updates overwrite makes this a plain put
		b.p.CPutTuples(kys, values)
	}

	return nil
}

func (b *BatchEncoder) EncodeSecondaryIndex(ctx context.Context, ind catalog.Index, srt bool) error {
	var err error
	secondaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(b.rh.Codec, b.rh.TableDesc.GetID(), ind.GetID())

	// Use the primary key encoding for covering indexes.
	if ind.GetEncodingType() == catenumpb.PrimaryIndexEncoding {
		return b.EncodePK(ctx, ind)
	}
	var piPreds coldata.Bools
	if b.partialIndexes != nil {
		piPreds = b.partialIndexes[ind.GetID()]
	}

	keyAndSuffixCols := b.rh.TableDesc.IndexFetchSpecKeyAndSuffixColumns(ind)
	keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]
	count := b.b.Length()
	// TODO: we should re-use these across indexes
	kys := make([]roachpb.Key, count)
	rowBufSize := 5*len(keyCols) + len(secondaryIndexKeyPrefix)
	buffer := make([]byte, 0, count*rowBufSize)

	for rowoffset, counter := 0, 0; rowoffset < count; rowoffset++ {
		if piPreds != nil && !piPreds.Get(rowoffset) {
			continue
		}
		offset := counter * rowBufSize
		kys[rowoffset] = buffer[offset : offset : rowBufSize+offset]
		kys[rowoffset] = append(kys[rowoffset], secondaryIndexKeyPrefix...)
		counter++
	}

	extraKeys, _, err := encodeColumns[[]byte](ind.IndexDesc().KeySuffixColumnIDs, nil /*directions*/, b.colMap, count, b.b.ColVecs(), nil /*keyPrefixs*/)
	if err != nil {
		return err
	}

	var nulls coldata.Nulls
	if ind.GetType() == descpb.IndexDescriptor_INVERTED {
		// Since the inverted indexes generate multiple keys per row just handle them
		// separately.
		return b.encodeInvertedSecondaryIndex(ind, kys, extraKeys)
	} else {
		if err := b.encodeIndexKey(kys, keyCols, nil, nulls); err != nil {
			return err
		}
	}
	noFamilies := b.rh.TableDesc.NumFamilies() == 1 || ind.GetVersion() == descpb.BaseIndexFormatVersion
	// Save length of base of key for reuse across families
	var keyPrefixOffsets []int32
	if !noFamilies {
		keyPrefixOffsets = make([]int32, count)
	}
	for row := 0; row < count; row++ {
		// Elided partial index keys will be nil.
		if kys[row] == nil {
			continue
		}
		if !ind.IsUnique() || nulls.NullAtChecked(row) {
			kys[row] = append(kys[row], extraKeys[row]...)
		}
		if !noFamilies {
			keyPrefixOffsets[row] = int32(len(kys[row]))
		}
	}

	if noFamilies {
		if err := b.encodeSecondaryIndexNoFamilies(ind, kys, extraKeys); err != nil {
			return err
		}
	} else {
		familyToColumns := rowenc.MakeFamilyToColumnMap(ind, b.rh.TableDesc)
		if err := b.encodeSecondaryIndexWithFamilies(familyToColumns, ind, kys, extraKeys, keyPrefixOffsets, rowBufSize); err != nil {
			return err
		}
	}
	return nil
}

func (b *BatchEncoder) encodeSecondaryIndexNoFamilies(ind catalog.Index, kys []roachpb.Key, extraKeyCols [][]byte) error {
	var values [][]byte
	var err error
	for row := 0; row < b.b.Length(); row++ {
		kys[row] = keys.MakeFamilyKey(kys[row], 0)
	}
	if ind.IsUnique() {
		values = extraKeyCols
	} else {
		// TODO: make preallocated slices out of big slice
		values = make([][]byte, b.b.Length())
	}
	cols := rowenc.GetValueColumns(ind)
	values, err = b.writeColumnValues(kys, values, ind, cols)
	if err != nil {
		return err
	}
	if ind.ForcePut() {
		b.p.PutBytes(kys, values)
	} else {
		b.p.InitPutBytes(kys, values, false)
	}
	return nil
}

func (b *BatchEncoder) encodeSecondaryIndexWithFamilies(familyMap map[catid.FamilyID][]rowenc.ValueEncodedColumn,
	ind catalog.Index, kys []roachpb.Key, extraKeyCols [][]byte,
	keyPrefixOffsets []int32, rowBufSize int) error {
	count := len(kys)
	var values [][]byte
	// TODO (rohany): is there a natural way of caching this information as well?
	// We have to iterate over the map in sorted family order. Other parts of the code
	// depend on a per-call consistent order of keys generated.
	familyIDs := make([]int, 0, len(familyMap))
	for familyID := range familyMap {
		familyIDs = append(familyIDs, int(familyID))
	}
	sort.Ints(familyIDs)
	for i, familyID := range familyIDs {
		storedColsInFam := familyMap[descpb.FamilyID(familyID)]

		// If we aren't storing any columns in this family and we are not the first family,
		// skip onto the next family. We need to write family 0 no matter what to ensure
		// that each row has at least one entry in the DB.
		if len(storedColsInFam) == 0 && familyID != 0 {
			continue
		}
		sort.Sort(rowenc.ByID(storedColsInFam))

		// reset keys for new family
		if i > 0 {
			buffer := make([]byte, 0, count*rowBufSize)
			newkys := make([]roachpb.Key, count)
			for row, counter := 0, 0; row < count; row++ {
				// Elided partial index keys will be nil.
				if kys[row] == nil {
					continue
				}
				offset := counter * rowBufSize
				newkys[row] = buffer[offset : offset : rowBufSize+offset]
				newkys[row] = append(newkys[row], kys[row][:keyPrefixOffsets[row]]...)
				counter++
			}
			kys = newkys
		}
		for row := 0; row < len(kys); row++ {
			kys[row] = keys.MakeFamilyKey(kys[row], uint32(familyID))
		}
		if ind.IsUnique() {
			values = extraKeyCols
		} else {
			// TODO: make preallocated slices out of big slice
			values = make([][]byte, b.b.Length())
		}
		var err error
		values, err = b.writeColumnValues(kys, values, ind, storedColsInFam)
		if err != nil {
			return err
		}
		for row := 0; row < len(kys); row++ {
			if familyID != 0 && len(values[row]) == 0 /* && !includeEmpty */ {
				kys[row] = nil
				continue
			}
		}
		if familyID == 0 {
			if ind.ForcePut() {
				b.p.PutBytes(kys, values)
			} else {
				b.p.InitPutBytes(kys, values, false)
			}
		} else {
			if ind.ForcePut() {
				b.p.PutTuples(kys, values)
			} else {
				b.p.InitPutTuples(kys, values, false /*failOnTombstones*/)
			}
		}
	}

	return nil
}

func (b *BatchEncoder) writeColumnValues(kys []roachpb.Key, values [][]byte, ind catalog.Index,
	cols []rowenc.ValueEncodedColumn) ([][]byte, error) {
	var err error
	count := b.b.Length()
	lastColIDs := make([]catid.ColumnID, count)

	for _, col := range cols {
		idx, ok := b.colMap.Get(col.Id)
		if !ok {
			// Column not being updated or inserted.
			continue
		}
		vec := b.b.ColVec(idx)
		for row := 0; row < count; row++ {
			if vec.Nulls().NullAt(row) || kys[row] == nil {
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

// encodeColumns is the vector version of rowenc.EncodeColumns. It is generic
// so we can use it on raw byte slices and roachpb.Key.
func encodeColumns[T []byte | roachpb.Key](
	columnIDs []descpb.ColumnID,
	directions []catenumpb.IndexColumn_Direction,
	colMap catalog.TableColMap,
	count int,
	vecs []coldata.Vec,
	keyPrefixes []T) ([]T, *coldata.Nulls, error) {
	keys := make([]T, count)
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

		// FIXME!
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
		return valueside.Encode(appendTo, colID, vec.Datum().Get(row).(tree.Datum), nil)
	}
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
