// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colenc_test

import (
	"bytes"
	"context"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colenc"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/stretchr/testify/require"
)

// TestEncoderEquality tests that the vector encoder and the row based encoder
// produce the exact same KV batches. Check constraints and partial indexes
// are left to copy datadriven tests so we don't have to muck with generating
// predicate columns.
func TestEncoderEquality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db.Exec("CREATE TYPE c AS (a INT, b INT)")
	rng, _ := randutil.NewTestRand()
	sv := &s.ClusterSettings().SV
	for _, tc := range []struct {
		ddl    string
		datums tree.Datums
	}{
		{"i INT PRIMARY KEY, j JSON, k INT,INVERTED INDEX (k,j), INDEX(k,i), FAMILY(j)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json), randgen.RandDatumSimple(rng, types.Int)}},

		{"i INT PRIMARY KEY, j JSON, k INT,INVERTED INDEX (k,j), INDEX(k,i), FAMILY(j),FAMILY(k)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json), randgen.RandDatumSimple(rng, types.Int)}},

		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL, INDEX(c,b,a), FAMILY(a),  FAMILY(b), FAMILY(c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}},

		// Test some weird datums
		{"i INT PRIMARY KEY, bi BIT, n NAME, ine INET", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.VarBit), randgen.RandDatumSimple(rng, types.Name), randgen.RandDatumSimple(rng, types.INet)}},

		// I'm not doing this right, it crashes in the row encoder...
		//{"i INT PRIMARY KEY, c c", []tree.Datum{tree.NewDInt(1234), tree.NewDTupleWithLen(types.Int, 2)}},
		{"i INT PRIMARY KEY, j JSON, k INT,INVERTED INDEX (k,j)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json), randgen.RandDatumSimple(rng, types.Int)}},
		{"i INT PRIMARY KEY, j JSON, INVERTED INDEX (j)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json)}},
		// Key encoding of vector types, both directions
		{"b BOOL, PRIMARY KEY (b ASC)", []tree.Datum{tree.DBoolFalse}},
		{"b BOOL, PRIMARY KEY (b DESC)", []tree.Datum{tree.DBoolFalse}},
		{"i INT, PRIMARY KEY (i ASC)", []tree.Datum{tree.NewDInt(1234)}},
		{"i INT, PRIMARY KEY (i DESC)", []tree.Datum{tree.NewDInt(1234)}},
		{"d DATE, PRIMARY KEY (d ASC)", []tree.Datum{tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(314159))}},
		{"d DATE, PRIMARY KEY (d DESC)", []tree.Datum{tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(314159))}},
		{"f FLOAT8, PRIMARY KEY (f ASC)", []tree.Datum{tree.NewDFloat(1.234)}},
		{"f FLOAT8, PRIMARY KEY (f DESC)", []tree.Datum{tree.NewDFloat(1.234)}},
		{"d DECIMAL, PRIMARY KEY (d ASC)", []tree.Datum{&tree.DDecimal{Decimal: *apd.New(123, 2)}}},
		{"d DECIMAL, PRIMARY KEY (d DESC)", []tree.Datum{&tree.DDecimal{Decimal: *apd.New(123, 2)}}},

		{"b BYTES, PRIMARY KEY (b ASC)", []tree.Datum{randgen.RandDatum(rng, types.Bytes, false)}},
		{"b BYTES, PRIMARY KEY (b DESC)", []tree.Datum{randgen.RandDatum(rng, types.Bytes, false)}},

		{"i INT PRIMARY KEY, b BYTES", []tree.Datum{tree.NewDInt(1234), randgen.RandDatum(rng, types.Bytes, true)}},

		{"s STRING, PRIMARY KEY (s ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.String)}},
		{"s STRING, PRIMARY KEY (s DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.String)}},

		{"u UUID, PRIMARY KEY (u ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Uuid)}},
		{"u UUID, PRIMARY KEY (u DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Uuid)}},

		{"ts TIMESTAMP, PRIMARY KEY (ts ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Timestamp)}},
		{"ts TIMESTAMP, PRIMARY KEY (ts DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Timestamp)}},

		{"ts TIMESTAMPTZ, PRIMARY KEY (ts ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.TimestampTZ)}},
		{"ts TIMESTAMPTZ, PRIMARY KEY (ts DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.TimestampTZ)}},

		{"i INTERVAL, PRIMARY KEY (i ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Interval)}},
		{"i INTERVAL, PRIMARY KEY (i DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Interval)}},

		// decodeTableKeyToCol supports JSON as key but keyside.Encode sez its unimplemented???
		// TDB: how do I test encoded key?

		// Now do all valueside types
		{"i INT PRIMARY KEY, a BOOL, b INT, c DATE, d FLOAT8, e DECIMAL, f BYTES, g STRING, h UUID, t TIMESTAMP, j TIMESTAMPTZ, k INTERVAL, l JSON",
			[]tree.Datum{
				randgen.RandDatumSimple(rng, types.Int),
				randgen.RandDatumSimple(rng, types.Bool),
				randgen.RandDatumSimple(rng, types.Int),
				randgen.RandDatumSimple(rng, types.Date),
				randgen.RandDatumSimple(rng, types.Float),
				randgen.RandDatumSimple(rng, types.Decimal),
				randgen.RandDatumSimple(rng, types.Bytes),
				randgen.RandDatumSimple(rng, types.String),
				randgen.RandDatumSimple(rng, types.Uuid),
				randgen.RandDatumSimple(rng, types.Timestamp),
				randgen.RandDatumSimple(rng, types.TimestampTZ),
				randgen.RandDatumSimple(rng, types.Interval),
				randgen.RandDatumSimple(rng, types.Json),
			}},

		// Column families and null handling
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL, INDEX (a,b,c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}},
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL, FAMILY(a),  FAMILY(b), FAMILY(c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}},
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL, FAMILY(a,b), FAMILY(c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}},
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL, FAMILY(a,b,c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}},
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}},
		{"i INT PRIMARY KEY, a INT, b INT, c INT, d INT, e INT, f INT", []tree.Datum{tree.NewDInt(1234), tree.NewDInt(1), tree.NewDInt(0), tree.NewDInt(-1), tree.NewDInt(math.MaxInt64), tree.NewDInt(math.MinInt64), tree.DNull}},
		{"i INT PRIMARY KEY, a FLOAT8, b FLOAT8, c FLOAT8, d FLOAT8", []tree.Datum{tree.NewDInt(1234), tree.DNaNFloat, tree.NewDFloat(0.0), tree.NewDFloat(math.MaxFloat64), tree.DNull}},
		{"i INT PRIMARY KEY, d DATE, e DATE", []tree.Datum{tree.NewDInt(1234), tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(314159)), tree.DNull}},
		{"i INT PRIMARY KEY, d DECIMAL, e DECIMAL", []tree.Datum{tree.NewDInt(1234), &tree.DDecimal{Decimal: *apd.New(123, 2)}, tree.DNull}},
		{"i INT PRIMARY KEY, b BYTES", []tree.Datum{tree.NewDInt(1234), tree.DNull}},
		{"i INT PRIMARY KEY, b STRING", []tree.Datum{tree.NewDInt(1234), tree.DNull}},
		{"i INT PRIMARY KEY, b UUID", []tree.Datum{tree.NewDInt(1234), tree.DNull}},
		{"i INT PRIMARY KEY, b TIMESTAMP", []tree.Datum{tree.NewDInt(1234), tree.DNull}},
		{"i INT PRIMARY KEY, b TIMESTAMPTZ", []tree.Datum{tree.NewDInt(1234), tree.DNull}},
		{"i INT PRIMARY KEY, b INTERVAL", []tree.Datum{tree.NewDInt(1234), tree.DNull}},

		// TODO: composite types
		// TODO: ignore indexes
		// TODO: unique indexes
		// TODO: array types
		// TODO: tuples
		// TODO: tsvector
		// TODO: covering secondary indexes
	} {
		r := sqlutils.MakeSQLRunner(db)
		// Create table, insert primary key since we're not including special rows like rowid.
		r.Exec(t, "CREATE TABLE t ("+tc.ddl+")")
		desc := desctestutils.TestingGetTableDescriptor(
			kvdb, keys.SystemSQLCodec, "defaultdb", "public", "t")
		rowKVs, err := buildRowKVs(tc.ddl, tc.datums, desc, sv)
		require.NoError(t, err)
		sort.Sort(&rowKVs)
		vecKVs, err := buildVecKVs(tc.ddl, tc.datums, desc, sv)
		require.NoError(t, err)
		sort.Sort(&vecKVs)
		checkEqual(t, rowKVs, vecKVs)
		r.Exec(t, "DROP TABLE t")
	}
}

func checkEqual(t *testing.T, rowKVs, vecKVs colenc.KVS) {
	require.Equal(t, len(rowKVs.Keys), len(rowKVs.Values))
	if len(rowKVs.Keys) != len(vecKVs.Keys) {
		t.Errorf("row keys:\n%s\nvec keys:\n%s\n", printKeys(rowKVs.Keys), printKeys(vecKVs.Keys))
	}
	if len(rowKVs.Values) != len(vecKVs.Values) {
		t.Errorf("row keys:\n%srow values:\n%s\nvec keys:\n%s\nvec values:\n%s\n", printKeys(rowKVs.Keys), printVals(rowKVs.Values), printKeys(vecKVs.Keys), printVals(vecKVs.Values))
	}
	for i, k1 := range rowKVs.Keys {
		k2 := vecKVs.Keys[i]
		if !bytes.Equal(k1, k2) {
			t.Errorf("key[%d]:\n%s\n didn't equal: \n%s", i, catalogkeys.PrettyKey(nil, k1, 0), catalogkeys.PrettyKey(nil, k2, 0))
		}
		v1, v2 := rowKVs.Values[i], vecKVs.Values[i]
		if !bytes.Equal(v1, v2) {
			var val1, val2 roachpb.Value
			val1.RawBytes = v1
			val2.RawBytes = v2
			t.Errorf("value[%d]:\n%s\n didn't equal: \n%s", i, val1.PrettyPrint(), val2.PrettyPrint())
		}
	}
}

func buildRowKVs(ddl string, datums tree.Datums, desc catalog.TableDescriptor, sv *settings.Values) (colenc.KVS, error) {
	inserter, err := row.MakeInserter(context.Background(), nil /*txn*/, keys.SystemSQLCodec, desc, desc.PublicColumns(), nil, sv, false, nil)
	if err != nil {
		return colenc.KVS{}, err
	}
	p := &capturePutter{}
	var pm row.PartialIndexUpdateHelper
	if err := inserter.InsertRow(context.Background(), p, datums, pm, false, true); err != nil {
		return colenc.KVS{}, err
	}
	return p.kvs, nil
}

func buildVecKVs(ddl string, datums tree.Datums, desc catalog.TableDescriptor, sv *settings.Values) (colenc.KVS, error) {
	rh := row.NewRowHelper(keys.SystemSQLCodec, desc, desc.WritableNonPrimaryIndexes(), sv, false, nil)
	rh.Init()
	rh.TraceKV = true
	p := &capturePutter{}
	cols := desc.PublicColumns()
	colMap := row.ColIDtoRowIndexFromCols(cols)
	typs := make([]*types.T, len(cols))
	for i, c := range cols {
		typs[i] = c.GetType()
	}
	factory := coldataext.NewExtendedColumnFactory(nil /*evalCtx */)
	b := coldata.NewMemBatchWithCapacity(typs, 1, factory)
	for i, t := range typs {
		converter := colconv.GetDatumToPhysicalFn(t)
		if datums[i] != tree.DNull {
			coldata.SetValueAt(b.ColVec(i), converter(datums[i]), 0 /* rowIdx */)
		} else {
			b.ColVec(i).Nulls().SetNull(0)
		}
	}
	b.SetLength(1)

	be := colenc.MakeEncoder(&rh, b, p, colMap, nil)
	if err := be.PrepareBatch(context.Background()); err != nil {
		return colenc.KVS{}, err
	}
	return p.kvs, nil
}

// How do I test this case?
func TestColIDToRowIndexNull(t *testing.T) {

}

func printVals(vals [][]byte) string {
	var buf strings.Builder
	var v roachpb.Value
	for i, _ := range vals {
		v.RawBytes = vals[i]
		buf.WriteString(v.PrettyPrint())
		buf.WriteByte('\n')
	}
	return buf.String()
}

func printKeys(kys []roachpb.Key) string {
	var buf strings.Builder
	for _, k := range kys {
		buf.WriteString(k.String())
		buf.WriteByte('\n')
	}
	return buf.String()
}

// TODO: checkMutationInput
// TODO: tryDoResponseAdmission

type capturePutter struct {
	kvs     colenc.KVS
	traceKV bool
}

func (c *capturePutter) CPut(key, value interface{}, expValue []byte) {
	if expValue != nil {
		panic("expValue unexpected")
	}
	k := key.(*roachpb.Key)
	c.kvs.Keys = append(c.kvs.Keys, *k)
	v := value.(*roachpb.Value)
	c.kvs.Values = append(c.kvs.Values, v.RawBytes)
}

func (c *capturePutter) Put(key, value interface{}) {
	k := key.(*roachpb.Key)
	c.kvs.Keys = append(c.kvs.Keys, *k)
	v := value.(*roachpb.Value)
	c.kvs.Values = append(c.kvs.Values, v.RawBytes)
}

func (c *capturePutter) InitPut(key, value interface{}, failOnTombstones bool) {
	k := key.(*roachpb.Key)
	c.kvs.Keys = append(c.kvs.Keys, *k)
	v := value.(*roachpb.Value)
	c.kvs.Values = append(c.kvs.Values, v.RawBytes)
}

func (c *capturePutter) Del(key ...interface{}) {
	panic("delete not expected")
}

func (c *capturePutter) CPutTuples(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if k == nil {
			continue
		}
		c.kvs.Keys = append(c.kvs.Keys, k)
		var kvValue roachpb.Value
		kvValue.SetTuple(values[i])
		c.kvs.Values = append(c.kvs.Values, kvValue.RawBytes)
	}
}

func (c *capturePutter) CPutValues(kys []roachpb.Key, values []roachpb.Value) {
	for i, k := range kys {
		if k == nil {
			continue
		}
		c.kvs.Keys = append(c.kvs.Keys, k)
		c.kvs.Values = append(c.kvs.Values, values[i].RawBytes)
	}
}

// we don't call this
func (c *capturePutter) PutBytes(kys []roachpb.Key, values [][]byte) {
	panic("unimplemented")
}
func (c *capturePutter) InitPutBytes(kys []roachpb.Key, values [][]byte, failOnTombstones bool) {
	for i, k := range kys {
		if k == nil {
			continue
		}
		c.kvs.Keys = append(c.kvs.Keys, k)
		var kvValue roachpb.Value
		kvValue.SetBytes(values[i])
		c.kvs.Values = append(c.kvs.Values, kvValue.RawBytes)
	}
}

// we don't call this
func (c *capturePutter) PutTuples(kys []roachpb.Key, values [][]byte) {
	panic("unimplemented")
}
func (c *capturePutter) InitPutTuples(kys []roachpb.Key, values [][]byte, failOnTombstones bool) {
	for i, k := range kys {
		if k == nil {
			continue
		}
		c.kvs.Keys = append(c.kvs.Keys, k)
		var kvValue roachpb.Value
		kvValue.SetTuple(values[i])
		c.kvs.Values = append(c.kvs.Values, kvValue.RawBytes)
	}
}
