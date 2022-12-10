// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package copy_test

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/copy/vec"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
)

const lineitemSchema string = `CREATE TABLE lineitem (
	l_orderkey      INT8 NOT NULL,
	l_partkey       INT8 NOT NULL,
	l_suppkey       INT8 NOT NULL,
	l_linenumber    INT8 NOT NULL,
	l_quantity      DECIMAL(15,2) NOT NULL,
	l_extendedprice DECIMAL(15,2) NOT NULL,
	l_discount      DECIMAL(15,2) NOT NULL,
	l_tax           DECIMAL(15,2) NOT NULL,
	l_returnflag    CHAR(1) NOT NULL,
	l_linestatus    CHAR(1) NOT NULL,
	l_shipdate      DATE NOT NULL,
	l_commitdate    DATE NOT NULL,
	l_receiptdate   DATE NOT NULL,
	l_shipinstruct  CHAR(25) NOT NULL,
	l_shipmode      CHAR(10) NOT NULL,
	l_comment       VARCHAR(44) NOT NULL,
	PRIMARY KEY     (l_orderkey, l_linenumber),
	INDEX l_ok      (l_orderkey ASC),
	INDEX l_pk      (l_partkey ASC),
	INDEX l_sk      (l_suppkey ASC),
	INDEX l_sd      (l_shipdate ASC),
	INDEX l_cd      (l_commitdate ASC),
	INDEX l_rd      (l_receiptdate ASC),
	INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
	INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
)`

const csvData = `%d|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the`

func TestCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	url, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	testCopy := func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "exec-ddl":
			err := conn.Exec(ctx, d.Input)
			if err != nil {
				require.NoError(t, err, "%s: %s", d.Pos, d.Cmd)
			}
			return ""
		case "copy", "copy-error":
			lines := strings.Split(d.Input, "\n")
			stmt := lines[0]
			data := strings.Join(lines[1:], "\n")
			rows, err := conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(data), stmt)
			if d.Cmd == "copy" {
				require.NoError(t, err, "%s\n%s\n", d.Cmd, d.Input)
				require.Equal(t, int(rows), len(lines)-1, "Not all rows were inserted")
			} else {
				return err.Error()
			}
			return fmt.Sprintf("%d", rows)
		case "query":
			rows, err := conn.Query(ctx, d.Input)
			require.NoError(t, err)

			vals := make([]driver.Value, len(rows.Columns()))
			var results string
			for {
				if err := rows.Next(vals); err == io.EOF {
					break
				} else if err != nil {
					require.NoError(t, err)
				}
				for i, v := range vals {
					if i > 0 {
						results += "|"
					}
					results += fmt.Sprintf("%v", v)
				}
				results += "\n"
			}
			err = rows.Close()
			require.NoError(t, err)
			return results
		default:
			return fmt.Sprintf("unknown command: %s\n", d.Cmd)
		}

	}
	datadriven.RunTest(t, testutils.TestDataPath(t, "copyfrom"), testCopy)
}

// TestCopyFromTransaction tests that copy from rows are written with
// same transaction timestamp when done under an explicit transaction,
// copy rows are same transaction when done with default settings and
// batches are in separate transactions when non atomic mode is enabled.
func TestCopyFromTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	url, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context

	decEq := func(v1, v2 driver.Value) bool {
		valToDecimal := func(v driver.Value) *apd.Decimal {
			mt, ok := v.(pgtype.Numeric)
			require.True(t, ok)
			buf, err := mt.EncodeText(nil, nil)
			require.NoError(t, err)
			decimal, _, err := apd.NewFromString(string(buf))
			require.NoError(t, err)
			return decimal
		}
		return valToDecimal(v1).Cmp(valToDecimal(v2)) == 0
	}

	testCases := []struct {
		name   string
		query  string
		data   []string
		testf  func(clisqlclient.Conn, func(clisqlclient.Conn))
		result func(f1, f2 driver.Value) bool
	}{
		{
			"explicit_copy",
			"COPY lineitem FROM STDIN WITH CSV DELIMITER '|';",
			[]string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)},
			func(tconn clisqlclient.Conn, f func(tconn clisqlclient.Conn)) {
				err := tconn.Exec(ctx, "BEGIN")
				require.NoError(t, err)
				f(tconn)
				err = tconn.Exec(ctx, "COMMIT")
				require.NoError(t, err)
			},
			decEq,
		},
		{
			"implicit_atomic",
			"COPY lineitem FROM STDIN WITH CSV DELIMITER '|';",
			[]string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)},
			func(tconn clisqlclient.Conn, f func(tconn clisqlclient.Conn)) {
				err := tconn.Exec(ctx, "SET copy_from_atomic_enabled = true")
				require.NoError(t, err)
				orig := sql.SetCopyFromBatchSize(1)
				defer sql.SetCopyFromBatchSize(orig)
				f(tconn)
			},
			decEq,
		},
		{
			"implicit_non_atomic",
			"COPY lineitem FROM STDIN WITH CSV DELIMITER '|';",
			[]string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)},
			func(tconn clisqlclient.Conn, f func(tconn clisqlclient.Conn)) {
				err := tconn.Exec(ctx, "SET copy_from_atomic_enabled = false")
				require.NoError(t, err)
				orig := sql.SetCopyFromBatchSize(1)
				defer sql.SetCopyFromBatchSize(orig)
				f(tconn)
			},
			func(f1, f2 driver.Value) bool { return !decEq(f1, f2) },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tconn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())
			tc.testf(tconn, func(tconn clisqlclient.Conn) {
				// Without this everything comes back as strings
				tconn.SetAlwaysInferResultTypes(true)
				// Put each test in its own db so they can be parallelized.
				err := tconn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s; USE %s", tc.name, tc.name))
				require.NoError(t, err)
				err = tconn.Exec(ctx, lineitemSchema)
				require.NoError(t, err)
				numrows, err := tconn.GetDriverConn().CopyFrom(ctx, strings.NewReader(strings.Join(tc.data, "\n")), tc.query)
				require.NoError(t, err)
				require.Equal(t, len(tc.data), int(numrows))

				result, err := tconn.QueryRow(ctx, "SELECT l_partkey FROM lineitem WHERE l_orderkey = 1")
				require.NoError(t, err)
				partKey, ok := result[0].(int64)
				require.True(t, ok)
				require.Equal(t, int64(155190), partKey)

				results, err := tconn.Query(ctx, "SELECT crdb_internal_mvcc_timestamp FROM lineitem")
				require.NoError(t, err)
				var lastts driver.Value
				firstTime := true
				vals := make([]driver.Value, 1)
				for {
					err = results.Next(vals)
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					if !firstTime {
						require.True(t, tc.result(lastts, vals[0]))
					} else {
						firstTime = false
					}
					lastts = vals[0]
				}
			})
			err := tconn.Exec(ctx, "TRUNCATE TABLE lineitem")
			require.NoError(t, err)
		})
	}
}

// BenchmarkCopyFrom measures copy performance against a TestServer.
func BenchmarkCopyFrom(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	url, cleanup := sqlutils.PGUrl(b, s.ServingSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	err := conn.Exec(ctx, lineitemSchema)
	require.NoError(b, err)

	// send data in 5 batches of 10k rows
	const ROWS = sql.CopyBatchRowSizeDefault * 4
	datalen := 0
	var rows []string
	for i := 0; i < ROWS; i++ {
		row := fmt.Sprintf(csvData, i)
		rows = append(rows, row)
		datalen += len(row)
	}
	rowsize := datalen / ROWS
	for _, batchSizeFactor := range []float64{.5, 1, 2, 4} {
		batchSize := int(batchSizeFactor * sql.CopyBatchRowSizeDefault)
		b.Run(fmt.Sprintf("%d", batchSize), func(b *testing.B) {
			actualRows := rows[:batchSize]
			for i := 0; i < b.N; i++ {
				pprof.Do(ctx, pprof.Labels("run", "copy"), func(ctx context.Context) {
					numrows, err := conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(strings.Join(actualRows, "\n")), "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';")
					require.NoError(b, err)
					require.Equal(b, int(numrows), len(actualRows))
				})
				b.StopTimer()
				err = conn.Exec(ctx, "TRUNCATE TABLE lineitem")
				require.NoError(b, err)
				b.StartTimer()
			}
			b.SetBytes(int64(len(actualRows) * rowsize))
		})
	}
}

const lineitemURL string = `https://storage.googleapis.com/cockroach-fixtures/tpch-csv/sf-1/lineitem.tbl.1`

// There's an extra dummy field because the file above ends lines with delimiter and standard CSV behavior is to
// interpret that as a column.
var lineitemURLSchema string = `
CREATE TABLE  lineitem (
	l_orderkey      INT8 NOT NULL,
	l_partkey       INT8 NOT NULL,
	l_suppkey       INT8 NOT NULL,
	l_linenumber    INT8 NOT NULL,
	l_quantity      DECIMAL(15,2) NOT NULL,
	l_extendedprice DECIMAL(15,2) NOT NULL,
	l_discount      DECIMAL(15,2) NOT NULL,
	l_tax           DECIMAL(15,2) NOT NULL,
	l_returnflag    CHAR(1) NOT NULL,
	l_linestatus    CHAR(1) NOT NULL,
	l_shipdate      DATE NOT NULL,
	l_commitdate    DATE NOT NULL,
	l_receiptdate   DATE NOT NULL,
	l_shipinstruct  CHAR(25) NOT NULL,
	l_shipmode      CHAR(10) NOT NULL,
	l_comment       VARCHAR(44) NOT NULL,
	l_dummy         CHAR(1),
	PRIMARY KEY     (l_orderkey, l_linenumber));
	`

var lineitemIndexes string = `
CREATE INDEX  l_ok ON lineitem    (l_orderkey);
CREATE INDEX  l_pk ON lineitem    (l_partkey);
CREATE INDEX  l_sk ON lineitem    (l_suppkey);
CREATE INDEX  l_sd ON lineitem    (l_shipdate);
CREATE INDEX  l_cd ON lineitem    (l_commitdate);
CREATE INDEX  l_rd ON lineitem    (l_receiptdate);
CREATE INDEX  l_pk_sk ON lineitem (l_partkey, l_suppkey);
CREATE INDEX  l_sk_pk ON lineitem (l_suppkey, l_partkey);
`

var lineitemSplits string = `
ALTER INDEX l_ok SPLIT AT VALUES (0);
ALTER INDEX l_pk SPLIT AT VALUES (0);
ALTER INDEX l_sk SPLIT AT VALUES (0);
ALTER INDEX l_sd SPLIT AT VALUES (0::DATE);
ALTER INDEX l_cd SPLIT AT VALUES (0::DATE);
ALTER INDEX l_rd SPLIT AT VALUES (0::DATE);
ALTER INDEX l_pk_sk SPLIT AT VALUES (0,0);
ALTER INDEX l_sk_pk SPLIT AT VALUES (0,0);
`

// BenchmarkCopyFrom measures copy performance against a TestServer.
func BenchmarkCopyFromSF1(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	path, err := filepath.Abs("lineitem.tbl.1")
	require.NoError(b, err)

	data, err := os.ReadFile(path)
	require.NoError(b, err)

	s, _, _ := serverutils.StartServer(b, tsa)
	defer s.Stopper().Stop(ctx)

	url, cleanup := sqlutils.PGUrl(b, s.ServingSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	err = conn.Exec(ctx, lineitemURLSchema)
	require.NoError(b, err)

	err = conn.Exec(ctx, lineitemIndexes)
	require.NoError(b, err)

	// err = conn.Exec(ctx, lineitemSplits)
	// require.NoError(b, err)

	b.Run("copy", func(b *testing.B) {
		numrows, err := conn.GetDriverConn().CopyFrom(ctx, bytes.NewReader(data), "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';")
		require.NoError(b, err)
		fmt.Printf("copied %d rows", numrows)
		b.SetBytes(int64(len(data)))
	})
}

func BenchmarkCopyFromTPCHSF1_1ParseCSV(b *testing.B) {
	path, err := filepath.Abs("lineitem.tbl.1")
	require.NoError(b, err)

	data, err := os.ReadFile(path)
	require.NoError(b, err)
	reader := bytes.NewReader(data)
	csvReader := csv.NewReader(reader)
	csvReader.ReuseRecord = true
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = 17
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := 0
		for {
			_, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				require.NoError(b, err)
			}
			rows++
		}
		reader.Reset(data)
		require.Equal(b, 750594, rows)
	}
	b.SetBytes(int64(len(data)))
}

var typs = []*types.T{
	types.Int,
	types.Int,
	types.Int,
	types.Int,
	types.Decimal,
	types.Decimal,
	types.Decimal,
	types.Decimal,
	types.String,
	types.String,
	types.Date,
	types.Date,
	types.Date,
	types.String,
	types.String,
	types.String,
	types.String,
}

func BenchmarkCopyFromTPCHSF1_1ParseDatum(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	path, err := filepath.Abs("lineitem.tbl.1")
	require.NoError(b, err)

	data, err := os.ReadFile(path)
	require.NoError(b, err)
	pctx := tree.NewParseTimeContext(timeutil.Now())

	csvReader := csv.NewReader(bytes.NewReader(data))
	csvReader.ReuseRecord = true
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = len(typs)
	records, err := csvReader.ReadAll()
	require.NoError(b, err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, r := range records {
			for col, f := range r {
				_, _, err := tree.ParseAndRequireString(typs[col], f.String(), pctx)
				require.NoError(b, err)
			}
		}
	}
	b.SetBytes(int64(len(data)))
}

func BenchmarkCopyFromTPCHSF1_1ParseCol(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	path, err := filepath.Abs("lineitem.tbl.1")
	require.NoError(b, err)

	data, err := os.ReadFile(path)
	require.NoError(b, err)
	pctx := tree.NewParseTimeContext(timeutil.Now())

	records, err := readAll(data)
	require.NoError(b, err)

	colBatchSize := 1024

	var vecHandlers = make([]tree.ValueHandler, len(typs))
	for i, t := range typs {
		vecHandlers[i] = vec.MakeHandler(t, colBatchSize)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row, r := range records {
			for col, f := range r {
				err := tree.ParseAndRequireStringEx(typs[col], f.String(), pctx, vecHandlers[col], &pgdate.ParseHelper{})
				if err != nil {
					require.NoError(b, err)
				}
			}
			// The Datum version throws away each Datum, to make it fair, throw
			// away after 1024 rows which is default batch size.
			if (row+1)%colBatchSize == 0 {
				for _, v := range vecHandlers {
					v.Reset()
				}
			}
		}
	}
	b.SetBytes(int64(len(data)))
}

func BenchmarkCopyFromTPCHSF1_1ParseColEx(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	path, err := filepath.Abs("lineitem.tbl.1")
	require.NoError(b, err)

	data, err := os.ReadFile(path)
	require.NoError(b, err)
	pctx := tree.NewParseTimeContext(timeutil.Now())

	records, err := readAll(data)
	require.NoError(b, err)

	colBatchSize := 1024

	var vecHandlers = make([]tree.ValueHandler, len(typs))
	for i, t := range typs {
		vecHandlers[i] = vec.MakeHandler(t, colBatchSize)
	}
	var ph pgdate.ParseHelper

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row, r := range records {
			for col, f := range r {
				err := tree.ParseAndRequireStringEx(typs[col], f.String(), pctx, vecHandlers[col], &ph)
				if err != nil {
					require.NoError(b, err)
				}
			}
			// The Datum version throws away each Datum, to make it fair, throw
			// away after 1024 rows which is default batch size.
			if (row+1)%colBatchSize == 0 {
				for _, v := range vecHandlers {
					v.Reset()
				}
			}
		}
	}
	b.SetBytes(int64(len(data)))
}

func readAll(data []byte) ([][]csv.Record, error) {
	csvReader := csv.NewReader(bytes.NewReader(data))
	csvReader.ReuseRecord = true
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = len(typs)
	return csvReader.ReadAll()
}

// same as above but with coldata.Batch instead of raw slices, loose >10% to
// interface overhead
func BenchmarkCopyFromTPCHSF1_1ParseBatch(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	path, err := filepath.Abs("lineitem.tbl.1")
	require.NoError(b, err)

	data, err := os.ReadFile(path)
	require.NoError(b, err)
	pctx := tree.NewParseTimeContext(timeutil.Now())

	records, err := readAll(data)
	require.NoError(b, err)

	batch := coldata.NewMemBatchWithCapacity(typs, 1024, coldata.StandardColumnFactory)
	var vecHandlers = make([]tree.ValueHandler, len(typs))
	for i := range typs {
		vecHandlers[i] = vec.MakeBatchHandler(batch, i)
	}

	var ph pgdate.ParseHelper
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for row, r := range records {
			// The Datum version throws away each Datum, to make it fair, throw
			// away after 1024 rows which is the batch size.
			if (row+1)%1024 == 0 {
				for _, v := range vecHandlers {
					v.Reset()
				}
			}
			for col, f := range r {
				err := tree.ParseAndRequireStringEx(typs[col], f.String(), pctx, vecHandlers[col], &ph)
				if err != nil {
					require.NoError(b, err)
				}
			}
		}
	}
	b.SetBytes(int64(len(data)))
}

func makeTableDescriptor() (catalog.TableDescriptor, error) {
	ctx := context.Background()
	tableID := descpb.ID(bootstrap.TestingUserDescID(0))
	semaCtx := tree.MakeSemaContext()
	stmt, err := parser.ParseOne(lineitemURLSchema)
	if err != nil {
		return nil, err
	}
	createTable, ok := stmt.AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.Errorf("expected *tree.CreateTable got %T", stmt)
	}
	// We need to assign a valid parent database ID to the table descriptor, but
	// the value itself doesn't matter, so we arbitrarily pick the system database
	// ID because we know it's valid.
	parentID := descpb.ID(keys.SystemDatabaseID)
	testSettings := cluster.MakeTestingClusterSettings()
	tableDesc, err := importer.MakeTestingSimpleTableDescriptor(
		ctx, &semaCtx, testSettings, createTable, parentID, keys.PublicSchemaID, tableID, importer.NoFKs, time.Now().UnixNano())
	if err != nil {
		return nil, err
	}
	return tableDesc.ImmutableCopy().(catalog.TableDescriptor), nil
}

type noopPutter struct {
}

func (n noopPutter) CPut(key, value interface{}, expValue []byte)          {}
func (n noopPutter) Put(key, value interface{})                            {}
func (n noopPutter) InitPut(key, value interface{}, failOnTombstones bool) {}
func (n noopPutter) Del(key ...interface{})                                {}

var st = cluster.MakeTestingClusterSettings()
var tsa = base.TestServerArgs{
	Settings:  st,
	CacheSize: 2 << 30,
}
var pctx = tree.NewParseTimeContext(timeutil.Now())
var pm row.PartialIndexUpdateHelper

func recordsToRC(records [][]csv.Record) (*rowcontainer.RowContainer, error) {
	ctx := context.Background()
	m := mon.NewUnlimitedMonitor(ctx, "test", mon.MemoryResource, nil, nil, math.MaxInt64, st)
	rc := rowcontainer.NewRowContainer(m.MakeBoundAccount(), colinfo.ColTypeInfoFromColTypes(typs))
	datums := make(tree.Datums, len(typs))
	for _, r := range records {
		for col, f := range r {
			var err error
			datums[col], _, err = tree.ParseAndRequireString(typs[col], f.String(), pctx)
			if err != nil {
				return nil, err
			}
		}
		rc.AddRow(ctx, datums)
	}
	return rc, nil
}

func BenchmarkCopyFromTPCHSF1_1KVBuildFromDatum(b *testing.B) {
	ctx := context.Background()
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	s, db, kvdb := serverutils.StartServer(b, tsa)
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec(lineitemURLSchema)
	require.NoError(b, err)
	_, err = db.Exec(lineitemIndexes)
	require.NoError(b, err)

	var datalen int
	var rc *rowcontainer.RowContainer
	{
		path, err := filepath.Abs("lineitem.tbl.1")
		require.NoError(b, err)
		data, err := os.ReadFile(path)
		require.NoError(b, err)
		datalen = len(data)
		records, err := readAll(data)
		require.NoError(b, err)
		rc, err = recordsToRC(records)
		require.NoError(b, err)
	}
	tblDesc := desctestutils.TestingGetTableDescriptor(
		kvdb, keys.SystemSQLCodec, "defaultdb", "public", "lineitem")
	require.NoError(b, err)
	txn := kvdb.NewTxn(ctx, "copybench")
	const batchSize = 100
	batches := make([]*kv.Batch, 0, rc.Len()/batchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inserter, err := row.MakeInserter(ctx, txn, keys.SystemSQLCodec, tblDesc, tblDesc.PublicColumns(), nil, &st.SV, false, nil)
		require.NoError(b, err)
		batch := txn.NewBatch()
		for r := 0; r < rc.Len(); r++ {
			inserter.InsertRow(ctx, batch, rc.At(r), pm, false, false)
			if (r+1)%batchSize == 0 {
				batches = append(batches, batch)
				batch = txn.NewBatch()
			}
		}
	}
	b.SetBytes(int64(datalen))
}

var codec = keys.SystemSQLCodec

func valuesideEncodeCol(appendTo []byte, typ *types.T, colID valueside.ColumnIDDelta, vec tree.ValueHandler, row int) ([]byte, error) {
	switch typ.Family() {
	case types.BoolFamily:
		bs := vec.Bools()
		return encoding.EncodeBoolValue(appendTo, uint32(colID), bs[row]), nil
	case types.IntFamily, types.DateFamily:
		is := vec.Ints()
		return encoding.EncodeIntValue(appendTo, uint32(colID), is[row]), nil
	case types.FloatFamily:
		fs := vec.Floats()
		return encoding.EncodeFloatValue(appendTo, uint32(colID), fs[row]), nil
	case types.DecimalFamily:
		ds := vec.Decimals()
		return encoding.EncodeDecimalValue(appendTo, uint32(colID), &ds[row]), nil
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		ss := vec.Strings()
		b := encoding.UnsafeConvertStringToBytes(string(ss[row]))
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

func colencodingEncodeKey(b []byte, typ *types.T, dir encoding.Direction, vec tree.ValueHandler, row int) []byte {
	// keyside.Encode(key, val, dir); err != nil {
	// cases taken from decodeTableKeyToCol
	switch typ.Family() {
	case types.BoolFamily:
		bs := vec.Bools()
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
		is := vec.Ints()
		i := is[row]
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, i)
		}
		return encoding.EncodeVarintDescending(b, i)
	case types.FloatFamily:
		fs := vec.Floats()
		f := fs[row]
		if dir == encoding.Ascending {
			return encoding.EncodeFloatAscending(b, f)
		}
		return encoding.EncodeFloatDescending(b, f)
	case types.DecimalFamily:
		ds := vec.Decimals()
		d := &ds[row]
		if dir == encoding.Ascending {
			return encoding.EncodeDecimalAscending(b, d)
		}
		return encoding.EncodeDecimalDescending(b, d)
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		ss := vec.Strings()
		s := ss[row]
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, s)
		}
		return encoding.EncodeStringDescending(b, s)
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

func encodePK(ctx context.Context, rh *row.RowHelper, batch *kv.Batch, desc catalog.TableDescriptor,
	ind catalog.Index, vecs []tree.ValueHandler,
	colIDToRowIndex catalog.TableColMap, from, count int) error {

	// EncodeIndexKey
	keyAndSuffixCols := desc.IndexFetchSpecKeyAndSuffixColumns(ind)
	keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]
	fetchedCols := desc.PublicColumns()

	kys := make([]roachpb.Key, count)
	rowBufSize := 5*len(keyCols) + len(rh.PrimaryIndexKeyPrefix)
	buffer := make([]byte, 0, count*rowBufSize)

	for rowoffset := 0; rowoffset < count; rowoffset++ {
		offset := rowoffset * rowBufSize
		kys[rowoffset] = buffer[offset : offset : rowBufSize+offset]
		kys[rowoffset] = append(kys[rowoffset], rh.PrimaryIndexKeyPrefix...)
	}

	//func EncodePartialIndexKey(
	for i := range keyCols {
		keyCol := &keyCols[i]
		dir, err := catalogkeys.IndexColumnEncodingDirection(keyCol.Direction)
		if err != nil {
			return err
		}
		rowCol, ok := colIDToRowIndex.Get(keyCol.ColumnID)
		if !ok {
			panic("nulls not handled")
		}
		vec := vecs[rowCol]
		for rowoffset := 0; rowoffset < count; rowoffset++ {
			row := from + rowoffset
			kys[rowoffset] = colencodingEncodeKey(kys[rowoffset], keyCol.Type, dir, vec, row)
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
			for rowoffset := 0; rowoffset < count; rowoffset++ {
				row := from + rowoffset
				if lastColIDs[rowoffset] > colID {
					return errors.AssertionFailedf("cannot write column id %d after %d", colID, lastColIDs[rowoffset])
				}
				colIDDelta := valueside.MakeColumnIDDelta(lastColIDs[rowoffset], colID)
				lastColIDs[rowoffset] = colID
				// check col type and vec type
				var err error
				values[rowoffset], err = valuesideEncodeCol(values[rowoffset], typ, colIDDelta, vec, row)
				if err != nil {
					return err
				}
			}
		}

		if traceKV {
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

var traceKV = false

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

func encodeSecondaryIndex(ctx context.Context, rh *row.RowHelper, b *kv.Batch,
	desc catalog.TableDescriptor, ind catalog.Index,
	vecs []tree.ValueHandler, colMap catalog.TableColMap, from int, count int, srt bool) error {
	fetchedCols := desc.PublicColumns()
	//entries, err := rowenc.EncodeSecondaryIndex(rh.Codec, rh.TableDesc, index, colIDtoRowIndex, values, includeEmpty)
	secondaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(codec, desc.GetID(), ind.GetID())
	_ = secondaryIndexKeyPrefix

	// Use the primary key encoding for covering indexes.
	if ind.GetEncodingType() == descpb.PrimaryIndexEncoding {
		return encodePK(ctx, rh, b, desc, ind, vecs, colMap, from, count)
	}

	keyAndSuffixCols := desc.IndexFetchSpecKeyAndSuffixColumns(ind)
	keyCols := keyAndSuffixCols[:ind.NumKeyColumns()]
	// TODO: we should re-use these
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
		for rowoffset := 0; rowoffset < count; rowoffset++ {
			row := from + rowoffset
			kys[rowoffset] = colencodingEncodeKey(kys[rowoffset], keyCol.Type, dir, vec, row)
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
		for rowoffset := 0; rowoffset < count; rowoffset++ {
			row := from + rowoffset
			// null checking!
			containsNull[rowoffset] = false
			extraKeys[rowoffset] = colencodingEncodeKey(extraKeys[rowoffset], typ, encoding.Ascending, vec, row)
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

	if traceKV {
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

// Build kv batches where we have a fixed number of rows and do PK and each index.
func buildKVBatchesFromVecs(ctx context.Context, batchSize int, txn *kv.Txn, numRows int, rh *row.RowHelper, colMap catalog.TableColMap,
	vecHandlers []tree.ValueHandler, sort bool) ([]*kv.Batch, error) {
	var batches []*kv.Batch
	for i := 0; i < numRows; i += batchSize {
		batch := txn.NewBatch()
		batches = append(batches, batch)
		thisBatchSize := batchSize
		if i+batchSize > numRows {
			thisBatchSize = numRows - i
		}
		err := encodePK(ctx, rh, batch, rh.TableDesc, rh.TableDesc.GetPrimaryIndex(), vecHandlers, colMap, i, thisBatchSize)
		if err != nil {
			return nil, err
		}
		for _, ind := range rh.TableDesc.WritableNonPrimaryIndexes() {
			err = encodeSecondaryIndex(ctx, rh, batch, rh.TableDesc, ind, vecHandlers, colMap, i, thisBatchSize, sort)
			if err != nil {
				return nil, err
			}
		}
	}
	return batches, nil
}

func BenchmarkCopyFromTPCHSF1_1KVBuildFromCol(b *testing.B) {
	ctx := context.Background()
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	s, db, kvdb := serverutils.StartServer(b, tsa)
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec(lineitemURLSchema)
	require.NoError(b, err)
	_, err = db.Exec(lineitemIndexes)
	require.NoError(b, err)

	var datalen int
	path, err := filepath.Abs("lineitem.tbl.1")
	require.NoError(b, err)
	data, err := os.ReadFile(path)
	require.NoError(b, err)
	datalen = len(data)
	records, err := readAll(data)
	require.NoError(b, err)

	var vecHandlers = make([]tree.ValueHandler, len(typs))
	for i, t := range typs {
		vecHandlers[i] = vec.MakeHandler(t, len(records))
	}
	var ph pgdate.ParseHelper
	for _, r := range records {
		for col, f := range r {
			err := tree.ParseAndRequireStringEx(typs[col], f.String(), pctx, vecHandlers[col], &ph)
			if err != nil {
				require.NoError(b, err)
			}
		}
	}
	desc := desctestutils.TestingGetTableDescriptor(kvdb, codec, "defaultdb", "public", "lineitem")
	require.NoError(b, err)
	txn := kvdb.NewTxn(ctx, "copybench")
	colIDToRowIndex := row.ColIDtoRowIndexFromCols(desc.PublicColumns())
	rh := row.NewRowHelper(codec, desc, desc.WritableNonPrimaryIndexes(), &st.SV, false, nil)
	rh.PrimaryIndexKeyPrefix = rowenc.MakeIndexKeyPrefix(rh.Codec, rh.TableDesc.GetID(), rh.TableDesc.GetPrimaryIndexID())
	b.ResetTimer()
	_, err = buildKVBatchesFromVecs(ctx, 1<<10, txn, len(records), &rh, colIDToRowIndex, vecHandlers, false)
	require.NoError(b, err)
	b.SetBytes(int64(datalen))
}

func BenchmarkCopyFromTPCHSF1_1InsertFromDatum(b *testing.B) {
	ctx := context.Background()
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	//	log.SetVModule("inserter=2,writer=2")
	tempDir, cleanup := testutils.TempDir(b)
	defer cleanup()
	tsa.StoreSpecs = []base.StoreSpec{{Path: tempDir}}
	s, db, kvdb := serverutils.StartServer(b, tsa)
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec(lineitemURLSchema)
	require.NoError(b, err)
	_, err = db.Exec(lineitemIndexes)
	require.NoError(b, err)

	var datalen int
	var rc *rowcontainer.RowContainer
	{
		path, err := filepath.Abs("lineitem.tbl.1")
		require.NoError(b, err)
		data, err := os.ReadFile(path)
		require.NoError(b, err)
		datalen = len(data)
		records, err := readAll(data)
		require.NoError(b, err)
		rc, err = recordsToRC(records)
		require.NoError(b, err)
	}
	tblDesc := desctestutils.TestingGetTableDescriptor(
		kvdb, keys.SystemSQLCodec, "defaultdb", "public", "lineitem")
	require.NoError(b, err)
	txn := kvdb.NewTxn(ctx, "copybench")
	const batchSize = 100
	batches := make([]*kv.Batch, 0, rc.Len()/batchSize)
	{
		inserter, err := row.MakeInserter(ctx, txn, keys.SystemSQLCodec, tblDesc, tblDesc.PublicColumns(), nil, &st.SV, false, nil)
		require.NoError(b, err)
		batch := txn.NewBatch()
		for i := 0; i < rc.Len(); i++ {
			inserter.InsertRow(ctx, batch, rc.At(i), pm, false, false)
			if (i+1)%batchSize == 0 {
				batches = append(batches, batch)
				batch = txn.NewBatch()
			}
		}
	}

	if err := doBatches(ctx, b, txn, batches, datalen); err != nil {
		require.NoError(b, err)
	}
}

func insertFromCol(b *testing.B, batchSize int, sorted bool) {
	ctx := context.Background()
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	tempDir, cleanup := testutils.TempDir(b)
	defer cleanup()
	tsa.StoreSpecs = []base.StoreSpec{{Path: tempDir}}
	s, db, kvdb := serverutils.StartServer(b, tsa)
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec(lineitemURLSchema)
	require.NoError(b, err)
	_, err = db.Exec(lineitemIndexes)
	require.NoError(b, err)

	// TODO: explore the relationship between kv batch size and
	// this setting and rope in KV.
	// kvcoord.TrackedWritesMaxSize.Override(ctx, &tsa.SV, 1<<20)
	// kvserver.MaxCommandSize.Override(ctx, &tsa.SV, 512<<20)

	var datalen int
	path, err := filepath.Abs("lineitem.tbl.1")
	require.NoError(b, err)
	data, err := os.ReadFile(path)
	require.NoError(b, err)
	datalen = len(data)
	records, err := readAll(data)
	require.NoError(b, err)

	var vecHandlers = make([]tree.ValueHandler, len(typs))
	for i, t := range typs {
		vecHandlers[i] = vec.MakeHandler(t, len(records))
	}
	var ph pgdate.ParseHelper

	for _, r := range records {
		for col, f := range r {
			err := tree.ParseAndRequireStringEx(typs[col], f.String(), pctx, vecHandlers[col], &ph)
			if err != nil {
				require.NoError(b, err)
			}
		}
	}
	desc := desctestutils.TestingGetTableDescriptor(kvdb, codec, "defaultdb", "public", "lineitem")
	require.NoError(b, err)
	txn := kvdb.NewTxn(ctx, "copybench")
	colIDToRowIndex := row.ColIDtoRowIndexFromCols(desc.PublicColumns())
	var metrics *rowinfra.Metrics
	rh := row.NewRowHelper(codec, desc, desc.WritableNonPrimaryIndexes(), &st.SV, false, metrics)
	rh.PrimaryIndexKeyPrefix = rowenc.MakeIndexKeyPrefix(rh.Codec, rh.TableDesc.GetID(), rh.TableDesc.GetPrimaryIndexID())
	batches, err := buildKVBatchesFromVecs(ctx, batchSize, txn, len(records), &rh, colIDToRowIndex, vecHandlers, sorted)
	require.NoError(b, err)

	if err := doBatches(ctx, b, txn, batches, datalen); err != nil {
		require.NoError(b, err)
	}
}

func BenchmarkCopyFromTPCHSF1_1InsertFromCol(b *testing.B) {
	insertFromCol(b, 1<<10, false)
}
func BenchmarkCopyFromTPCHSF1_1InsertFromColSorted(b *testing.B) {
	insertFromCol(b, 1<<10, true)
}

func BenchmarkCopyFromTPCHSF1_1InsertFromColSorted4k(b *testing.B) {
	insertFromCol(b, 1<<12, true)
}
func BenchmarkCopyFromTPCHSF1_1InsertFromColSorted16k(b *testing.B) {
	insertFromCol(b, 1<<14, true)
}

func BenchmarkCopyFromTPCHSF1_1InsertFromColSorted64k(b *testing.B) {
	insertFromCol(b, 1<<16, true)
}

// Build kv batches where we do all PK first and then move to each index and cut off each batch at command limit.
func buildFullKVBatchesFromVecs(ctx context.Context, batchSize, limit int, txn *kv.Txn, numRows int, rh *row.RowHelper, colMap catalog.TableColMap,
	vecHandlers []tree.ValueHandler, sort bool) ([]*kv.Batch, error) {
	var batches []*kv.Batch
	batch := txn.NewBatch()
	batches = append(batches, batch)
	for i := 0; i < numRows; i += batchSize {
		thisBatchSize := batchSize
		if i+batchSize > numRows {
			thisBatchSize = numRows - i
		}
		if err := encodePK(ctx, rh, batch, rh.TableDesc, rh.TableDesc.GetPrimaryIndex(), vecHandlers, colMap, i, thisBatchSize); err != nil {
			return nil, err
		}
		if bytes := batch.ApproximateMutationBytes(); bytes > limit {
			batch = txn.NewBatch()
			batches = append(batches, batch)
		}
	}

	for i := 0; i < numRows; i += batchSize {
		thisBatchSize := batchSize
		if i+batchSize > numRows {
			thisBatchSize = numRows - i
		}
		for _, ind := range rh.TableDesc.WritableNonPrimaryIndexes() {
			if err := encodeSecondaryIndex(ctx, rh, batch, rh.TableDesc, ind, vecHandlers, colMap, i, thisBatchSize, sort); err != nil {
				return nil, err
			}
			if bytes := batch.ApproximateMutationBytes(); bytes > limit {
				batch = txn.NewBatch()
				batches = append(batches, batch)
			}
		}
	}

	return batches, nil
}

func BenchmarkCopyFromTPCHSF1_1InsertFromColFullCommands(b *testing.B) {
	ctx := context.Background()
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	tempDir, cleanup := testutils.TempDir(b)
	defer cleanup()
	tsa.StoreSpecs = []base.StoreSpec{{Path: tempDir}}
	s, db, kvdb := serverutils.StartServer(b, tsa)
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec(lineitemURLSchema)
	require.NoError(b, err)
	_, err = db.Exec(lineitemIndexes)
	require.NoError(b, err)

	// TODO: explore the relationship between kv batch size and
	// this setting and rope in KV.
	// kvcoord.TrackedWritesMaxSize.Override(ctx, &tsa.SV, 1<<20)
	// kvserver.MaxCommandSize.Override(ctx, &tsa.SV, 512<<20)

	var datalen int
	path, err := filepath.Abs("lineitem.tbl.1")
	require.NoError(b, err)
	data, err := os.ReadFile(path)
	require.NoError(b, err)
	datalen = len(data)
	records, err := readAll(data)
	require.NoError(b, err)

	var vecHandlers = make([]tree.ValueHandler, len(typs))
	for i, t := range typs {
		vecHandlers[i] = vec.MakeHandler(t, len(records))
	}
	var ph pgdate.ParseHelper

	for _, r := range records {
		for col, f := range r {
			err := tree.ParseAndRequireStringEx(typs[col], f.String(), pctx, vecHandlers[col], &ph)
			if err != nil {
				require.NoError(b, err)
			}
		}
	}
	desc := desctestutils.TestingGetTableDescriptor(kvdb, codec, "defaultdb", "public", "lineitem")
	require.NoError(b, err)
	txn := kvdb.NewTxn(ctx, "copybench")
	colIDToRowIndex := row.ColIDtoRowIndexFromCols(desc.PublicColumns())
	var metrics *rowinfra.Metrics
	rh := row.NewRowHelper(codec, desc, desc.WritableNonPrimaryIndexes(), &st.SV, false, metrics)
	rh.PrimaryIndexKeyPrefix = rowenc.MakeIndexKeyPrefix(rh.Codec, rh.TableDesc.GetID(), rh.TableDesc.GetPrimaryIndexID())
	batches, err := buildFullKVBatchesFromVecs(ctx, 10<<10, 21*(1<<20), txn, len(records), &rh, colIDToRowIndex, vecHandlers, true)
	require.NoError(b, err)

	if err := doBatches(ctx, b, txn, batches, datalen); err != nil {
		require.NoError(b, err)
	}
}

func doBatches(ctx context.Context, b *testing.B, txn *kv.Txn, batches []*kv.Batch, datalen int) error {
	b.ResetTimer()
	for _, batch := range batches {
		start := timeutil.Now()
		bytes := batch.ApproximateMutationBytes()
		if err := txn.Run(ctx, batch); err != nil {
			return err
		}
		fmt.Printf("txn.Run batch of %d bytes at %s\n", bytes, humanizeutil.DataRate(int64(bytes), time.Since(start)))
	}
	start := timeutil.Now()
	if err := txn.Commit(ctx); err != nil {
		return err
	}
	fmt.Printf("txn.Commit in %s\n", humanizeutil.Duration(time.Since(start)))
	b.SetBytes(int64(datalen))
	return nil
}
