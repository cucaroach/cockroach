// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"hash"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
)

func init() {
	if exportOp != 56 {
		// If this error occurs make sure the new op is the last one in order to not
		// invalidate existing plan hashes. If we are just adding an operator at the
		// end there's no need to update version below.
		panic(errors.AssertionFailedf("Operator field changed (%d), please update and consider incrementing version", exportOp))
	}
}

// version tracks major changes to how we encode plans or to the operator set.
// It isn't necessary to increment it when adding a single operator but if we
// remove an operator or change the operator set or decide to use a more
// efficient encoding version should be incremented.
var version = 1

// PlanGist is a compact representation of a logical plan meant to be used as
// a key and log for different plans used to implement a particular query. A
// gist doesn't change for the following:
//
// - literal constant values
// - alias names
// - grouping column names
// - constraint values
// - estimated rows stats
//
// The notion is that the gist is the rough shape of the plan that represents
// the way the plan operators are put together and what tables and indexes they
// use.
type PlanGist struct {
	gist string
	hash uint64
}

// String returns the gist bytes as a base64 encoded string.
func (fp PlanGist) String() string {
	return fp.gist
}

// Hash returns a 64 bit hash of the gist. Note that this is symbolically stable
// across table/index ids, i.e. indexes from two different databases with
// different ids but the same name will have the same hash.
func (fp PlanGist) Hash() uint64 {
	return fp.hash
}

// PlanGistFactory is an exec.Factory that produces a gist by eaves
// dropping on the exec builder phase of compilation.
type PlanGistFactory struct {
	hash uint64

	wrappedFactory exec.Factory
	// buffer is used for reading and writing (i.e. decoding and encoding) but on
	// on the write path it is used via the writer field which is a multi-writer
	// writing to the buffer and to the hash. The exception is when we're dealing
	// with ids where we will write the id to the buffer and the "string" to the
	// hash.  This allows the hash to be id agnostic (ie hash's will be stable
	// across plans from different databases with different DDL history).
	buffer bytes.Buffer
	//writer io.Writer

	nodeStack []*Node
	catalog   cat.Catalog
}

var _ exec.Factory = &PlanGistFactory{}
var _ hash.Hash64 = &PlanGistFactory{}

func (f *PlanGistFactory) Write(bs []byte) (int, error) {
	hash := f.hash
	for _, c := range bs {
		hash *= 1099511628211
		hash ^= uint64(c)
	}
	f.hash = hash
	return len(bs), nil
}

func (f *PlanGistFactory) Sum64() uint64 {
	return f.hash
}

func (f *PlanGistFactory) Size() int { return 8 }

func (f *PlanGistFactory) Sum(in []byte) []byte {
	v := uint64(f.hash)
	return append(in, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (f *PlanGistFactory) BlockSize() int { return 1 }

func (f *PlanGistFactory) Reset() { f.hash = 14695981039346656037 }
func (f *PlanGistFactory) Init(wrappedFactory exec.Factory) {
	f.wrappedFactory = wrappedFactory
	f.hash = 14695981039346656037
	f.encodeInt(version)
}

// NewPlanGistFactory creates a new PlanGistFactory.
func NewPlanGistFactory(wrappedFactory exec.Factory) *PlanGistFactory {
	f := new(PlanGistFactory)
	f.Init(wrappedFactory)
	return f
}

// ConstructPlan delegates to the wrapped factory.
func (f *PlanGistFactory) ConstructPlan(
	root exec.Node,
	subqueries []exec.Subquery,
	cascades []exec.Cascade,
	checks []exec.Node,
	rootRowCount int64,
) (exec.Plan, error) {
	plan, err := f.wrappedFactory.ConstructPlan(root, subqueries, cascades, checks, rootRowCount)
	return plan, err
}

// PlanGist returns a pointer to a PlanGist.
func (f *PlanGistFactory) PlanGist() PlanGist {
	return PlanGist{gist: base64.StdEncoding.EncodeToString(f.buffer.Bytes()),
		hash: f.hash}
}

// DecodePlanGistToRows converts a gist to a logical plan and returns the rows.
func DecodePlanGistToRows(gist string, catalog cat.Catalog) ([]string, error) {
	flags := Flags{HideValues: true, Redact: RedactAll}
	ob := NewOutputBuilder(flags)
	explainPlan, err := DecodePlanGistToPlan(gist, catalog)
	if err != nil {
		return nil, err
	}
	err = Emit(explainPlan, ob, func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string { return "" })
	if err != nil {
		return nil, err
	}
	return ob.BuildStringRows(), nil
}

// DecodePlanGistToPlan constructs an explain.Node tree from a gist.
func DecodePlanGistToPlan(s string, cat cat.Catalog) (plan *Plan, retErr error) {
	f := NewPlanGistFactory(exec.StubFactory{})
	f.catalog = cat
	bytes, err := base64.StdEncoding.DecodeString(s)

	if err != nil {
		return nil, err
	}

	// Clear out buffer which will have version in it from NewPlanGistFactory.
	f.buffer.Reset()
	f.buffer.Write(bytes)
	plan = &Plan{}

	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate internal errors without having to add
			// error checks everywhere throughout the code. This is only possible
			// because the code does not update shared state and does not manipulate
			// locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				retErr = e
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
	}()

	ver := f.decodeInt()
	if ver != version {
		return nil, errors.Errorf("unsupported old plan gist version %d", ver)
	}

	for {
		op := f.decodeOp()
		if op == unknownOp {
			break
		}
		switch op {
		case errorIfRowsOp:
			plan.Checks = append(plan.Checks, f.popChild())
		}
	}

	plan.Root = f.popChild()

	for _, n := range f.nodeStack {
		subquery := exec.Subquery{
			Root: n,
		}
		plan.Subqueries = append(plan.Subqueries, subquery)
	}

	return plan, nil
}

func (f *PlanGistFactory) decodeOp() execOperator {
	val, err := f.buffer.ReadByte()
	if err != nil || val == 0 {
		return unknownOp
	}
	n, err := f.decodeOperatorBody(execOperator(val))
	if err != nil {
		panic(err)
	}
	f.nodeStack = append(f.nodeStack, n)

	return n.op
}

func (f *PlanGistFactory) popChild() *Node {
	l := len(f.nodeStack)
	n := f.nodeStack[l-1]
	f.nodeStack = f.nodeStack[:l-1]

	return n
}

func (f *PlanGistFactory) encodeOperator(op execOperator) {
	f.encodeByte(byte(op))
}

func (f *PlanGistFactory) encodeInt(i int) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], int64(i))
	_, err := f.buffer.Write(buf[:n])
	if err != nil {
		panic(err)
	}
	_, err = f.Write(buf[:n])
	if err != nil {
		panic(err)
	}
}

func (f *PlanGistFactory) decodeInt() int {
	val, err := binary.ReadVarint(&f.buffer)
	if err != nil {
		panic(err)
	}

	return int(val)
}

// encodeDataSource encodes tables and indexes and does a numeric id based
// encoding to the gist and a symbolic encoding to the hash.
func (f *PlanGistFactory) encodeDataSource(id cat.StableID, name tree.Name) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], int64(id))
	_, err := f.buffer.Write(buf[:n])
	if err != nil {
		panic(err)
	}
	_, err = f.Write([]byte(string(name)))
	if err != nil {
		panic(err)
	}
}

func (f *PlanGistFactory) encodeID(id cat.StableID) {
	f.encodeInt(int(id))
}

func (f *PlanGistFactory) decodeID() cat.StableID {
	return cat.StableID(f.decodeInt())
}

func (f *PlanGistFactory) decodeTable() cat.Table {
	id := f.decodeID()
	ds, _, err := f.catalog.ResolveDataSourceByID(context.TODO(), cat.Flags{}, id)
	if err != nil {
		return nil
	}

	return ds.(cat.Table)
}

func (f *PlanGistFactory) decodeIndex(tbl cat.Table) cat.Index {
	id := f.decodeID()
	if tbl == nil {
		return nil
	}
	for i, n := 0, tbl.IndexCount(); i < n; i++ {
		if tbl.Index(i).ID() == id {
			return tbl.Index(i)
		}
	}

	return nil
}

// TODO: implement this and figure out how to test...
func (f *PlanGistFactory) decodeSchema() cat.Schema {
	id := f.decodeID()
	_ = id
	return nil
}

func (f *PlanGistFactory) encodeNodeColumnOrdinals(vals []exec.NodeColumnOrdinal) {
	f.encodeInt(len(vals))
}

func (f *PlanGistFactory) decodeNodeColumnOrdinals() []exec.NodeColumnOrdinal {
	l := f.decodeInt()
	vals := make([]exec.NodeColumnOrdinal, l)
	return vals
}

func (f *PlanGistFactory) encodeResultColumns(vals colinfo.ResultColumns) {
	f.encodeInt(len(vals))
}

func (f *PlanGistFactory) decodeResultColumns() colinfo.ResultColumns {
	numCols := f.decodeInt()
	return make(colinfo.ResultColumns, numCols)
}

func (f *PlanGistFactory) encodeByte(b byte) {
	bs := []byte{b}
	_, err := f.buffer.Write(bs)
	if err != nil {
		panic(err)
	}
	_, err = f.Write(bs)
	if err != nil {
		panic(err)
	}
}

func (f *PlanGistFactory) decodeByte() byte {
	val, err := f.buffer.ReadByte()
	if err != nil {
		panic(err)
	}
	return val
}

func (f *PlanGistFactory) decodeJoinType() descpb.JoinType {
	val := f.decodeByte()
	return descpb.JoinType(val)
}

func (f *PlanGistFactory) encodeBool(b bool) {
	if b {
		f.encodeByte(1)
	} else {
		f.encodeByte(0)
	}
}

func (f *PlanGistFactory) decodeBool() bool {
	val := f.decodeByte()
	return val != 0
}

// TODO: enable this or remove it...
func (f *PlanGistFactory) encodeColumnOrdering(cols colinfo.ColumnOrdering) {
}

func (f *PlanGistFactory) decodeColumnOrdering() colinfo.ColumnOrdering {
	return nil
}

func (f *PlanGistFactory) encodeScanParams(params exec.ScanParams) {
	err := params.NeededCols.Encode(&f.buffer)
	if err != nil {
		panic(err)
	}

	if params.IndexConstraint != nil {
		f.encodeInt(params.IndexConstraint.Spans.Count())
	} else {
		f.encodeInt(0)
	}

	if params.InvertedConstraint != nil {
		f.encodeInt(params.InvertedConstraint.Len())
	} else {
		f.encodeInt(0)
	}

	f.encodeInt(int(params.HardLimit))
}

func (f *PlanGistFactory) decodeScanParams() exec.ScanParams {
	neededCols := util.FastIntSet{}
	err := neededCols.Decode(&f.buffer)
	if err != nil {
		panic(err)
	}

	var idxConstraint *constraint.Constraint
	l := f.decodeInt()
	if l > 0 {
		idxConstraint = new(constraint.Constraint)
		idxConstraint.Spans.Alloc(l)
		var sp constraint.Span
		idxConstraint.Spans.Append(&sp)
	}

	var invertedConstraint inverted.Spans
	l = f.decodeInt()
	if l > 0 {
		invertedConstraint = make([]inverted.Span, l)
	}

	hardLimit := f.decodeInt()

	return exec.ScanParams{NeededCols: neededCols, IndexConstraint: idxConstraint, InvertedConstraint: invertedConstraint, HardLimit: int64(hardLimit)}
}

func (f *PlanGistFactory) encodeRows(rows [][]tree.TypedExpr) {
	f.encodeInt(len(rows))
}

func (f *PlanGistFactory) decodeRows() [][]tree.TypedExpr {
	numRows := f.decodeInt()
	return make([][]tree.TypedExpr, numRows)
}
