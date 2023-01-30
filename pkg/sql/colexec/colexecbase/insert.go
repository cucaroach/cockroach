// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecbase

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colenc"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

type vectorInserter struct {
	colexecop.OneInputHelper
	helper                row.RowHelper
	insertCols            []catalog.Column
	insertColIDtoRowIndex catalog.TableColMap
	checkOrds             intsets.Fast
	retBatch              coldata.Batch
	flowCtx               *execinfra.FlowCtx
	semaCtx               *tree.SemaContext
}

var _ colexecop.Operator = &vectorInserter{}

func NewInsertOp(ctx context.Context, flowCtx *execinfra.FlowCtx, spec *execinfrapb.InsertSpec,
	input colexecop.Operator, typs []*types.T, outputIdx int,
) colexecop.Operator {
	desc := flowCtx.TableDescriptor(ctx, &spec.Table)
	insCols := make([]catalog.Column, len(spec.ColumnIDs))
	for i, c := range spec.ColumnIDs {
		col, err := catalog.MustFindColumnByID(desc, c)
		if err != nil {
			panic(err)
		}
		insCols[i] = col
	}
	v := vectorInserter{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		// TODO: figure out how to plumb row metrics which is used to report large rows
		helper:                row.NewRowHelper(flowCtx.Codec(), desc, desc.WritableNonPrimaryIndexes(), &flowCtx.Cfg.Settings.SV, false, nil /*metrics*/),
		retBatch:              coldata.NewMemBatchWithCapacity(typs, 1, coldata.StandardColumnFactory),
		flowCtx:               flowCtx,
		insertCols:            insCols,
		insertColIDtoRowIndex: row.ColIDtoRowIndexFromCols(insCols),
	}
	if spec.CheckOrds != nil {
		v.checkOrds.Decode(bytes.NewReader(spec.CheckOrds))
		v.semaCtx = flowCtx.NewSemaContext(v.flowCtx.Txn)
	}
	v.helper.TraceKV = flowCtx.TraceKV
	return &v
}

func (v *vectorInserter) Init(ctx context.Context) {
	v.OneInputHelper.Init(ctx)
	v.helper.Init()
}

func (v *vectorInserter) GetPartialIndexMap(b coldata.Batch) map[catid.IndexID]coldata.Bools {
	var partialIndexColMap map[descpb.IndexID]coldata.Bools
	// Create a set of partial index IDs to not write to. Indexes should not be
	// written to when they are partial indexes and the row does not satisfy the
	// predicate. This set is passed as a parameter to tableInserter.row below.
	pindexes := v.helper.TableDesc.PartialIndexes()
	if n := len(pindexes); n > 0 {
		colOffset := len(v.insertCols) + v.checkOrds.Len()
		numCols := len(b.ColVecs()) - colOffset
		if numCols != len(pindexes) {
			panic(errors.AssertionFailedf("num extra columns didn't match number of partial indexes"))
		}
		for i := 0; i < numCols; i++ {
			if partialIndexColMap == nil {
				partialIndexColMap = make(map[descpb.IndexID]coldata.Bools)
			}
			partialIndexColMap[pindexes[i].GetID()] = b.ColVec(i + colOffset).Bool()
		}
	}
	return partialIndexColMap
}

func (v *vectorInserter) Next() coldata.Batch {
	ctx := v.Ctx
	b := v.Input.Next()
	if b.Length() == 0 {
		return coldata.ZeroBatch
	}

	if !v.checkOrds.Empty() {
		if err := v.checkMutationInput(ctx, b); err != nil {
			panic(err)
		}
	}
	partialIndexColMap := v.GetPartialIndexMap(b)

	kvb := v.flowCtx.Txn.NewBatch()
	var p row.Putter = kvb
	if v.flowCtx.TraceKV {
		p = &tracePutter{p: kvb, ctx: ctx}
	}
	// FIXME:  allow InsertBatch to partially insert stuff and loop here til everything is done,
	// if there are a ton of secondary indexes we could hit workmem limit building kv batch so we
	// need to be able to do it in chunks of rows.
	enc := colenc.MakeEncoder(&v.helper, b, p, v.insertColIDtoRowIndex, partialIndexColMap)
	if err := enc.PrepareBatch(ctx); err != nil {
		panic(err)
	}
	// FIXME: if we are autocommitting this should be CommitInBatch
	if err := v.flowCtx.Txn.Run(ctx, kvb); err != nil {
		panic(row.ConvertBatchError(ctx, v.helper.TableDesc, kvb))
	}
	v.retBatch.ColVec(0).Int64().Set(0, int64(b.Length()))
	v.retBatch.SetLength(1)

	// FIXME: Possibly initiate a run of CREATE STATISTICS.
	// params.ExecCfg().StatsRefresher.NotifyMutation(n.run.ti.ri.Helper.TableDesc, len(n.input))

	return v.retBatch
}

func (v *vectorInserter) checkMutationInput(ctx context.Context, b coldata.Batch) error {
	checks := v.helper.TableDesc.EnforcedCheckConstraints()
	colIdx := 0
	for i, ch := range checks {
		if !v.checkOrds.Contains(i) {
			continue
		}
		vec := b.ColVec(colIdx + len(v.insertCols))
		for r := 0; r < b.Length(); r++ {
			if !vec.Bool()[r] && !vec.Nulls().NullAt(r) {
				return row.CheckFailed(ctx, v.semaCtx, v.flowCtx.EvalCtx.SessionData(), v.helper.TableDesc, ch)
			}
		}
		colIdx++
	}
	return nil
}
