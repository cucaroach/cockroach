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
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/colenc"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type vectorInserter struct {
	colexecop.OneInputHelper
	helper                row.RowHelper
	insertCols            []catalog.Column
	insertColIDtoRowIndex catalog.TableColMap
	retBatch              coldata.Batch
	txn                   *kv.Txn
}

var _ colexecop.Operator = &vectorInserter{}

func NewInsertOp(ctx context.Context, flowCtx *execinfra.FlowCtx, spec *execinfrapb.InsertSpec,
	input colexecop.Operator, typs []*types.T, outputIdx int,
) colexecop.Operator {
	desc := flowCtx.TableDescriptor(ctx, &spec.Table)
	insCols := make([]catalog.Column, len(spec.ColumnIDs))
	for i, c := range spec.ColumnIDs {
		col, err := desc.FindColumnWithID(c)
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
		txn:                   flowCtx.Txn,
		insertCols:            insCols,
		insertColIDtoRowIndex: row.ColIDtoRowIndexFromCols(insCols),
	}
	v.helper.TraceKV = flowCtx.TraceKV
	return &v
}

func (i *vectorInserter) Init(ctx context.Context) {
	i.OneInputHelper.Init(ctx)
	i.helper.Init()
}

func (i *vectorInserter) Next() coldata.Batch {
	ctx := context.TODO()
	b := i.Input.Next()
	if b.Length() == 0 {
		return coldata.ZeroBatch
	}
	// FIXME: figure out where to do kv trace, maybe wrap b in custom Putter interface?
	kvb := i.txn.NewBatch()
	// FIXME:  allow InsertBatch to partially insert stuff and loop here til everything is done,
	// if there are a ton of secondary indexes we could hit workmem limit building kv batch so we
	// need to be able to do it in chunks of rows.
	if err := colenc.InsertBatch(ctx, &i.helper, b, kvb, i.insertColIDtoRowIndex); err != nil {
		panic(err)
	}
	// FIXME: if we are autocommitting this should be CommitInBatch
	if err := i.txn.Run(ctx, kvb); err != nil {
		panic(row.ConvertBatchError(ctx, i.helper.TableDesc, kvb))
	}
	i.retBatch.ColVec(0).Int64().Set(0, int64(b.Length()))
	i.retBatch.SetLength(1)

	// FIXME: Possibly initiate a run of CREATE STATISTICS.
	// params.ExecCfg().StatsRefresher.NotifyMutation(n.run.ti.ri.Helper.TableDesc, len(n.input))

	return i.retBatch
}
