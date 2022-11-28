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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type vectorInserter struct {
	colexecop.InitHelper
	colexecop.OneInputNode
	input                 colexecop.Operator
	helper                row.RowHelper
	insertCols            []catalog.Column
	insertColIDtoRowIndex catalog.TableColMap
	retBatch              coldata.Batch
}

var _ colexecop.Operator = &vectorInserter{}

func NewInsertOp(ctx context.Context, flowCtx *execinfra.FlowCtx, spec *execinfrapb.InsertSpec,
	input colexecop.Operator, typs []*types.T, outputIdx int,
) colexecop.Operator {
	desc := flowCtx.TableDescriptor(ctx, &spec.Table)
	v := vectorInserter{
		input: input,
		// TODO: figure out how to plumb row metrics which is used to report large rows
		helper:   row.NewRowHelper(flowCtx.Codec(), desc, desc.WritableNonPrimaryIndexes(), &flowCtx.Cfg.Settings.SV, false, nil /*metrics*/),
		retBatch: coldata.NewMemBatchWithCapacity(typs, 1, coldata.StandardColumnFactory),
	}
	v.helper.TraceKV = flowCtx.TraceKV
	return &v
}

func (i *vectorInserter) Init(ctx context.Context) {
	i.helper.Init()
}

func (i *vectorInserter) Next() coldata.Batch {
	b := i.input.Next()
	if b.Length() == 0 {
		return coldata.ZeroBatch
	}
	// insert rows...
	i.retBatch.ColVec(0).Int64().Set(0, int64(b.Length()))
	i.retBatch.SetLength(1)
	return i.retBatch
}
