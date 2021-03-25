// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/errors"
)

type stepProcessor struct {
	execinfra.ProcessorBase
	input      execinfra.RowSource
	rowCounter int
	step       int // const?
}

var _ execinfra.Processor = &stepProcessor{}
var _ execinfra.RowSource = &stepProcessor{}
var _ execinfra.OpNode = &stepProcessor{}

const stepProcName = "step"

func newStepProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*stepProcessor, error) {
	n := &stepProcessor{input: input}
	if err := n.Init(
		n,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{InputsToDrain: []execinfra.RowSource{n.input}},
	); err != nil {
		return nil, err
	}
	ctx := flowCtx.EvalCtx.Ctx()
	if execinfra.ShouldCollectStats(ctx, flowCtx) {
		n.input = newInputStatCollector(n.input)
		n.ExecStatsForTrace = n.execStatsForTrace
	}
	return n, nil
}

// Start is part of the RowSource interface.
func (n *stepProcessor) Start(ctx context.Context) {
	ctx = n.StartInternal(ctx, noopProcName)
	n.input.Start(ctx)
}

// Next is part of the RowSource interface.
func (n *stepProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for n.State == execinfra.StateRunning {
		row, meta := n.input.Next()

		if meta != nil {
			if meta.Err != nil {
				n.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}

		n.rowCounter++
		if n.rowCounter%n.step == 1 {
			if outRow := n.ProcessRowHelper(row); outRow != nil {
				return outRow, nil
			}
		} else {
			if row == nil {
				n.MoveToDraining(nil /* err */)
				break
			}
		}
	}
	return nil, n.DrainHelper()
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (n *stepProcessor) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(n.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Output: n.Out.Stats(),
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (n *stepProcessor) ChildCount(bool) int {
	if _, ok := n.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (n *stepProcessor) Child(nth int, _ bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := n.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to noop is not an execinfra.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
