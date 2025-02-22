// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type requestedStat struct {
	columns             []descpb.ColumnID
	histogram           bool
	histogramMaxBuckets uint32
	name                string
	inverted            bool
}

const histogramSamples = 10000

// maxTimestampAge is the maximum allowed age of a scan timestamp during table
// stats collection, used when creating statistics AS OF SYSTEM TIME. The
// timestamp is advanced during long operations as needed. See TableReaderSpec.
//
// The lowest TTL we recommend is 10 minutes. This value must be lower than
// that.
var maxTimestampAge = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.stats.max_timestamp_age",
	"maximum age of timestamp during table statistics collection",
	5*time.Minute,
)

func (dsp *DistSQLPlanner) createAndAttachSamplers(
	p *PhysicalPlan,
	desc catalog.TableDescriptor,
	tableStats []*stats.TableStatistic,
	details jobspb.CreateStatsDetails,
	sampledColumnIDs []descpb.ColumnID,
	jobID jobspb.JobID,
	reqStats []requestedStat,
	sketchSpec, invSketchSpec []execinfrapb.SketchSpec,
) *PhysicalPlan {
	// Set up the samplers.
	sampler := &execinfrapb.SamplerSpec{
		Sketches:         sketchSpec,
		InvertedSketches: invSketchSpec,
	}
	sampler.MaxFractionIdle = details.MaxFractionIdle
	// For partial statistics this loop should only iterate once
	// since we only support one reqStat at a time.
	for _, s := range reqStats {
		if s.histogram {
			sampler.SampleSize = histogramSamples
			// This could be anything >= 2 to produce a histogram, but the max number
			// of buckets is probably also a reasonable minimum number of samples. (If
			// there are fewer rows than this in the table, there will be fewer
			// samples of course, which is fine.)
			sampler.MinSampleSize = s.histogramMaxBuckets
		}
	}

	// The sampler outputs the original columns plus a rank column, five
	// sketch columns, and two inverted histogram columns.
	outTypes := make([]*types.T, 0, len(p.GetResultTypes())+5)
	outTypes = append(outTypes, p.GetResultTypes()...)
	// An INT column for the rank of each row.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the sketch index.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the number of rows processed.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the number of rows that have a NULL in any sketch
	// column.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the size of the columns in this sketch.
	outTypes = append(outTypes, types.Int)
	// A BYTES column with the sketch data.
	outTypes = append(outTypes, types.Bytes)
	// An INT column indicating the inverted sketch index.
	outTypes = append(outTypes, types.Int)
	// A BYTES column with the inverted index key datum.
	outTypes = append(outTypes, types.Bytes)

	p.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{Sampler: sampler},
		execinfrapb.PostProcessSpec{},
		outTypes,
		execinfrapb.Ordering{},
	)

	// Estimate the expected number of rows based on existing stats in the cache.
	var rowsExpected uint64
	if len(tableStats) > 0 {
		overhead := stats.AutomaticStatisticsFractionStaleRows.Get(&dsp.st.SV)
		if autoStatsFractionStaleRowsForTable, ok := desc.AutoStatsFractionStaleRows(); ok {
			overhead = autoStatsFractionStaleRowsForTable
		}
		// Convert to a signed integer first to make the linter happy.
		rowsExpected = uint64(int64(
			// The total expected number of rows is the same number that was measured
			// most recently, plus some overhead for possible insertions.
			float64(tableStats[0].RowCount) * (1 + overhead),
		))
	}

	// Set up the final SampleAggregator stage.
	agg := &execinfrapb.SampleAggregatorSpec{
		Sketches:         sketchSpec,
		InvertedSketches: invSketchSpec,
		SampleSize:       sampler.SampleSize,
		MinSampleSize:    sampler.MinSampleSize,
		SampledColumnIDs: sampledColumnIDs,
		TableID:          desc.GetID(),
		JobID:            jobID,
		RowsExpected:     rowsExpected,
		DeleteOtherStats: details.DeleteOtherStats,
	}
	// Plan the SampleAggregator on the gateway, unless we have a single Sampler.
	node := dsp.gatewaySQLInstanceID
	if len(p.ResultRouters) == 1 {
		node = p.Processors[p.ResultRouters[0]].SQLInstanceID
	}
	p.AddSingleGroupStage(
		node,
		execinfrapb.ProcessorCoreUnion{SampleAggregator: agg},
		execinfrapb.PostProcessSpec{},
		[]*types.T{},
	)
	p.PlanToStreamColMap = []int{}
	return p
}

func (dsp *DistSQLPlanner) createPartialStatsPlan(
	ctx context.Context,
	planCtx *PlanningCtx,
	desc catalog.TableDescriptor,
	reqStats []requestedStat,
	jobID jobspb.JobID,
	details jobspb.CreateStatsDetails,
) (*PhysicalPlan, error) {

	// Currently, we limit the number of requests for partial statistics
	// stats at a given point in time to 1.
	// TODO (faizaanmadhani): Add support for multiple distinct requested
	// partial stats in one job.
	if len(reqStats) > 1 {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported, "cannot process multiple partial statistics at once")
	}

	reqStat := reqStats[0]

	if len(reqStat.columns) > 1 {
		// TODO (faizaanmadhani): Add support for creating multi-column stats
		return nil, pgerror.Newf(pgcode.FeatureNotSupported, "multi-column partial statistics are not currently supported")
	}

	// Fetch all stats for the table that matches the given table descriptor.
	tableStats, err := planCtx.ExtendedEvalCtx.ExecCfg.TableStatsCache.GetTableStats(ctx, desc)
	if err != nil {
		return nil, err
	}

	column, err := desc.FindColumnWithID(reqStat.columns[0])
	if err != nil {
		return nil, err
	}

	// Calculate the column we need to scan
	// TODO (faizaanmadhani): Iterate through all columns in a requested stat when
	// when we add support for multi-column statistics.
	var colCfg scanColumnsConfig
	colCfg.wantedColumns = append(colCfg.wantedColumns, column.GetID())

	// Initialize a dummy scanNode for the requested statistic.
	scan := scanNode{desc: desc}
	err = scan.initDescSpecificCol(colCfg, column)
	if err != nil {
		return nil, err
	}
	// Map the ColumnIDs to their ordinals in scan.cols
	// This loop should only iterate once, since we only
	// handle single column partial statistics.
	// TODO(faizaanmadhani): Add support for multi-column partial stats next
	var colIdxMap catalog.TableColMap
	for i, c := range scan.cols {
		colIdxMap.Set(c.GetID(), i)
	}

	var sb span.Builder
	sb.Init(planCtx.EvalContext(), planCtx.ExtendedEvalCtx.Codec, desc, scan.index)

	var histogram []cat.HistogramBucket
	// Find the histogram from the newest table statistic for our column
	// that is not partial and not forecasted. The first one we find will
	// be the latest due to the newest to oldest ordering property of the
	// cache.
	for _, t := range tableStats {
		if len(t.ColumnIDs) == 1 && column.GetID() == t.ColumnIDs[0] && t.PartialPredicate == "" && t.Name != jobspb.ForecastStatsName {
			histogram = t.Histogram
			break
		}
	}
	if len(histogram) == 0 {
		return nil, pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"column %s does not have a prior statistic, "+
				"or the prior histogram has no buckets and a partial statistic cannot be collected",
			column.GetName())
	}
	lowerBound, upperBound, err := getUsingExtremesBounds(planCtx, histogram)
	if err != nil {
		return nil, err
	}
	extremesSpans, err := constructUsingExtremesSpans(lowerBound, upperBound, scan.index)
	if err != nil {
		return nil, err
	}
	extremesPredicate := constructUsingExtremesPredicate(lowerBound, upperBound, column.GetName(), scan.index)
	// Get roachpb.Spans from constraint.Spans
	scan.spans, err = sb.SpansFromConstraintSpan(&extremesSpans, span.NoopSplitter())
	if err != nil {
		return nil, err
	}
	p, err := dsp.createTableReaders(ctx, planCtx, &scan)
	if err != nil {
		return nil, err
	}
	if details.AsOf != nil {
		val := maxTimestampAge.Get(&dsp.st.SV)
		for i := range p.Processors {
			spec := p.Processors[i].Spec.Core.TableReader
			spec.MaxTimestampAgeNanos = uint64(val)
		}
	}

	sampledColumnIDs := make([]descpb.ColumnID, len(scan.cols))
	spec := execinfrapb.SketchSpec{
		SketchType:          execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
		GenerateHistogram:   reqStat.histogram,
		HistogramMaxBuckets: reqStat.histogramMaxBuckets,
		Columns:             make([]uint32, len(reqStat.columns)),
		StatName:            reqStat.name,
		PartialPredicate:    extremesPredicate,
	}
	// For now, this loop should iterate only once, as we only
	// handle single-column partial statistics.
	// TODO(faizaanmadhani): Add support for multi-column partial stats next
	for i, colID := range reqStat.columns {
		colIdx, ok := colIdxMap.Get(colID)
		if !ok {
			panic("necessary column not scanned")
		}
		streamColIdx := uint32(p.PlanToStreamColMap[colIdx])
		spec.Columns[i] = streamColIdx
		sampledColumnIDs[streamColIdx] = colID
	}
	var sketchSpec, invSketchSpec []execinfrapb.SketchSpec
	if reqStat.inverted {
		// Find the first inverted index on the first column for collecting
		// histograms. Although there may be more than one index, we don't
		// currently have a way of using more than one or deciding which one
		// is better.
		//
		// We do not generate multi-column stats with histograms, so there
		// is no need to find an index for multi-column stats here.
		//
		// TODO(mjibson): allow multiple inverted indexes on the same column
		// (i.e., with different configurations). See #50655.
		if len(reqStat.columns) == 1 {
			for _, index := range desc.PublicNonPrimaryIndexes() {
				if index.GetType() == descpb.IndexDescriptor_INVERTED && index.InvertedColumnID() == column.GetID() {
					spec.Index = index.IndexDesc()
					break
				}
			}
		}
		// Even if spec.Index is nil because there isn't an inverted index
		// on the requested stats column, we can still proceed. We aren't
		// generating histograms in that case so we don't need an index
		// descriptor to generate the inverted index entries.
		invSketchSpec = append(invSketchSpec, spec)
	} else {
		sketchSpec = append(sketchSpec, spec)
	}
	return dsp.createAndAttachSamplers(
		p,
		desc,
		tableStats,
		details,
		sampledColumnIDs,
		jobID,
		reqStats,
		sketchSpec, invSketchSpec), nil
}

func (dsp *DistSQLPlanner) createStatsPlan(
	ctx context.Context,
	planCtx *PlanningCtx,
	desc catalog.TableDescriptor,
	reqStats []requestedStat,
	jobID jobspb.JobID,
	details jobspb.CreateStatsDetails,
) (*PhysicalPlan, error) {
	if len(reqStats) == 0 {
		return nil, errors.New("no stats requested")
	}

	// Calculate the set of columns we need to scan.
	var colCfg scanColumnsConfig
	var tableColSet catalog.TableColSet
	for _, s := range reqStats {
		for _, c := range s.columns {
			if !tableColSet.Contains(c) {
				tableColSet.Add(c)
				colCfg.wantedColumns = append(colCfg.wantedColumns, c)
			}
		}
	}

	// Create the table readers; for this we initialize a dummy scanNode.
	scan := scanNode{desc: desc}
	err := scan.initDescDefaults(colCfg)
	if err != nil {
		return nil, err
	}
	var colIdxMap catalog.TableColMap
	for i, c := range scan.cols {
		colIdxMap.Set(c.GetID(), i)
	}
	var sb span.Builder
	sb.Init(planCtx.EvalContext(), planCtx.ExtendedEvalCtx.Codec, desc, scan.index)
	scan.spans, err = sb.UnconstrainedSpans()
	if err != nil {
		return nil, err
	}
	scan.isFull = true

	p, err := dsp.createTableReaders(ctx, planCtx, &scan)
	if err != nil {
		return nil, err
	}

	if details.AsOf != nil {
		// If the read is historical, set the max timestamp age.
		val := maxTimestampAge.Get(&dsp.st.SV)
		for i := range p.Processors {
			spec := p.Processors[i].Spec.Core.TableReader
			spec.MaxTimestampAgeNanos = uint64(val)
		}
	}

	var sketchSpecs, invSketchSpecs []execinfrapb.SketchSpec
	sampledColumnIDs := make([]descpb.ColumnID, len(scan.cols))
	for _, s := range reqStats {
		spec := execinfrapb.SketchSpec{
			SketchType:          execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
			GenerateHistogram:   s.histogram,
			HistogramMaxBuckets: s.histogramMaxBuckets,
			Columns:             make([]uint32, len(s.columns)),
			StatName:            s.name,
		}
		for i, colID := range s.columns {
			colIdx, ok := colIdxMap.Get(colID)
			if !ok {
				panic("necessary column not scanned")
			}
			streamColIdx := uint32(p.PlanToStreamColMap[colIdx])
			spec.Columns[i] = streamColIdx
			sampledColumnIDs[streamColIdx] = colID
		}
		if s.inverted {
			// Find the first inverted index on the first column for collecting
			// histograms. Although there may be more than one index, we don't
			// currently have a way of using more than one or deciding which one
			// is better.
			//
			// We do not generate multi-column stats with histograms, so there
			// is no need to find an index for multi-column stats here.
			//
			// TODO(mjibson): allow multiple inverted indexes on the same column
			// (i.e., with different configurations). See #50655.
			if len(s.columns) == 1 {
				col := s.columns[0]
				for _, index := range desc.PublicNonPrimaryIndexes() {
					if index.GetType() == descpb.IndexDescriptor_INVERTED && index.InvertedColumnID() == col {
						spec.Index = index.IndexDesc()
						break
					}
				}
			}
			// Even if spec.Index is nil because there isn't an inverted index
			// on the requested stats column, we can still proceed. We aren't
			// generating histograms in that case so we don't need an index
			// descriptor to generate the inverted index entries.
			invSketchSpecs = append(invSketchSpecs, spec)
		} else {
			sketchSpecs = append(sketchSpecs, spec)
		}
	}

	tableStats, err := planCtx.ExtendedEvalCtx.ExecCfg.TableStatsCache.GetTableStats(ctx, desc)
	if err != nil {
		return nil, err
	}

	return dsp.createAndAttachSamplers(
		p,
		desc,
		tableStats,
		details,
		sampledColumnIDs,
		jobID,
		reqStats,
		sketchSpecs, invSketchSpecs), nil
}

func (dsp *DistSQLPlanner) createPlanForCreateStats(
	ctx context.Context, planCtx *PlanningCtx, jobID jobspb.JobID, details jobspb.CreateStatsDetails,
) (*PhysicalPlan, error) {
	reqStats := make([]requestedStat, len(details.ColumnStats))
	histogramCollectionEnabled := stats.HistogramClusterMode.Get(&dsp.st.SV)
	for i := 0; i < len(reqStats); i++ {
		histogram := details.ColumnStats[i].HasHistogram && histogramCollectionEnabled
		var histogramMaxBuckets uint32 = stats.DefaultHistogramBuckets
		if details.ColumnStats[i].HistogramMaxBuckets > 0 {
			histogramMaxBuckets = details.ColumnStats[i].HistogramMaxBuckets
		}
		reqStats[i] = requestedStat{
			columns:             details.ColumnStats[i].ColumnIDs,
			histogram:           histogram,
			histogramMaxBuckets: histogramMaxBuckets,
			name:                details.Name,
			inverted:            details.ColumnStats[i].Inverted,
		}
	}

	if len(reqStats) == 0 {
		return nil, errors.New("no stats requested")
	}

	tableDesc := tabledesc.NewBuilder(&details.Table).BuildImmutableTable()
	if details.UsingExtremes {
		return dsp.createPartialStatsPlan(ctx, planCtx, tableDesc, reqStats, jobID, details)
	}
	return dsp.createStatsPlan(ctx, planCtx, tableDesc, reqStats, jobID, details)
}

func (dsp *DistSQLPlanner) planAndRunCreateStats(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	job *jobs.Job,
	resultWriter *RowResultWriter,
) error {
	ctx = logtags.AddTag(ctx, "create-stats-distsql", nil)

	details := job.Details().(jobspb.CreateStatsDetails)
	physPlan, err := dsp.createPlanForCreateStats(ctx, planCtx, job.ID(), details)
	if err != nil {
		return err
	}

	dsp.FinalizePlan(planCtx, physPlan)

	recv := MakeDistSQLReceiver(
		ctx,
		resultWriter,
		tree.DDL,
		evalCtx.ExecCfg.RangeDescriptorCache,
		txn,
		evalCtx.ExecCfg.Clock,
		evalCtx.Tracing,
		evalCtx.ExecCfg.ContentionRegistry,
	)
	defer recv.Release()

	dsp.Run(ctx, planCtx, txn, physPlan, recv, evalCtx, nil /* finishedSetupFn */)
	return resultWriter.Err()
}

// getUsingExtremesBounds returns a tree.Datum representing the upper and lower
// bounds of the USING EXTREMES span for partial statistics.
func getUsingExtremesBounds(
	planCtx *PlanningCtx, histogram []cat.HistogramBucket,
) (tree.Datum, tree.Datum, error) {
	lowerBound := histogram[0].UpperBound
	upperBound := histogram[len(histogram)-1].UpperBound
	// Pick the earliest lowerBound that is not null,
	// but if none exist, return error
	for i := range histogram {
		hist := &histogram[i]
		if hist.UpperBound.Compare(planCtx.EvalContext(), tree.DNull) != 0 {
			lowerBound = hist.UpperBound
			break
		}
	}
	if lowerBound.Compare(planCtx.EvalContext(), tree.DNull) == 0 {
		return tree.DNull, tree.DNull,
			pgerror.Newf(
				pgcode.ObjectNotInPrerequisiteState,
				"only NULL values exist in the index, so partial stats cannot be collected")
	}
	return lowerBound, upperBound, nil
}

// constructUsingExtremesPredicate returns string of a predicate identifying
// the upper and lower bounds of the stats collection.
func constructUsingExtremesPredicate(
	lowerBound tree.Datum, upperBound tree.Datum, columnName string, index catalog.Index,
) string {
	lbExpr := tree.ComparisonExpr{
		Operator: treecmp.MakeComparisonOperator(treecmp.LT),
		Left:     &tree.ColumnItem{ColumnName: tree.Name(columnName)},
		Right:    lowerBound,
	}

	ubExpr := tree.ComparisonExpr{
		Operator: treecmp.MakeComparisonOperator(treecmp.GT),
		Left:     &tree.ColumnItem{ColumnName: tree.Name(columnName)},
		Right:    upperBound,
	}
	nullExpr := tree.IsNullExpr{
		Expr: &tree.ColumnItem{ColumnName: tree.Name(columnName)},
	}

	pred := tree.OrExpr{
		Left: &nullExpr,
		Right: &tree.OrExpr{
			Left:  &lbExpr,
			Right: &ubExpr,
		},
	}
	return tree.Serialize(&pred)
}

// constructExtremesSpans returns a constraint.Spans consisting of a
// lowerbound and upperbound span covering the extremes of an index.
func constructUsingExtremesSpans(
	lowerBound tree.Datum, upperBound tree.Datum, index catalog.Index,
) (constraint.Spans, error) {
	var lbSpan constraint.Span
	var ubSpan constraint.Span
	if index.GetKeyColumnDirection(0) == catpb.IndexColumn_ASC {
		lbSpan.Init(constraint.EmptyKey, constraint.IncludeBoundary, constraint.MakeKey(lowerBound), constraint.ExcludeBoundary)
		ubSpan.Init(constraint.MakeKey(upperBound), constraint.ExcludeBoundary, constraint.EmptyKey, constraint.IncludeBoundary)
	} else {
		lbSpan.Init(constraint.MakeKey(lowerBound), constraint.ExcludeBoundary, constraint.EmptyKey, constraint.IncludeBoundary)
		ubSpan.Init(constraint.EmptyKey, constraint.IncludeBoundary, constraint.MakeKey(upperBound), constraint.ExcludeBoundary)
	}
	var extremesSpans constraint.Spans
	if index.GetKeyColumnDirection(0) == catpb.IndexColumn_ASC {
		extremesSpans.InitSingleSpan(&lbSpan)
		extremesSpans.Append(&ubSpan)
	} else {
		extremesSpans.InitSingleSpan(&ubSpan)
		extremesSpans.Append(&lbSpan)
	}

	return extremesSpans, nil
}
