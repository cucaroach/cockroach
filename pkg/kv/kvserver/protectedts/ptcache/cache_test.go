// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptcache_test

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCacheBasic exercises the basic behavior of the Cache.
func TestCacheBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				ProtectedTS: &protectedts.TestingKnobs{
					DisableProtectedTimestampForMultiTenant: true,
				},
			},
		})
	defer s.Stopper().Stop(ctx)
	p := ptstorage.WithDatabase(
		ptstorage.New(s.ClusterSettings(),
			s.InternalExecutor().(sqlutil.InternalExecutor),
			&protectedts.TestingKnobs{DisableProtectedTimestampForMultiTenant: true}),
		s.DB(),
	)

	// Set the poll interval to be very short.
	protectedts.PollInterval.Override(ctx, &s.ClusterSettings().SV, 500*time.Microsecond)

	c := ptcache.New(ptcache.Config{
		Settings: s.ClusterSettings(),
		DB:       s.DB(),
		Storage:  p,
	})
	require.NoError(t, c.Start(ctx, s.Stopper()))

	// Make sure that protected timestamp gets updated.
	ts := waitForAsOfAfter(t, c, hlc.Timestamp{})

	// Make sure that it gets updated again.
	waitForAsOfAfter(t, c, ts)

	// Then we'll add a record and make sure it gets seen.
	sp := tableSpan(42)
	r, createdAt := protect(t, s, p, s.Clock().Now(), sp)
	testutils.SucceedsSoon(t, func() error {
		var coveredBy []*ptpb.Record
		seenTS := c.Iterate(ctx, sp.Key, sp.EndKey,
			func(r *ptpb.Record) (wantMore bool) {
				coveredBy = append(coveredBy, r)
				return true
			})
		if len(coveredBy) == 0 {
			assert.True(t, seenTS.Less(createdAt), "%v %v", seenTS, createdAt)
			return errors.Errorf("expected %v to be covered", sp)
		}
		require.True(t, !seenTS.Less(createdAt), "%v %v", seenTS, createdAt)
		require.EqualValues(t, []*ptpb.Record{r}, coveredBy)
		return nil
	})

	// Then release the record and make sure that that gets seen.
	require.Nil(t, p.Release(ctx, nil /* txn */, r.ID.GetUUID()))
	testutils.SucceedsSoon(t, func() error {
		var coveredBy []*ptpb.Record
		_ = c.Iterate(ctx, sp.Key, sp.EndKey,
			func(r *ptpb.Record) (wantMore bool) {
				coveredBy = append(coveredBy, r)
				return true
			})
		if len(coveredBy) > 0 {
			return errors.Errorf("expected %v not to be covered", sp)
		}
		return nil
	})
}

func TestRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := &scanTracker{}
	ptsKnobs := &protectedts.TestingKnobs{
		DisableProtectedTimestampForMultiTenant: true,
	}
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: st.requestFilter,
				},
				ProtectedTS: ptsKnobs,
			},
		})
	defer s.Stopper().Stop(ctx)
	p := ptstorage.WithDatabase(
		ptstorage.New(
			s.ClusterSettings(),
			s.InternalExecutor().(sqlutil.InternalExecutor),
			ptsKnobs),
		s.DB(),
	)

	// Set the poll interval to be very long.
	protectedts.PollInterval.Override(ctx, &s.ClusterSettings().SV, 500*time.Hour)

	c := ptcache.New(ptcache.Config{
		Settings: s.ClusterSettings(),
		DB:       s.DB(),
		Storage:  p,
	})
	require.NoError(t, c.Start(ctx, s.Stopper()))
	t.Run("already up-to-date", func(t *testing.T) {
		ts := waitForAsOfAfter(t, c, hlc.Timestamp{})
		st.resetCounters()
		require.NoError(t, c.Refresh(ctx, ts))
		st.verifyCounters(t, 0, 0) // already up to date
	})
	t.Run("needs refresh, no change", func(t *testing.T) {
		ts := waitForAsOfAfter(t, c, hlc.Timestamp{})
		require.NoError(t, c.Refresh(ctx, ts.Next()))
		st.verifyCounters(t, 1, 0) // just need to scan meta
	})
	t.Run("needs refresh, with change", func(t *testing.T) {
		_, createdAt := protect(t, s, p, s.Clock().Now(), metaTableSpan)
		st.resetCounters()
		require.NoError(t, c.Refresh(ctx, createdAt))
		st.verifyCounters(t, 2, 1) // need to scan meta and then scan everything
	})
	t.Run("cancelation returns early", func(t *testing.T) {
		withCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan struct{})
		st.setFilter(func(ba *roachpb.BatchRequest) *roachpb.Error {
			if scanReq, ok := ba.GetArg(roachpb.Scan); ok {
				scan := scanReq.(*roachpb.ScanRequest)
				if scan.Span().Overlaps(metaTableSpan) {
					<-done
				}
			}
			return nil
		})
		go func() { time.Sleep(time.Millisecond); cancel() }()
		require.EqualError(t, c.Refresh(withCancel, s.Clock().Now()),
			context.Canceled.Error())
		close(done)
	})
	t.Run("error propagates while fetching metadata", func(t *testing.T) {
		st.setFilter(func(ba *roachpb.BatchRequest) *roachpb.Error {
			if scanReq, ok := ba.GetArg(roachpb.Scan); ok {
				scan := scanReq.(*roachpb.ScanRequest)
				if scan.Span().Overlaps(metaTableSpan) {
					return roachpb.NewError(errors.New("boom"))
				}
			}
			return nil
		})
		defer st.setFilter(nil)
		require.Regexp(t, "boom", c.Refresh(ctx, s.Clock().Now()).Error())
	})
	t.Run("error propagates while fetching records", func(t *testing.T) {
		protect(t, s, p, s.Clock().Now(), metaTableSpan)
		st.setFilter(func(ba *roachpb.BatchRequest) *roachpb.Error {
			if scanReq, ok := ba.GetArg(roachpb.Scan); ok {
				scan := scanReq.(*roachpb.ScanRequest)
				if scan.Span().Overlaps(recordsTableSpan) {
					return roachpb.NewError(errors.New("boom"))
				}
			}
			return nil
		})
		defer st.setFilter(nil)
		require.Regexp(t, "boom", c.Refresh(ctx, s.Clock().Now()).Error())
	})
	t.Run("Iterate does not hold mutex", func(t *testing.T) {
		inIterate := make(chan chan struct{})
		rec, createdAt := protect(t, s, p, s.Clock().Now(), metaTableSpan)
		require.NoError(t, c.Refresh(ctx, createdAt))
		go c.Iterate(ctx, keys.MinKey, keys.MaxKey, func(r *ptpb.Record) (wantMore bool) {
			if r.ID.GetUUID() != rec.ID.GetUUID() {
				return true
			}
			// Make sure we see the record we created and use it to signal the main
			// goroutine.
			waitUntil := make(chan struct{})
			inIterate <- waitUntil
			<-waitUntil
			defer close(inIterate)
			return false
		})
		// Wait until we get to the record in iteration and pause, perform an
		// operation, amd then refresh after it. This will demonstrate that the
		// iteration call does not block concurrent refreshes.
		ch := <-inIterate
		require.NoError(t, p.Release(ctx, nil /* txn */, rec.ID.GetUUID()))
		require.NoError(t, c.Refresh(ctx, s.Clock().Now()))
		// Signal the Iterate loop to exit and wait for it to close the channel.
		close(ch)
		<-inIterate
	})
}

func TestStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	setup := func() (serverutils.TestServerInterface, *ptcache.Cache) {
		s, _, _ := serverutils.StartServer(t,
			base.TestServerArgs{
				Knobs: base.TestingKnobs{
					ProtectedTS: &protectedts.TestingKnobs{
						DisableProtectedTimestampForMultiTenant: true,
					},
				},
			})
		p := s.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		// Set the poll interval to be very long.
		protectedts.PollInterval.Override(ctx, &s.ClusterSettings().SV, 500*time.Hour)
		c := ptcache.New(ptcache.Config{
			Settings: s.ClusterSettings(),
			DB:       s.DB(),
			Storage:  p,
		})
		return s, c
	}

	t.Run("double start", func(t *testing.T) {
		tc, c := setup()
		defer tc.Stopper().Stop(ctx)
		require.NoError(t, c.Start(ctx, tc.Stopper()))
		require.EqualError(t, c.Start(ctx, tc.Stopper()),
			"cannot start a Cache more than once")
	})
	t.Run("already stopped", func(t *testing.T) {
		s, c := setup()
		s.Stopper().Stop(ctx)
		require.EqualError(t, c.Start(ctx, s.Stopper()),
			stop.ErrUnavailable.Error())
	})
}

func TestQueryRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	p := ptstorage.WithDatabase(
		ptstorage.New(
			s.ClusterSettings(),
			s.InternalExecutor().(sqlutil.InternalExecutor),
			&protectedts.TestingKnobs{DisableProtectedTimestampForMultiTenant: true}),
		s.DB(),
	)
	// Set the poll interval to be very long.
	protectedts.PollInterval.Override(ctx, &s.ClusterSettings().SV, 500*time.Hour)
	c := ptcache.New(ptcache.Config{
		Settings: s.ClusterSettings(),
		DB:       s.DB(),
		Storage:  p,
	})
	require.NoError(t, c.Start(ctx, s.Stopper()))

	// Wait for the initial fetch.
	waitForAsOfAfter(t, c, hlc.Timestamp{})
	// Create two records.
	sp42 := tableSpan(42)
	r1, createdAt1 := protect(t, s, p, s.Clock().Now(), sp42)
	r2, createdAt2 := protect(t, s, p, s.Clock().Now(), sp42)
	// Ensure they both don't exist and that the read timestamps precede the
	// create timestamps.
	exists1, asOf := c.QueryRecord(ctx, r1.ID.GetUUID())
	require.False(t, exists1)
	require.True(t, asOf.Less(createdAt1))
	exists2, asOf := c.QueryRecord(ctx, r2.ID.GetUUID())
	require.False(t, exists2)
	require.True(t, asOf.Less(createdAt2))
	// Go refresh the state and make sure they both exist.
	require.NoError(t, c.Refresh(ctx, createdAt2))
	exists1, asOf = c.QueryRecord(ctx, r1.ID.GetUUID())
	require.True(t, exists1)
	require.True(t, !asOf.Less(createdAt1))
	exists2, asOf = c.QueryRecord(ctx, r2.ID.GetUUID())
	require.True(t, exists2)
	require.True(t, !asOf.Less(createdAt2))
	// Release 2 and then create 3.
	require.NoError(t, p.Release(ctx, nil /* txn */, r2.ID.GetUUID()))
	r3, createdAt3 := protect(t, s, p, s.Clock().Now(), sp42)
	exists2, asOf = c.QueryRecord(ctx, r2.ID.GetUUID())
	require.True(t, exists2)
	require.True(t, asOf.Less(createdAt3))
	exists3, asOf := c.QueryRecord(ctx, r3.ID.GetUUID())
	require.False(t, exists3)
	require.True(t, asOf.Less(createdAt3))
	// Go refresh the state and make sure 1 and 3 exist.
	require.NoError(t, c.Refresh(ctx, createdAt3))
	exists1, _ = c.QueryRecord(ctx, r1.ID.GetUUID())
	require.True(t, exists1)
	exists2, _ = c.QueryRecord(ctx, r2.ID.GetUUID())
	require.False(t, exists2)
	exists3, _ = c.QueryRecord(ctx, r3.ID.GetUUID())
	require.True(t, exists3)
}

func TestIterate(t *testing.T) {
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	p := ptstorage.WithDatabase(
		ptstorage.New(s.ClusterSettings(),
			s.InternalExecutor().(sqlutil.InternalExecutor),
			&protectedts.TestingKnobs{DisableProtectedTimestampForMultiTenant: true}),
		s.DB(),
	)

	// Set the poll interval to be very long.
	protectedts.PollInterval.Override(ctx, &s.ClusterSettings().SV, 500*time.Hour)

	c := ptcache.New(ptcache.Config{
		Settings: s.ClusterSettings(),
		DB:       s.DB(),
		Storage:  p,
	})
	require.NoError(t, c.Start(ctx, s.Stopper()))

	sp42 := tableSpan(42)
	sp43 := tableSpan(43)
	sp44 := tableSpan(44)
	r1, _ := protect(t, s, p, s.Clock().Now(), sp42)
	r2, _ := protect(t, s, p, s.Clock().Now(), sp43)
	r3, _ := protect(t, s, p, s.Clock().Now(), sp44)
	r4, _ := protect(t, s, p, s.Clock().Now(), sp42, sp43)
	require.NoError(t, c.Refresh(ctx, s.Clock().Now()))
	t.Run("all", func(t *testing.T) {
		var recs records
		c.Iterate(ctx, sp42.Key, sp44.EndKey, recs.accumulate)
		require.EqualValues(t, recs.sorted(), (&records{r1, r2, r3, r4}).sorted())
	})
	t.Run("some", func(t *testing.T) {
		var recs records
		c.Iterate(ctx, sp42.Key, sp42.EndKey, recs.accumulate)
		require.EqualValues(t, recs.sorted(), (&records{r1, r4}).sorted())
	})
	t.Run("none", func(t *testing.T) {
		var recs records
		c.Iterate(ctx, sp42.Key, sp42.EndKey, recs.accumulate)
		require.EqualValues(t, recs.sorted(), (&records{r1, r4}).sorted())
	})
	t.Run("early return from iteration", func(t *testing.T) {
		var called int
		c.Iterate(ctx, sp42.Key, sp44.EndKey, func(*ptpb.Record) (wantMore bool) {
			called++
			return false
		})
		require.Equal(t, 1, called)
	})
}

type records []*ptpb.Record

func (recs *records) accumulate(r *ptpb.Record) (wantMore bool) {
	(*recs) = append(*recs, r)
	return true
}

func (recs *records) sorted() []*ptpb.Record {
	sort.Slice(*recs, func(i, j int) bool {
		return bytes.Compare((*recs)[i].ID[:], (*recs)[j].ID[:]) < 0
	})
	return *recs
}

func TestGetProtectionTimestamps(t *testing.T) {
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				ProtectedTS: &protectedts.TestingKnobs{
					DisableProtectedTimestampForMultiTenant: true,
				},
			},
		})
	defer s.Stopper().Stop(ctx)
	// Set the poll interval to be very long.
	protectedts.PollInterval.Override(ctx, &s.ClusterSettings().SV, 500*time.Hour)

	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}
	sp42 := tableSpan(42)
	sp43 := tableSpan(43)
	sp44 := tableSpan(44)
	sp4243 := roachpb.Span{Key: sp42.Key, EndKey: sp43.EndKey}

	for _, testCase := range []struct {
		name string
		test func(t *testing.T, p protectedts.Storage, c *ptcache.Cache, cleanup func(...*ptpb.Record))
	}{
		{
			name: "multiple records apply to a single span",
			test: func(t *testing.T, p protectedts.Storage, c *ptcache.Cache, cleanup func(...*ptpb.Record)) {
				r1, _ := protect(t, s, p, ts(10), sp42)
				r2, _ := protect(t, s, p, ts(11), sp42)
				r3, _ := protect(t, s, p, ts(6), sp42)
				require.NoError(t, c.Refresh(ctx, s.Clock().Now()))

				protectionTimestamps, _, err := c.GetProtectionTimestamps(ctx, sp42)
				require.NoError(t, err)
				sort.Slice(protectionTimestamps, func(i, j int) bool {
					return protectionTimestamps[i].Less(protectionTimestamps[j])
				})
				require.Equal(t, []hlc.Timestamp{ts(6), ts(10), ts(11)}, protectionTimestamps)
				cleanup(r1, r2, r3)
			},
		},
		{
			name: "no records apply",
			test: func(t *testing.T, p protectedts.Storage, c *ptcache.Cache, cleanup func(...*ptpb.Record)) {
				r1, _ := protect(t, s, p, ts(5), sp43)
				r2, _ := protect(t, s, p, ts(10), sp44)
				require.NoError(t, c.Refresh(ctx, s.Clock().Now()))
				protectionTimestamps, _, err := c.GetProtectionTimestamps(ctx, sp42)
				require.NoError(t, err)
				require.Equal(t, []hlc.Timestamp(nil), protectionTimestamps)
				cleanup(r1, r2)
			},
		},
		{
			name: "multiple overlapping spans multiple records",
			test: func(t *testing.T, p protectedts.Storage, c *ptcache.Cache, cleanup func(...*ptpb.Record)) {
				r1, _ := protect(t, s, p, ts(10), sp42)
				r2, _ := protect(t, s, p, ts(15), sp42)
				r3, _ := protect(t, s, p, ts(5), sp43)
				r4, _ := protect(t, s, p, ts(6), sp43)
				r5, _ := protect(t, s, p, ts(25), keys.EverythingSpan)
				// Also add a record that doesn't overlap with the requested span and
				// ensure it isn't retrieved below.
				r6, _ := protect(t, s, p, ts(20), sp44)
				require.NoError(t, c.Refresh(ctx, s.Clock().Now()))

				protectionTimestamps, _, err := c.GetProtectionTimestamps(ctx, sp4243)
				require.NoError(t, err)
				sort.Slice(protectionTimestamps, func(i, j int) bool {
					return protectionTimestamps[i].Less(protectionTimestamps[j])
				})
				require.Equal(
					t, []hlc.Timestamp{ts(5), ts(6), ts(10), ts(15), ts(25)}, protectionTimestamps,
				)
				cleanup(r1, r2, r3, r4, r5, r6)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			p := ptstorage.WithDatabase(
				ptstorage.New(s.ClusterSettings(),
					s.InternalExecutor().(sqlutil.InternalExecutor),
					&protectedts.TestingKnobs{DisableProtectedTimestampForMultiTenant: true}),
				s.DB(),
			)

			c := ptcache.New(ptcache.Config{
				Settings: s.ClusterSettings(),
				DB:       s.DB(),
				Storage:  p,
			})
			require.NoError(t, c.Start(ctx, s.Stopper()))

			testCase.test(t, p, c, func(records ...*ptpb.Record) {
				for _, r := range records {
					require.NoError(t, p.Release(ctx, nil, r.ID.GetUUID()))
				}
			})
		})
	}
}

func TestSettingChangedLeadsToFetch(t *testing.T) {
	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	p := ptstorage.WithDatabase(
		ptstorage.New(s.ClusterSettings(),
			s.InternalExecutor().(sqlutil.InternalExecutor),
			&protectedts.TestingKnobs{DisableProtectedTimestampForMultiTenant: true}),
		s.DB(),
	)

	// Set the poll interval to be very long.
	protectedts.PollInterval.Override(ctx, &s.ClusterSettings().SV, 500*time.Hour)

	c := ptcache.New(ptcache.Config{
		Settings: s.ClusterSettings(),
		DB:       s.DB(),
		Storage:  p,
	})
	require.NoError(t, c.Start(ctx, s.Stopper()))

	// Make sure that the initial state has been fetched.
	ts := waitForAsOfAfter(t, c, hlc.Timestamp{})
	// Make sure there isn't another rapid fetch.
	// If there were a bug it might fail under stress.
	time.Sleep(time.Millisecond)
	_, asOf := c.QueryRecord(ctx, uuid.UUID{})
	require.Equal(t, asOf, ts)
	// Set the polling interval back to something very short.
	protectedts.PollInterval.Override(ctx, &s.ClusterSettings().SV, 100*time.Microsecond)
	// Ensure that the state is updated again soon.
	waitForAsOfAfter(t, c, ts)
}

func waitForAsOfAfter(t *testing.T, c protectedts.Cache, ts hlc.Timestamp) (asOf hlc.Timestamp) {
	testutils.SucceedsSoon(t, func() error {
		var exists bool
		exists, asOf = c.QueryRecord(context.Background(), uuid.UUID{})
		require.False(t, exists)
		if !ts.Less(asOf) {
			return errors.Errorf("expected an update to occur")
		}
		return nil
	})
	return asOf
}

func tableSpan(tableID uint32) roachpb.Span {
	return roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(tableID),
		EndKey: keys.SystemSQLCodec.TablePrefix(tableID).PrefixEnd(),
	}
}

func protect(
	t *testing.T,
	s serverutils.TestServerInterface,
	p protectedts.Storage,
	protectTS hlc.Timestamp,
	spans ...roachpb.Span,
) (r *ptpb.Record, createdAt hlc.Timestamp) {
	r = &ptpb.Record{
		ID:              uuid.MakeV4().GetBytes(),
		Timestamp:       protectTS,
		Mode:            ptpb.PROTECT_AFTER,
		DeprecatedSpans: spans,
	}
	ctx := context.Background()
	txn := s.DB().NewTxn(ctx, "test")
	require.NoError(t, p.Protect(ctx, txn, r))
	require.NoError(t, txn.Commit(ctx))
	_, err := p.GetRecord(ctx, nil, r.ID.GetUUID())
	require.NoError(t, err)
	createdAt = txn.CommitTimestamp()
	return r, createdAt
}

var (
	metaTableSpan    = tableSpan(uint32(systemschema.ProtectedTimestampsMetaTable.GetID()))
	recordsTableSpan = tableSpan(uint32(systemschema.ProtectedTimestampsRecordsTable.GetID()))
)

type scanTracker struct {
	mu                syncutil.Mutex
	metaTableScans    int
	recordsTableScans int
	filterFunc        func(ba *roachpb.BatchRequest) *roachpb.Error
}

func (st *scanTracker) resetCounters() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.metaTableScans, st.recordsTableScans = 0, 0
}

func (st *scanTracker) verifyCounters(t *testing.T, expMeta, expRecords int) {
	t.Helper()
	st.mu.Lock()
	defer st.mu.Unlock()
	require.Equal(t, expMeta, st.metaTableScans)
	require.Equal(t, expRecords, st.recordsTableScans)
}

func (st *scanTracker) setFilter(f func(*roachpb.BatchRequest) *roachpb.Error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.filterFunc = f
}

func (st *scanTracker) requestFilter(_ context.Context, ba *roachpb.BatchRequest) *roachpb.Error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if scanReq, ok := ba.GetArg(roachpb.Scan); ok {
		scan := scanReq.(*roachpb.ScanRequest)
		if scan.Span().Overlaps(metaTableSpan) {
			st.metaTableScans++
		} else if scan.Span().Overlaps(recordsTableSpan) {
			st.recordsTableScans++
		}
	}
	if st.filterFunc != nil {
		return st.filterFunc(ba)
	}
	return nil
}
