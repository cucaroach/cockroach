// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storepool

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// OverrideStorePool is an implementation of AllocatorStorePool that allows
// the ability to override a node's liveness status for the purposes of
// evaluation for the allocator, otherwise delegating to an actual StorePool
// for all its logic, including management and lookup of store descriptors.
//
// The OverrideStorePool is meant to provide a read-only overlay to an
// StorePool, and as such, read-only methods are dispatched to the underlying
// StorePool using the configured NodeLivenessFunc override. Methods that
// mutate the state of the StorePool such as UpdateLocalStoreAfterRebalance
// are instead no-ops.
//
// NB: Despite the fact that StorePool.DetailsMu is held in write mode in
// some of the dispatched functions, these do not mutate the state of the
// underlying StorePool.
type OverrideStorePool struct {
	sp *StorePool

	overrideNodeLivenessFn NodeLivenessFunc
}

var _ AllocatorStorePool = &OverrideStorePool{}

func NewOverrideStorePool(storePool *StorePool, nl NodeLivenessFunc) *OverrideStorePool {
	return &OverrideStorePool{
		sp:                     storePool,
		overrideNodeLivenessFn: nl,
	}
}

func (o *OverrideStorePool) String() string {
	return o.sp.statusString(o.overrideNodeLivenessFn)
}

// IsStoreReadyForRoutineReplicaTransfer implements the AllocatorStorePool interface.
func (o *OverrideStorePool) IsStoreReadyForRoutineReplicaTransfer(
	ctx context.Context, targetStoreID roachpb.StoreID,
) bool {
	return o.sp.isStoreReadyForRoutineReplicaTransferInternal(ctx, targetStoreID, o.overrideNodeLivenessFn)
}

// DecommissioningReplicas implements the AllocatorStorePool interface.
func (o *OverrideStorePool) DecommissioningReplicas(
	repls []roachpb.ReplicaDescriptor,
) []roachpb.ReplicaDescriptor {
	return o.sp.decommissioningReplicasWithLiveness(repls, o.overrideNodeLivenessFn)
}

// GetStoreList implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetStoreList(
	filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	o.sp.DetailsMu.Lock()
	defer o.sp.DetailsMu.Unlock()

	var storeIDs roachpb.StoreIDSlice
	for storeID := range o.sp.DetailsMu.StoreDetails {
		storeIDs = append(storeIDs, storeID)
	}
	return o.sp.getStoreListFromIDsLocked(storeIDs, o.overrideNodeLivenessFn, filter)
}

// GetStoreListFromIDs implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetStoreListFromIDs(
	storeIDs roachpb.StoreIDSlice, filter StoreFilter,
) (StoreList, int, ThrottledStoreReasons) {
	o.sp.DetailsMu.Lock()
	defer o.sp.DetailsMu.Unlock()
	return o.sp.getStoreListFromIDsLocked(storeIDs, o.overrideNodeLivenessFn, filter)
}

// LiveAndDeadReplicas implements the AllocatorStorePool interface.
func (o *OverrideStorePool) LiveAndDeadReplicas(
	repls []roachpb.ReplicaDescriptor, includeSuspectAndDrainingStores bool,
) (liveReplicas, deadReplicas []roachpb.ReplicaDescriptor) {
	return o.sp.liveAndDeadReplicasWithLiveness(repls, o.overrideNodeLivenessFn, includeSuspectAndDrainingStores)
}

// ClusterNodeCount implements the AllocatorStorePool interface.
func (o *OverrideStorePool) ClusterNodeCount() int {
	return o.sp.ClusterNodeCount()
}

// IsDeterministic implements the AllocatorStorePool interface.
func (o *OverrideStorePool) IsDeterministic() bool {
	return o.sp.deterministic
}

// Clock implements the AllocatorStorePool interface.
func (o *OverrideStorePool) Clock() *hlc.Clock {
	return o.sp.clock
}

// GetLocalitiesByNode implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetLocalitiesByNode(
	replicas []roachpb.ReplicaDescriptor,
) map[roachpb.NodeID]roachpb.Locality {
	return o.sp.GetLocalitiesByNode(replicas)
}

// GetLocalitiesByStore implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetLocalitiesByStore(
	replicas []roachpb.ReplicaDescriptor,
) map[roachpb.StoreID]roachpb.Locality {
	return o.sp.GetLocalitiesByStore(replicas)
}

// GetStores implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetStores() map[roachpb.StoreID]roachpb.StoreDescriptor {
	return o.sp.GetStores()
}

// GetStoreDescriptor implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GetStoreDescriptor(
	storeID roachpb.StoreID,
) (roachpb.StoreDescriptor, bool) {
	return o.sp.GetStoreDescriptor(storeID)
}

// GossipNodeIDAddress implements the AllocatorStorePool interface.
func (o *OverrideStorePool) GossipNodeIDAddress(
	nodeID roachpb.NodeID,
) (*util.UnresolvedAddr, error) {
	return o.sp.GossipNodeIDAddress(nodeID)
}

// UpdateLocalStoreAfterRebalance implements the AllocatorStorePool interface.
// This override method is a no-op, as
// StorePool.UpdateLocalStoreAfterRebalance(..) is not a read-only method and
// mutates the state of the held store details.
func (o *OverrideStorePool) UpdateLocalStoreAfterRebalance(
	_ roachpb.StoreID, _ allocator.RangeUsageInfo, _ roachpb.ReplicaChangeType,
) {
}

// UpdateLocalStoresAfterLeaseTransfer implements the AllocatorStorePool interface.
// This override method is a no-op, as
// StorePool.UpdateLocalStoresAfterLeaseTransfer(..) is not a read-only method and
// mutates the state of the held store details.
func (o *OverrideStorePool) UpdateLocalStoresAfterLeaseTransfer(
	_ roachpb.StoreID, _ roachpb.StoreID, _ float64,
) {
}

// UpdateLocalStoreAfterRelocate implements the AllocatorStorePool interface.
// This override method is a no-op, as
// StorePool.UpdateLocalStoreAfterRelocate(..) is not a read-only method and
// mutates the state of the held store details.
func (o *OverrideStorePool) UpdateLocalStoreAfterRelocate(
	_, _ []roachpb.ReplicationTarget, _, _ []roachpb.ReplicaDescriptor, _ roachpb.StoreID, _ float64,
) {
}
