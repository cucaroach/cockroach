// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nstree

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// MutableCatalog is like Catalog but mutable.
type MutableCatalog struct {
	Catalog
}

var _ validate.ValidationDereferencer = MutableCatalog{}

func (mc *MutableCatalog) maybeInitialize() {
	if mc.IsInitialized() {
		return
	}
	mc.byID = makeByIDMap()
	mc.byName = makeByNameMap()
}

// Clear empties the MutableCatalog.
func (mc *MutableCatalog) Clear() {
	if mc.IsInitialized() {
		mc.byID.clear()
		mc.byName.clear()
	}
	*mc = MutableCatalog{}
}

func (mc *MutableCatalog) ensureForID(id descpb.ID) *byIDEntry {
	mc.maybeInitialize()
	newEntry := &byIDEntry{
		id: id,
	}
	if replaced := mc.byID.upsert(newEntry); replaced != nil {
		*newEntry = *(replaced.(*byIDEntry))
	} else {
		mc.byteSize += newEntry.ByteSize()
	}
	return newEntry
}

func (mc *MutableCatalog) ensureForName(key catalog.NameKey) *byNameEntry {
	mc.maybeInitialize()
	newEntry := &byNameEntry{
		parentID:       key.GetParentID(),
		parentSchemaID: key.GetParentSchemaID(),
		name:           key.GetName(),
	}
	if replaced := mc.byName.upsert(newEntry); replaced != nil {
		*newEntry = *(replaced.(*byNameEntry))
	} else {
		mc.byteSize += newEntry.ByteSize()
	}
	return newEntry
}

// UpsertDescriptorEntry adds a descriptor to the MutableCatalog.
func (mc *MutableCatalog) UpsertDescriptorEntry(desc catalog.Descriptor) {
	if desc == nil || desc.GetID() == descpb.InvalidID {
		return
	}
	e := mc.ensureForID(desc.GetID())
	mc.byteSize -= e.ByteSize()
	e.desc = desc
	mc.byteSize += e.ByteSize()
}

// DeleteDescriptorEntry removes a descriptor from the MutableCatalog.
func (mc *MutableCatalog) DeleteDescriptorEntry(id descpb.ID) {
	if id == descpb.InvalidID || !mc.IsInitialized() {
		return
	}
	if removed := mc.byID.delete(id); removed != nil {
		mc.byteSize -= removed.(catalogEntry).ByteSize()
	}
}

// UpsertNamespaceEntry adds a name -> id mapping to the MutableCatalog.
func (mc *MutableCatalog) UpsertNamespaceEntry(
	key catalog.NameKey, id descpb.ID, mvccTimestamp hlc.Timestamp,
) {
	if key == nil || id == descpb.InvalidID {
		return
	}
	e := mc.ensureForName(key)
	e.id = id
	e.timestamp = mvccTimestamp
}

// DeleteNamespaceEntry removes a name -> id mapping from the MutableCatalog.
func (mc *MutableCatalog) DeleteNamespaceEntry(key catalog.NameKey) {
	if key == nil || !mc.IsInitialized() {
		return
	}
	if removed := mc.byName.delete(key); removed != nil {
		mc.byteSize -= removed.(catalogEntry).ByteSize()
	}
}

// UpsertComment upserts a ((ObjectID, SubID, CommentType) -> Comment) mapping
// into the catalog.
func (mc *MutableCatalog) UpsertComment(key catalogkeys.CommentKey, cmt string) {
	e := mc.ensureForID(descpb.ID(key.ObjectID))
	mc.byteSize -= e.ByteSize()
	c := &e.comments[key.CommentType]
	if ordinal, found := c.subObjectOrdinals.Get(int(key.SubID)); found {
		c.comments[ordinal] = cmt
	} else {
		c.subObjectOrdinals.Set(int(key.SubID), len(c.comments))
		c.comments = append(c.comments, cmt)
	}
	mc.byteSize += e.ByteSize()
}

// UpsertZoneConfig upserts a (descriptor id -> zone config) mapping into the
// catalog.
func (mc *MutableCatalog) UpsertZoneConfig(
	id descpb.ID, zoneConfig *zonepb.ZoneConfig, rawBytes []byte,
) {
	e := mc.ensureForID(id)
	mc.byteSize -= e.ByteSize()
	e.zc = zone.NewZoneConfigWithRawBytes(zoneConfig, rawBytes)
	mc.byteSize += e.ByteSize()
}

// FilterByIDs returns a subset of the catalog only for the desired IDs.
func (mc *MutableCatalog) FilterByIDs(ids []descpb.ID) Catalog {
	if !mc.IsInitialized() {
		return Catalog{}
	}
	var ret MutableCatalog
	for _, id := range ids {
		found := mc.byID.get(id)
		if found == nil {
			continue
		}
		e := ret.ensureForID(id)
		*e = *found.(*byIDEntry)
	}
	return ret.Catalog
}

// FilterByNames returns a subset of the catalog only for the desired names.
func (mc *MutableCatalog) FilterByNames(nameInfos []descpb.NameInfo) Catalog {
	if !mc.IsInitialized() {
		return Catalog{}
	}
	var ret MutableCatalog
	for _, ni := range nameInfos {
		found := mc.byName.getByName(ni.ParentID, ni.ParentSchemaID, ni.Name)
		if found == nil {
			continue
		}
		e := ret.ensureForName(&ni)
		*e = *found.(*byNameEntry)
	}
	return ret.Catalog
}

// AddAll adds the contents of the provided catalog to this one.
func (mc *MutableCatalog) AddAll(c Catalog) {
	if !c.IsInitialized() {
		return
	}
	_ = c.byName.ascend(func(entry catalog.NameEntry) error {
		e := mc.ensureForName(entry)
		mc.byteSize -= e.ByteSize()
		*e = *(entry.(*byNameEntry))
		mc.byteSize += e.ByteSize()
		return nil
	})
	_ = c.byID.ascend(func(entry catalog.NameEntry) error {
		e := mc.ensureForID(entry.GetID())
		mc.byteSize -= e.ByteSize()
		*e = *(entry.(*byIDEntry))
		mc.byteSize += e.ByteSize()
		return nil
	})
}
