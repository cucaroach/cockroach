// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type virtualDescriptors struct {
	vs catalog.VirtualSchemas
}

func makeVirtualDescriptors(schemas catalog.VirtualSchemas) virtualDescriptors {
	return virtualDescriptors{vs: schemas}
}

func (tc *virtualDescriptors) getSchemaByName(schemaName string) catalog.SchemaDescriptor {
	if tc.vs == nil {
		return nil
	}
	if sc, ok := tc.vs.GetVirtualSchema(schemaName); ok {
		return sc.Desc()
	}
	return nil
}

func (tc *virtualDescriptors) getObjectByName(
	schema string, object string, flags tree.ObjectLookupFlags,
) (isVirtual bool, _ catalog.Descriptor, _ error) {
	if tc.vs == nil {
		return false, nil, nil
	}
	scEntry, ok := tc.vs.GetVirtualSchema(schema)
	if !ok {
		return false, nil, nil
	}
	obj, err := scEntry.GetObjectByName(object, flags)
	if err != nil {
		return true, nil, err
	}
	if obj == nil {
		return true, nil, nil
	}
	if flags.RequireMutable {
		return true, nil, catalog.NewMutableAccessToVirtualObjectError(scEntry, obj)
	}
	return true, obj.Desc(), nil
}

func (tc virtualDescriptors) getByID(
	ctx context.Context, id descpb.ID, mutable bool,
) (catalog.Descriptor, error) {
	if tc.vs == nil {
		return nil, nil
	}
	if vd, found := tc.vs.GetVirtualObjectByID(id); found {
		if mutable {
			vs, found := tc.vs.GetVirtualSchemaByID(vd.Desc().GetParentSchemaID())
			if !found {
				return nil, errors.AssertionFailedf(
					"cannot resolve mutable virtual descriptor %d with unknown parent schema %d",
					id, vd.Desc().GetParentSchemaID(),
				)
			}
			return nil, catalog.NewMutableAccessToVirtualObjectError(vs, vd)
		}
		return vd.Desc(), nil
	}
	return tc.getSchemaByID(ctx, id, mutable)
}

func (tc virtualDescriptors) getSchemaByID(
	ctx context.Context, id descpb.ID, mutable bool,
) (catalog.SchemaDescriptor, error) {
	if tc.vs == nil {
		return nil, nil
	}
	vs, found := tc.vs.GetVirtualSchemaByID(id)
	switch {
	case !found:
		return nil, nil
	case mutable:
		return nil, catalog.NewMutableAccessToVirtualSchemaError(vs.Desc())
	default:
		return vs.Desc(), nil
	}
}

func (tc virtualDescriptors) forEachVirtualObject(
	sc catalog.SchemaDescriptor, fn func(obj catalog.Descriptor),
) {
	scEntry, ok := tc.vs.GetVirtualSchemaByID(sc.GetID())
	if !ok {
		return
	}
	scEntry.VisitTables(func(object catalog.VirtualObject) {
		fn(object.Desc())
	})
}
