// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colenc

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// MarshalLegacy produces the value encoding of the given datum (constrained by
// the given column type) into a roachpb.Value, using the legacy version 1
// encoding (see docs/tech-notes/encoding.md).
//
// This encoding is used when when the table format does not use column
// families, such as pre-2.0 tables and some system tables.
//
// If val's type is incompatible with colType, or if colType is not yet
// implemented by this function, an error is returned.
// This is copied from pkg/sql/rowenc/valueside/legacy.go
func MarshalLegacy(colType *types.T, vec coldata.Vec, row int) (roachpb.Value, error) {
	var r roachpb.Value

	if vec.Nulls().NullAt(row) {
		return r, nil
	}

	switch colType.Family() {
	case types.BoolFamily:
		if vec.Type().Family() == types.BoolFamily {
			r.SetBool(vec.Bool()[row])
			return r, nil
		}
	case types.IntFamily, types.DateFamily:
		if vec.Type().Family() == types.IntFamily {
			r.SetInt(vec.Int64()[row])
			return r, nil
		}
	case types.FloatFamily:
		if vec.Type().Family() == types.FloatFamily {
			r.SetFloat(vec.Float64()[row])
			return r, nil
		}
	case types.DecimalFamily:
		if vec.Type().Family() == types.DecimalFamily {
			err := r.SetDecimal(&vec.Decimal()[row])
			return r, err
		}
	case types.StringFamily, types.BytesFamily, types.UuidFamily:
		switch vec.Type().Family() {
		case types.StringFamily, types.BytesFamily, types.UuidFamily:
			b := vec.Bytes().Get(row)
			r.SetString(*(*string)(unsafe.Pointer(&b)))
			return r, nil
		}
	case types.TimestampFamily, types.TimestampTZFamily:
		switch vec.Type().Family() {
		case types.TimestampFamily, types.TimestampTZFamily:
			t := vec.Timestamp().Get(row)
			r.SetTime(t)
			return r, nil
		}
	case types.IntervalFamily:
		if vec.Type().Family() == types.IntervalFamily {
			err := r.SetDuration(vec.Interval()[row])
			return r, err
		}
	case types.JsonFamily:
		if vec.Type().Family() == types.JsonFamily {
			j := vec.JSON().Get(row)
			data, err := json.EncodeJSON(nil, j)
			if err != nil {
				return r, err
			}
			r.SetBytes(data)
			return r, nil
		}
	default:
		return valueside.MarshalLegacy(colType, vec.Datum().Get(row).(tree.Datum))
	}
	return r, errors.AssertionFailedf("mismatched type %q vs %q", vec.Type(), colType.Family())
}
