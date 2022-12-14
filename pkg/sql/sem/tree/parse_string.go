// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ParseAndRequireString parses s as type t for simple types. Collated
// strings are not handled.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseContext (either for the time or the local timezone).
func ParseAndRequireString(
	t *types.T, s string, ctx ParseContext,
) (d Datum, dependsOnContext bool, err error) {
	switch t.Family() {
	case types.ArrayFamily:
		d, dependsOnContext, err = ParseDArrayFromString(ctx, s, t.ArrayContents())
	case types.BitFamily:
		r, err := ParseDBitArray(s)
		if err != nil {
			return nil, false, err
		}
		d = FormatBitArrayToType(r, t)
	case types.BoolFamily:
		d, err = ParseDBool(strings.TrimSpace(s))
	case types.BytesFamily:
		d, err = ParseDByte(s)
	case types.DateFamily:
		d, dependsOnContext, err = ParseDDate(ctx, s)
	case types.DecimalFamily:
		d, err = ParseDDecimal(strings.TrimSpace(s))
	case types.FloatFamily:
		d, err = ParseDFloat(strings.TrimSpace(s))
	case types.INetFamily:
		d, err = ParseDIPAddrFromINetString(s)
	case types.IntFamily:
		d, err = ParseDInt(strings.TrimSpace(s))
	case types.IntervalFamily:
		itm, typErr := t.IntervalTypeMetadata()
		if typErr != nil {
			return nil, false, typErr
		}
		d, err = ParseDIntervalWithTypeMetadata(intervalStyle(ctx), s, itm)
	case types.Box2DFamily:
		d, err = ParseDBox2D(s)
	case types.GeographyFamily:
		d, err = ParseDGeography(s)
	case types.GeometryFamily:
		d, err = ParseDGeometry(s)
	case types.JsonFamily:
		d, err = ParseDJSON(s)
	case types.OidFamily:
		if t.Oid() != oid.T_oid && s == ZeroOidValue {
			d = WrapAsZeroOid(t)
		} else {
			d, err = ParseDOidAsInt(s)
		}
	case types.CollatedStringFamily:
		d, err = NewDCollatedString(s, t.Locale(), ctx.GetCollationEnv())
	case types.StringFamily:
		s = truncateString(s, t)
		return NewDString(s), false, nil
	case types.TimeFamily:
		d, dependsOnContext, err = ParseDTime(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimeTZFamily:
		d, dependsOnContext, err = ParseDTimeTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimestampFamily:
		d, dependsOnContext, err = ParseDTimestamp(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.TimestampTZFamily:
		d, dependsOnContext, err = ParseDTimestampTZ(ctx, s, TimeFamilyPrecisionToRoundDuration(t.Precision()))
	case types.UuidFamily:
		d, err = ParseDUuidFromString(s)
	case types.EnumFamily:
		var e DEnum
		e, err = MakeDEnumFromLogicalRepresentation(t, s)
		if err == nil {
			d = NewDEnum(e)
		}
	case types.TSQueryFamily:
		d, err = ParseDTSQuery(s)
	case types.TSVectorFamily:
		d, err = ParseDTSVector(s)
	case types.TupleFamily:
		d, dependsOnContext, err = ParseDTupleFromString(ctx, s, t)
	case types.VoidFamily:
		d = DVoidDatum
	default:
		return nil, false, errors.AssertionFailedf("unknown type %s (%T)", t, t)
	}
	if err != nil {
		return d, dependsOnContext, err
	}
	d, err = AdjustValueToType(t, d)
	return d, dependsOnContext, err
}

func truncateString(s string, t *types.T) string {
	// If the string type specifies a limit we truncate to that limit:
	//   'hello'::CHAR(2) -> 'he'
	// This is true of all the string type variants.
	if t.Width() > 0 {
		s = util.TruncateString(s, int(t.Width()))
	}
	return s
}

// ParseDOidAsInt parses the input and returns it as an OID. If the input
// is not formatted as an int, an error is returned.
func ParseDOidAsInt(s string) (*DOid, error) {
	i, err := strconv.ParseInt(strings.TrimSpace(s), 0, 64)
	if err != nil {
		return nil, MakeParseError(s, types.Oid, err)
	}
	return IntToOid(DInt(i))
}

// FormatBitArrayToType formats bit arrays such that they fill the total width
// if too short, or truncate if too long.
func FormatBitArrayToType(d *DBitArray, t *types.T) *DBitArray {
	if t.Width() == 0 || d.BitLen() == uint(t.Width()) {
		return d
	}
	a := d.BitArray.Clone()
	switch t.Oid() {
	case oid.T_varbit:
		// VARBITs do not have padding attached, so only truncate.
		if uint(t.Width()) < a.BitLen() {
			a = a.ToWidth(uint(t.Width()))
		}
	default:
		a = a.ToWidth(uint(t.Width()))
	}
	return &DBitArray{a}
}

// ValueHandler is an interface to allow raw types to extracted from strings.
type ValueHandler interface {
	Null()
	Date(d pgdate.Date)
	Datum(d Datum)
	Bool(b bool)
	Bytes(b []byte)
	Decimal() *apd.Decimal
	Float(f float64)
	Int(i int64)
	Duration(d duration.Duration)
	JSON(j json.JSON)
	String(s string)
	TimestampTZ(t time.Time)
}

func ParseAndRequireStringEx(t *types.T, s string, ctx ParseTimeContext, vh ValueHandler, ph *pgdate.ParseHelper) (err error) {
	switch t.Family() {
	case types.BoolFamily:
		var b bool
		if b, err = ParseBool(strings.TrimSpace(s)); err == nil {
			vh.Bool(b)
		}
	case types.BytesFamily:
		var res []byte
		if res, err = lex.DecodeRawBytesToByteArrayAuto([]byte(s)); err != nil {
			vh.Bytes(res)
		} else {
			err = MakeParseError(s, types.Bytes, err)
		}
	case types.DateFamily:
		now := relativeParseTime(ctx)
		var t pgdate.Date
		if t, _, err = pgdate.ParseDate(now, dateStyle(ctx), s, ph); err == nil {
			vh.Date(t)
		}
	case types.DecimalFamily:
		dec := vh.Decimal()
		if err = setDecimalString(s, dec); err != nil {
			// Erase any invalid results.
			*dec = apd.Decimal{}
			err = MakeParseError(s, types.Decimal, err)
		}
	case types.FloatFamily:
		var f float64
		if f, err = strconv.ParseFloat(s, 64); err == nil {
			vh.Float(f)
		} else {
			err = MakeParseError(s, types.Float, err)
		}
	case types.IntFamily:
		var i int64
		if i, err = strconv.ParseInt(s, 0, 64); err == nil {
			vh.Int(i)
		} else {
			err = MakeParseError(s, types.Int, err)
		}
	case types.JsonFamily:
		var j json.JSON
		if j, err = json.ParseJSON(s); err == nil {
			vh.JSON(j)
		} else {
			err = pgerror.Wrapf(err, pgcode.Syntax, "could not parse JSON")
		}
	case types.StringFamily:
		s = truncateString(s, t)
		vh.String(s)
	case types.TimestampTZFamily:
		now := relativeParseTime(ctx)
		var ts time.Time
		if ts, _, err = pgdate.ParseTimestamp(now, dateStyle(ctx), s); err == nil {
			// Always normalize time to the current location.
			if ts, err = checkTimeBounds(ts, TimeFamilyPrecisionToRoundDuration(t.Precision())); err == nil {
				vh.TimestampTZ(ts)
			}
		}
	default:
		var d Datum
		if d, _, err = ParseAndRequireString(t, s, ctx); err == nil {
			vh.Datum(d)
		}
	}
	return err
}
