package vec

import (
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

func MakeHandler(t *types.T, size int) tree.ValueHandler {
	switch t {
	case types.Int:
		return &intHandler{i: make([]int64, size)}
	case types.String:
		return &stringHandler{s: make([]string, size)}
	case types.Date:
		return &dateHandler{d: make([]int64, size)}
	case types.Decimal:
		return &decHandler{d: make([]apd.Decimal, size)}
	}
	return nil
}

type datumHandler struct {
	d   tree.Datums
	row int
}

func (v *datumHandler) Datum(d tree.Datum) {
	v.d[v.row] = d
	v.row++
}

func (v *datumHandler) Reset() {
	v.row = 0
}

func (v *datumHandler) Datums() tree.Datums {
	return v.d
}
func (v *datumHandler) Bools() []bool           { panic("not implemented") }
func (v *datumHandler) Ints() []int64           { panic("not implemented") }
func (v *datumHandler) Floats() []float64       { panic("not implemented") }
func (v *datumHandler) Strings() []string       { panic("not implemented") }
func (v *datumHandler) Decimals() []apd.Decimal { panic("not implemented") }

func (v *datumHandler) Date(d pgdate.Date)           { panic("not implemented") }
func (v *datumHandler) Bool(b bool)                  { panic("not implemented") }
func (v *datumHandler) Bytes(b []byte)               { panic("not implemented") }
func (v *datumHandler) Decimal() *apd.Decimal        { panic("not implemented") }
func (v *datumHandler) Float(f float64)              { panic("not implemented") }
func (v *datumHandler) Int(i int64)                  { panic("not implemented") }
func (v *datumHandler) Duration(d duration.Duration) { panic("not implemented") }
func (v *datumHandler) JSON(j json.JSON)             { panic("not implemented") }
func (v *datumHandler) String(s string)              { panic("not implemented") }
func (v *datumHandler) TimestampTZ(t time.Time)      { panic("not implemented") }

var _ tree.ValueHandler = &datumHandler{}

type intHandler struct {
	i   []int64
	row int
}

func (v *intHandler) Int(i int64) {
	v.i[v.row] = i
	v.row++
}

func (v *intHandler) Reset() {
	v.row = 0
}

func (v *intHandler) Ints() []int64 {
	return v.i
}

func (v *intHandler) Bools() []bool                { panic("not implemented") }
func (v *intHandler) Datums() tree.Datums          { panic("not implemented") }
func (v *intHandler) Floats() []float64            { panic("not implemented") }
func (v *intHandler) Strings() []string            { panic("not implemented") }
func (v *intHandler) Decimals() []apd.Decimal      { panic("not implemented") }
func (v *intHandler) Date(d pgdate.Date)           { panic("not implemented") }
func (v *intHandler) Datum(d tree.Datum)           { panic("not implemented") }
func (v *intHandler) Bool(b bool)                  { panic("not implemented") }
func (v *intHandler) Bytes(b []byte)               { panic("not implemented") }
func (v *intHandler) Decimal() *apd.Decimal        { panic("not implemented") }
func (v *intHandler) Float(f float64)              { panic("not implemented") }
func (v *intHandler) Duration(d duration.Duration) { panic("not implemented") }
func (v *intHandler) JSON(j json.JSON)             { panic("not implemented") }
func (v *intHandler) String(s string)              { panic("not implemented") }
func (v *intHandler) TimestampTZ(t time.Time)      { panic("not implemented") }

var _ tree.ValueHandler = &intHandler{}

type decHandler struct {
	d   []apd.Decimal
	row int
}

func (v *decHandler) Reset() {
	v.row = 0
}

func (v *decHandler) Decimals() []apd.Decimal {
	return v.d
}

// TODO: can we in place construct Decimal?
func (v *decHandler) Decimal() *apd.Decimal {
	d := &v.d[v.row]
	v.row++
	return d
}

func (v *decHandler) Bools() []bool                { panic("not implemented") }
func (v *decHandler) Ints() []int64                { panic("not implemented") }
func (v *decHandler) Datums() tree.Datums          { panic("not implemented") }
func (v *decHandler) Floats() []float64            { panic("not implemented") }
func (v *decHandler) Strings() []string            { panic("not implemented") }
func (v *decHandler) Date(d pgdate.Date)           { panic("not implemented") }
func (v *decHandler) Datum(d tree.Datum)           { panic("not implemented") }
func (v *decHandler) Bool(b bool)                  { panic("not implemented") }
func (v *decHandler) Bytes(b []byte)               { panic("not implemented") }
func (v *decHandler) Float(f float64)              { panic("not implemented") }
func (v *decHandler) Int(i int64)                  { panic("not implemented") }
func (v *decHandler) Duration(d duration.Duration) { panic("not implemented") }
func (v *decHandler) JSON(j json.JSON)             { panic("not implemented") }
func (v *decHandler) String(s string)              { panic("not implemented") }
func (v *decHandler) TimestampTZ(t time.Time)      { panic("not implemented") }

var _ tree.ValueHandler = &decHandler{}

type dateHandler struct {
	d   []int64
	row int
}

func (v *dateHandler) Reset() {
	v.row = 0
}

func (v *dateHandler) Date(d pgdate.Date) {
	v.d[v.row] = d.UnixEpochDaysWithOrig()
	v.row++
}

func (v *dateHandler) Ints() []int64 {
	return v.d
}
func (v *dateHandler) Bools() []bool                { panic("not implemented") }
func (v *dateHandler) Datums() tree.Datums          { panic("not implemented") }
func (v *dateHandler) Floats() []float64            { panic("not implemented") }
func (v *dateHandler) Strings() []string            { panic("not implemented") }
func (v *dateHandler) Decimals() []apd.Decimal      { panic("not implemented") }
func (v *dateHandler) Datum(d tree.Datum)           { panic("not implemented") }
func (v *dateHandler) Bool(b bool)                  { panic("not implemented") }
func (v *dateHandler) Bytes(b []byte)               { panic("not implemented") }
func (v *dateHandler) Decimal() *apd.Decimal        { panic("not implemented") }
func (v *dateHandler) Float(f float64)              { panic("not implemented") }
func (v *dateHandler) Int(i int64)                  { panic("not implemented") }
func (v *dateHandler) Duration(d duration.Duration) { panic("not implemented") }
func (v *dateHandler) JSON(j json.JSON)             { panic("not implemented") }
func (v *dateHandler) String(s string)              { panic("not implemented") }
func (v *dateHandler) TimestampTZ(t time.Time)      { panic("not implemented") }

var _ tree.ValueHandler = &dateHandler{}

type stringHandler struct {
	s   []string
	row int
}

func (v *stringHandler) String(s string) {
	v.s[v.row] = s
	v.row++
}
func (v *stringHandler) Reset() {
	v.row = 0
}

func (v *stringHandler) Strings() []string {
	return v.s
}
func (v *stringHandler) Ints() []int64                { panic("not implemented") }
func (v *stringHandler) Bools() []bool                { panic("not implemented") }
func (v *stringHandler) Datums() tree.Datums          { panic("not implemented") }
func (v *stringHandler) Floats() []float64            { panic("not implemented") }
func (v *stringHandler) Decimals() []apd.Decimal      { panic("not implemented") }
func (v *stringHandler) Date(d pgdate.Date)           { panic("not implemented") }
func (v *stringHandler) Datum(d tree.Datum)           { panic("not implemented") }
func (v *stringHandler) Bool(b bool)                  { panic("not implemented") }
func (v *stringHandler) Bytes(b []byte)               { panic("not implemented") }
func (v *stringHandler) Decimal() *apd.Decimal        { panic("not implemented") }
func (v *stringHandler) Float(f float64)              { panic("not implemented") }
func (v *stringHandler) Int(i int64)                  { panic("not implemented") }
func (v *stringHandler) Duration(d duration.Duration) { panic("not implemented") }
func (v *stringHandler) JSON(j json.JSON)             { panic("not implemented") }
func (v *stringHandler) TimestampTZ(t time.Time)      { panic("not implemented") }

var _ tree.ValueHandler = &stringHandler{}

// move to coldataext?
func MakeBatchHandler(b coldata.Batch, col int) tree.ValueHandler {
	return &batchHandler{col: b.ColVec(col)}
}

type batchHandler struct {
	col coldata.Vec
	row int
}

func (v *batchHandler) Ints() []int64           { return v.col.Int64() }
func (v *batchHandler) Datums() tree.Datums     { panic("not implemented") }
func (v *batchHandler) Floats() []float64       { panic("not implemented") }
func (v *batchHandler) Decimals() []apd.Decimal { panic("not implemented") }
func (v *batchHandler) Strings() []string       { panic("not implemented") }
func (v *batchHandler) Bools() []bool           { panic("not implemented") }

func (v *batchHandler) String(s string) {
	v.col.Bytes().Set(v.row, []byte(s))
	v.row++
}
func (v *batchHandler) Reset() {
	v.row = 0
}
func (v *batchHandler) Date(d pgdate.Date) {
	v.col.Int64().Set(v.row, d.UnixEpochDaysWithOrig())
	v.row++
}
func (v *batchHandler) Datum(d tree.Datum) {
	v.col.Datum().Set(v.row, d)
	v.row++
}
func (v *batchHandler) Bool(b bool) {
	v.col.Bool().Set(v.row, b)
	v.row++
}
func (v *batchHandler) Bytes(b []byte) {
	v.col.Bytes().Set(v.row, b)
	v.row++
}
func (v *batchHandler) Decimal() *apd.Decimal {
	d := &v.col.Decimal()[v.row]
	v.row++
	return d
}
func (v *batchHandler) Float(f float64) {
	v.col.Float64().Set(v.row, f)
	v.row++
}
func (v *batchHandler) Int(i int64) {
	v.col.Int64().Set(v.row, i)
	v.row++
}
func (v *batchHandler) Duration(d duration.Duration) {
	v.col.Interval().Set(v.row, d)
	v.row++
}
func (v *batchHandler) JSON(j json.JSON) {
	v.col.JSON().Set(v.row, j)
	v.row++
}
func (v *batchHandler) TimestampTZ(t time.Time) {
	v.col.Timestamp().Set(v.row, t)
	v.row++
}
