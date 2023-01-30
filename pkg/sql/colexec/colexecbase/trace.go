// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TODO(cucaroach): maybe move to row and use there too.
type tracePutter struct {
	p   row.Putter
	ctx context.Context
}

var _ row.Putter = &tracePutter{}

func (t *tracePutter) CPut(key, value interface{}, expValue []byte) {
	log.VEventfDepth(t.ctx, 1, 2, "CPut %v -> %v", key, value)
	t.p.CPut(key, value, expValue)
}

func (t *tracePutter) Put(key, value interface{}) {
	log.VEventfDepth(t.ctx, 1, 2, "Put %v -> %v", key, value)
	t.p.Put(key, value)
}
func (t *tracePutter) InitPut(key, value interface{}, failOnTombstones bool) {
	log.VEventfDepth(t.ctx, 1, 2, "InitPut %v -> %v", key, value)
	t.p.Put(key, value)

}
func (t *tracePutter) Del(key ...interface{}) {
	log.VEventfDepth(t.ctx, 1, 2, "Del %v", key)
	t.p.Del(key...)
}

func (t *tracePutter) CPutValues(kys []roachpb.Key, values []roachpb.Value) {
	for i, k := range kys {
		log.VEventfDepth(t.ctx, 1, 2, "CPut %v -> %s", k, values[i].PrettyPrint())
	}
	t.p.CPutValues(kys, values)

}

func (t *tracePutter) CPutTuples(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if k != nil {
			var v roachpb.Value
			v.SetTuple(values[i])
			log.VEventfDepth(t.ctx, 1, 2, "CPut %v -> %v", k, v.PrettyPrint())
		}
	}
	t.p.CPutTuples(kys, values)

}
func (t *tracePutter) PutBytes(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if k != nil {
			var v roachpb.Value
			v.SetBytes(values[i])
			log.VEventfDepth(t.ctx, 1, 2, "Put %v -> %v", k, v.PrettyPrint())
		}
	}
	t.p.PutBytes(kys, values)

}
func (t *tracePutter) InitPutBytes(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if k != nil {
			var v roachpb.Value
			v.SetBytes(values[i])
			log.VEventfDepth(t.ctx, 1, 2, "InitPut %v -> %v", k, v.PrettyPrint())
		}
	}
	t.p.InitPutBytes(kys, values)
}

func (t *tracePutter) PutTuples(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if k != nil {
			var v roachpb.Value
			v.SetBytes(values[i])
			log.VEventfDepth(t.ctx, 1, 2, "Put %v -> %v", k, v.PrettyPrint())
		}
	}
	t.p.PutTuples(kys, values)

}
func (t *tracePutter) InitPutTuples(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if k != nil {
			var v roachpb.Value
			v.SetBytes(values[i])
			log.VEventfDepth(t.ctx, 1, 2, "InitPut %v -> %v", k, v.PrettyPrint())
		}
	}
	t.p.InitPutTuples(kys, values)
}
