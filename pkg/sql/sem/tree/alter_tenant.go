// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// AlterTenantReplication represents an ALTER TENANT REPLICATION statement.
type AlterTenantReplication struct {
	TenantName Expr
	Command    JobCommand
}

var _ Statement = &AlterTenantReplication{}

// Format implements the NodeFormatter interface.
func (n *AlterTenantReplication) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TENANT ")
	ctx.FormatNode(n.TenantName)
	ctx.WriteByte(' ')
	if n.Command == PauseJob || n.Command == ResumeJob {
		ctx.WriteString(JobCommandToStatement[n.Command])
		ctx.WriteByte(' ')
	}
	ctx.WriteString("REPLICATION")
}
