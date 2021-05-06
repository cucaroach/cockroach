- Feature Name: Distributed Token Bucket
- Status: draft
- Start Date: 2021-04-29
- Authors: Tommy Reilly, Radu Berinde
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: ?? (only have a JIRA?)

# Summary

Our database can distribute work across many processes and machines in flexible ways. From remote distsql execution to multi-tenant setups where each SQL tenant runs in their own process utilizing a shared kvserver. In order to measure and limit the work done by a single client there needs to be a way to track resource usage across these different processes and machines. This RFC describes a distributed token bucket that can be utilized to track resource consumption, enable and constrain bursts of activity and limit resource consumption for cost control and congestion management purposes. 

# Motivation

Measuring work is important so customers can assess how computational tasks incur real world costs, whether that is electricity and hardware costs for self hosted deployments or a cloud provider monthly bills. In addition some workloads are more important than others and its desirable to place limits on how much resources certain workloads can consume. Furthermore the current system lacks controls to prevent resource heavy workloads from saturating parts of the system leading to destabilization and availability outages. Rate limiting to maintain overhead resource availability overhead

# Technical design

The fundamental notion is that the bucket is refilled at a fixed rate corresponding to the expected steady state resource consumption and the bucket is sized according to the desired ability to handle bursts in traffic. So important line of business workloads may be given a high refill rate and a big bucket while lesser workloads can be configured with a low refill rate and a small bucket size. 

Tokens are abstract unitless values meant to roughly approximate CPU, memory and I/O costs, they do not incorporate at rest storage costs.  How these resources are combined and equated to tokens is 

Audience: CockroachDB team members, expert users.

## Drawbacks

...

## Rationale and Alternatives

...

# Explain it to folk outside of your team

Audience: PMs, doc writers, end-users, CockroachDB team members in other areas of the project.

# Unresolved questions

Audience: all participants to the RFC review.
