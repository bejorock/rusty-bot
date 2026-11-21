---
id: durable-temporal-session-storage
type: interface
status: proposed
producer: session storage SDK
consumer: Temporal engine, PostgreSQL adapter and caller applications
version: 1
created: 2026-11-21T07:23:13.114Z
updated: 2026-11-21T07:23:16.848Z
---

# Durable Temporal session storage v1

Scope: text-only sessions; no tools or changes to the local engine. Application configures and injects Arc<dyn SessionStorage>; PostgreSQL pool/credentials belong to caller, never Workflow input/history.

## Identity and authority
Application Session ID is an opaque string independent of Temporal identity. Storage assigns a stable random Workflow ID on session creation; Temporal may create successive runs under that ID after idle/explicit completion. Creation is atomic; existing model/config must match. One open execution per mapped Workflow ID is enforced by Temporal UseExisting conflict policy. A worker load Activity claims its Run ID as storage ownership fence; stale-run draft/finish writes fail. Clients must use a consistent namespace and shared task queue for the same store; binding recorded on creation and mismatches rejected.

## Storage contract
SessionStorage asynchronous operations: create/get session; load/claim run; begin turn(request ID,text); write cumulative draft(sequence); finish turn(reply or error). Transactions serialize per-session mutations. Request IDs deduplicate retries and cannot be reused for different text. Turn numbers increase atomically. Completed turn writes are idempotent. Drafts ignore stale sequences and cannot replace final results. Completed messages are used for model restoration, failed/in-progress drafts are excluded. Interrupted prior-run drafts are marked failed on claim; persisted completed results remain intact.

## Workflow lifecycle
Workflow loads/claims session in Activity before accepting turns. Database I/O occurs only in Activities, never deterministic Workflow code. Begin/persist/finish Activities retry safely; model generation remains single-attempt. A turn is acknowledged only after final storage commit. Inactivity is measured between turns with a durable timer and refreshed after admitted turns. Closure waits active handlers. Reopening starts/attaches mapped Workflow with no chain lifetime timeout, loads state through Activity. Continue-As-New is a separate future history-management feature, not implemented in this increment.

## Reading/streaming
Session.snapshot reads shared storage, not Workflow Query; includes cumulative draft and Failed status. Progress batches at 100ms and writes via Activity worker (not per-token Workflow events); existing partial Signal/Query compatibility may remain during migration. Storage failure prevents successful acknowledgement. Model credentials, database credentials and connection pools stay outside serialized Workflow state.

## Validation
Unit tests with memory adapter; isolated PostgreSQL contract tests verify concurrent create, request conflict/idempotency, sequence ordering, stale-owner fencing. Existing remote Temporal tests plus PostgreSQL lifecycle tests prove idle completion/restart restoration, storage draft visibility, and fresh-worker recovery without re-running completed model Activities. Only newly created dedicated database is migrated; no existing database schema modified. No automatic database deletion.