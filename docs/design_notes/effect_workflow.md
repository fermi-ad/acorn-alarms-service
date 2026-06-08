# Effect Workflow Design Note

This note records the intended boundary between domain decision-making and side-effect execution in the alarms service.

## Current shape

The runtime is intentionally split into four responsibilities:

- adapters translate external protocols into internal messages
- ingress applies queueing and overload policy
- the coordinator decides domain state and reconciliation
- effect executors perform transport-facing work and report results back

Today the only effect is publish, but the architecture is being prepared for additional side effects such as bypass-related service calls.

## Core rule

The coordinator is the only writer of domain state.

Effect executors must never mutate alarm state directly. They may only:

- execute requested work
- preserve the original effect identity
- emit result messages back to the coordinator

This keeps correctness reasoning in one place even when transport behavior is concurrent, retried, or reordered.

## Decision versus execution

A user or automated input should be understood as producing three distinct things:

1. a domain decision
2. an execution plan
3. a reconciliation policy

### 1. Domain decision

The coordinator decides:

- whether the input is allowed
- what the latest desired state is for the alarm key
- whether the input should replace prior speculative intent

### 2. Execution plan

The coordinator emits one or more effects describing work to perform.

Examples:

- publish the new alarm state
- call a future bypass integration
- notify another downstream system

Effects are requests for work, not permission for executors to reinterpret domain rules.

### 3. Reconciliation policy

The coordinator also owns the meaning of returned results:

- which result confirms current intent
- which result is stale and should be ignored for state removal
- which failures should fail a waiting user confirmation
- which successes may still advance confirmed state

## When adding more side effects

Before implementing a new side effect, decide whether it is:

- an independent consequence of a domain decision, or
- an ordered workflow step whose result changes the meaning of later steps

### Independent effects

If effects are independent, the current pattern can continue:

- coordinator emits multiple effects
- each executor reports results independently
- coordinator reconciles each result by effect identity

### Ordered workflow effects

If effects are ordered, model that ordering explicitly.

Examples of workflow questions:

- does bypass confirmation wait for publish only, or also for the external bypass service
- if publish succeeds but the bypass service fails, what should the user observe
- if a newer command supersedes an older one, which in-flight workflow results still matter

Do not hide workflow sequencing inside ad hoc branching in the coordinator or inside effect executors.

## Recommended extension pattern

When the next side effect is added, preserve these constraints:

- adapters translate only
- ingress queues only
- coordinator decides only
- executors execute only
- all reconciliation returns through result messages

A useful mental model is:

1. ingress receives input
2. coordinator decides desired state and emits effect plan
3. executors perform work concurrently
4. executors return result events with stable identities
5. coordinator reconciles those results against current intent

## Confirmation semantics

User confirmation semantics must be explicit whenever multiple side effects exist.

For each user command, define:

- what counts as accepted
- what counts as completed
- which downstream failures are user-visible
- whether confirmation waits for one effect or all required effects

Without this, the code will drift toward transport-specific behavior instead of domain-specific behavior.

## Why this note exists

The service already relies on monotonic ids, speculative state, and stale-result reconciliation. Those mechanisms are strong enough to support more side effects, but only if the single-writer rule is preserved.

This note exists to keep future changes from smearing domain logic across adapters, executors, and transport callbacks.