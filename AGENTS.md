# Repository working agreement

## Scope

- Read README.md and the relevant specification before changing behavior.
- The original three-step MVP, A12 and later upload/GIF/cancellation plus six-step DAG/cache enhancements have recorded acceptance. Preserve old frozen tasks and accepted recovery behavior; use same-origin static HTML/CSS/JS, not a full frontend project. Historical implementation records are not promises to retain retired product entry points.
- The subsequently authorized lightweight monitoring console adds real global counts, searchable/paged task queues, dependency views and detail tabs on that same frontend. Monitoring reads must not drive execution, fabricate attempts/progress or force-release unconfirmed resources; this is not authorization for an account system or full operations platform.
- Current product scope is monitoring and task management: no frontend upload/create controls. New public submissions may use managed-v1 for business-registered sequential steps; animation-dag-v2 remains the default example/regression, and animation-v1 is retired for new keys, not deleted from history. Dynamic registration is immutable/idempotent intent, not execution authorization; an open workflow waits for explicit finish and must not succeed merely because currently registered work completed. Preserve frozen legacy execution/recovery and matching old-key replay, plus backend upload/media compatibility.
- Subsequently authorized step-level hosting keeps central orchestration/fencing authoritative. Preserve local, bundled external and independently implemented external backends, dynamic sequential registration and historical frozen workflows. The latest approved slice adds preconfigured nodes with separate credentials/capabilities/admission, per-node CPU quotas and multiple MOCK devices, verified remote HTTPS and actual shared-reference checks. No automatic discovery, new scheduler service, arbitrary job adoption/plugins or real models/GPU. Bind immutable commands and exit receipts to the original node, device and state-store identity; a lost/replaced store is not proof of physical exit. Real cross-device deployment requires supplied machines, permissions and mounts, not invented acceptance.
- No arbitrary-code plugins, workflow editor, distributed cache platform or external publishing service. The new DAG/cache scope does not authorize real models/GPU or public deployment. Keep recovery and resource cleanup grounded in real database/process evidence; do not equate a lost lease with physical process exit.
- Follow docs/ROADMAP.md. Do not add Kubernetes, Redis, an arbitrary DAG editor, multi-tenant billing or real model integration without a concrete approved requirement.
- Do not spawn subagents unless the user explicitly authorizes delegation.

## Files and tools

- Prefer FastCtx inspect_local_file / grep / glob for local inspection when available; use replace for mechanical replacement and apply_patch for generated content or semantic edits.
- On Windows use PowerShell 7+ (pwsh), never Windows PowerShell 5.1 or compatibility remoting.
- Do not use browser automation for backend-only changes. Prefer focused tests and direct API evidence.
- Keep personal configuration, secrets, real user uploads, runtime data and local logs out of Git.
- Do not alter other repositories or global Git configuration.

## Bounded implementation and verification

- Use the simplest complete solution for the current requirement. Do not introduce frameworks, abstractions, extension points or unrelated refactors for hypothetical future needs.
- Every added mechanism or test must protect a stated requirement, a directly affected guarantee, a supported trust boundary or a known regression. Do not expand work into a generic defensive matrix.
- Verify changed behavior and directly affected callers, data and recovery paths. Keep mandatory project checks, explicit acceptance requirements and known-regression tests.
- Reuse successful verification when the relevant inputs and final state have not changed. Documentation-only edits do not require rerunning unrelated runtime scenarios.
- Do not add checksum manifests, proof files or verification scripts solely to demonstrate completion. Preserve fingerprints and integrity mechanisms that have an actual protocol or consumer.
- Persistence, execution history, lease renewal, fencing, checkpoint compatibility, GPU cleanup and required security boundaries are in-scope guarantees, not optional overengineering.
- Finish when the requested result and required verification are complete and no known in-scope blocker remains. State untested boundaries honestly; do not add speculative hardening, repeated reviews or extra checklists to prolong the task.

## Execution guarantees

- Heartbeat/lease settings are not model execution time limits. A healthy long task renews its lease.
- Default hard attempt timeout and task deadline are disabled. Optional hard limits must be explicit in the immutable task configuration.
- Keep heartbeat/supervision independent of the model execution subprocess, and detect child exit instead of renewing a dead computation forever.
- Fence every executor-originated authoritative state, checkpoint and artifact commit by current Attempt, execution generation and valid lease. Cache admission and Task-level submit/resume/cancel transitions use Task-row transactions and cannot masquerade as executor completion.
- A lost lease is not proof that a GPU process stopped. Quarantine uncertain resources until reconciled.
- Never claim exactly-once computation or arbitrary model-internal checkpoint recovery.

## Validation and documentation

- Run checks for changed behavior and directly affected recovery guarantees, not an unrelated defensive matrix.
- Keep state transitions, APIs and timeout semantics aligned with docs/SPEC.md.
- Distinguish planned behavior, static checks, MOCK acceptance and real infrastructure verification.
- Update docs/DEVELOPMENT_LOG.md with actual observations; do not invent token counts, human hours or failure stories.
- Only publish content within the user's requested scope. Do not add a license without the user's choice.
