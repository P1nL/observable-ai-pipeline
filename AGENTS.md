# Repository working agreement

## Scope

- Read README.md and the relevant specification before changing behavior.
- The current stage is documentation/configuration bootstrap, not a working pipeline.
- Follow docs/ROADMAP.md. Do not add Kubernetes, Redis, an arbitrary DAG editor, multi-tenant billing or real model integration without a concrete approved requirement.
- Do not spawn subagents unless the user explicitly authorizes delegation.

## Files and tools

- Prefer FastCtx inspect_local_file / grep / glob for local inspection when available; use replace for mechanical replacement and apply_patch for generated content or semantic edits.
- On Windows use PowerShell 7+ (pwsh), never Windows PowerShell 5.1 or compatibility remoting.
- Do not use browser automation for backend-only changes. Prefer focused tests and direct API evidence.
- Keep personal configuration, secrets, real user uploads, runtime data and local logs out of Git.
- Do not alter other repositories or global Git configuration.

## Execution guarantees

- Heartbeat/lease settings are not model execution time limits. A healthy long task renews its lease.
- Default hard attempt timeout and task deadline are disabled. Optional hard limits must be explicit in the immutable task configuration.
- Keep heartbeat/supervision independent of the model execution subprocess, and detect child exit instead of renewing a dead computation forever.
- Fence every authoritative state, checkpoint and artifact commit by current Attempt, execution generation and valid lease.
- A lost lease is not proof that a GPU process stopped. Quarantine uncertain resources until reconciled.
- Never claim exactly-once computation or arbitrary model-internal checkpoint recovery.

## Validation and documentation

- Run checks for changed behavior and directly affected recovery guarantees, not an unrelated defensive matrix.
- Keep state transitions, APIs and timeout semantics aligned with docs/SPEC.md.
- Distinguish planned behavior, static checks, MOCK acceptance and real infrastructure verification.
- Update docs/DEVELOPMENT_LOG.md with actual observations; do not invent token counts, human hours or failure stories.
- Only publish content within the user's requested scope. Do not add a license without the user's choice.
