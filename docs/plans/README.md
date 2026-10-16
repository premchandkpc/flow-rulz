# Engineering plans

Plans define how an approved change will be delivered. They expose dependencies, ownership, sequence, verification and rollback. The linked spec owns requirements; a plan should not quietly redefine them.

## Documents
- [Change plan template](change-plan-template.md): cross-package, persistence, concurrency, security or operational changes.
- [Legacy architecture redesign plan](legacy-architecture-redesign.md): historical draft; verify every assumption against current source before reuse.
- [Root implementation plan](../IMPLEMENTATION_PLAN.md): historical notes that may have drifted.
- [Roadmap](../roadmap.md): direction/proposals, not evidence of shipped behavior.

## Required planning practices
1. Link spec, issue, owner, target branch and baseline SHA.
2. Describe actual current flow using source paths.
3. Break work into reviewable steps with dependencies and package ownership.
4. Include migration, schema, generated artifacts, SDK, Go/Rust, console and deployment impacts.
5. Identify retries, concurrency, compatibility, rollout and rollback risks.
6. Map acceptance criteria to tests and update evidence as work progresses.
7. Record blocked work, decisions and residual risks.

Use explicit statuses. Mark a step complete only when its exit evidence exists. A completed plan references the final PR-head SHA and CI run, not an earlier local commit.
