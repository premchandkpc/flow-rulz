# Engineering plans

Plans define how and in what order to implement an approved change. A plan is a working execution artifact, not the product specification.

## Files
- [Change plan template](change-plan-template.md): cross-package, persistence, concurrency, security or operational changes.
- [Legacy architecture redesign plan](legacy-architecture-redesign.md): historical draft; verify every task against current source before reuse.
- [Root implementation plan](../IMPLEMENTATION_PLAN.md): historical plan; assumptions may be stale.
- [Roadmap](../roadmap.md): planned direction, not evidence of shipped behavior.

## Planning rules
- Link every plan to an approved spec or explain why a spec is unnecessary.
- Order steps by dependencies and keep each step reviewable.
- Identify file/package ownership, migrations, compatibility, rollout, rollback, and failure recovery.
- Map each acceptance criterion to a test or an explicitly owned verification gap.
- Define exit criteria and residual risks.
- Never check a task complete merely because code was written; cite the check and commit.
