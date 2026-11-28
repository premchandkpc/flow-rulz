# CLAUDE.md

@AGENTS.md

## Documentation work

Before creating or changing anything under `docs/`, read `docs/STANDARDS.md`. Short version:

- A doc may only claim what code, schemas, migrations, tests or CI prove. Label the rest: Implemented, Tested, Verified end-to-end, Partial, Planned, Unverified.
- Delivery is at-least-once. Never write "exactly-once" for the pipeline. Broker ACK, worker ACK and external-effect completion are three different things.
- Name the worker mode (durable, direct, broker) in every claim about delivery, ordering or recovery.
- Every doc under `docs/` needs front matter: `title`, `type`, `status`, `owner`, `last_verified`. Use `last_verified: never` unless you actually checked the doc against code at a commit.
- One canonical home per topic. Link, do not copy. Copy a template from `docs/templates/`.
- Do not edit `historical` reports or accepted ADR decisions. Add a newer report or a superseding ADR.
- Never delete substantial documentation to tidy a change.
- Finish with `make docs-check`. Report its actual output.

Skills: `write-doc`, `write-adr`, `verify-doc` (in `.claude/skills/`).
