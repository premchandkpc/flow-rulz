---
name: verify-doc
description: Verify an existing FlowRulz doc against the code and update its last_verified. Use to work down the "last_verified: never" backlog.
---

# verify-doc

Input: a doc path. Pick from `python3 scripts/check_docs.py | tail` backlog; start with `docs/architecture/`, `operations.md`, `api.md`, `worker.md`.

1. Record the commit: `git rev-parse --short HEAD`.
2. List every claim: fields, flags, env vars, routes, behaviors, guarantees, commands, file paths.
3. Check each against source, schema, migration, test or CI. Run each command in a clean checkout where possible; real PostgreSQL and NATS for driver or recovery claims.
4. Fix wrong claims in place. Mark what you could not prove as Unverified or Partial. Do not delete content to make it pass.
5. Only if every claim is now checked, set `last_verified: YYYY-MM-DD @ <sha>`. If you could not finish, leave `never` and say what remains.
6. Do not touch `historical` docs except links. Run `make docs-check`.
7. Report: claims checked, claims fixed, claims left unverified, commands run with outcomes.
