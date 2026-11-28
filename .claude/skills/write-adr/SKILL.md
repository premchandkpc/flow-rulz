---
name: write-adr
description: Write or supersede a FlowRulz architecture decision record in docs/adr/. Use when a hard-to-reverse design choice is made or reversed.
---

# write-adr

1. Read `docs/STANDARDS.md` ADR rules and two existing ADRs in `docs/adr/`.
2. Take the next number from `docs/adr/README.md`. Numbers are never reused.
3. Copy `docs/templates/adr.md`. One decision only. Sections: Context, Decision, Alternatives considered (at least two), Consequences, Rollback.
4. Consequences must state delivery-guarantee impact and any gap between the decision and merged code.
5. Status is `proposed` until accepted. Never edit the Decision of an accepted ADR; to reverse it, write a new ADR with `supersedes`, then set the old one to `superseded` with `superseded_by`.
6. Add a row to the table in `docs/adr/README.md`.
7. Run `make docs-check`.
