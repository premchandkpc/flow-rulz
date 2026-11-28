---
name: write-doc
description: Create or substantially edit a FlowRulz doc under docs/. Use for any new guide, reference, runbook, spec, plan, proposal or report. Picks the type, template and folder, then enforces docs/STANDARDS.md.
---

# write-doc

1. Read `docs/STANDARDS.md` sections 1-5.
2. Search `docs/` for the topic first (`grep -ril <keyword> docs/`). If a canonical home exists, edit it; do not create a second file.
3. Pick the type with the table in STANDARDS section 2. Copy the matching template from `docs/templates/` (or the spec, plan, report, BDD template in its own folder).
4. Read the code the doc will describe: handlers, config structs, migrations, tests. Every behavioral claim must come from what you read. Put the paths in `source_of_truth`.
5. Write. Answer first. Use exact names (env vars, routes, tables). Label unproven claims Planned or Unverified. Name the worker mode for delivery claims. Never write "exactly-once" for the pipeline.
6. Set front matter. Use `last_verified: never` unless you ran every command and traced every claim; then use `YYYY-MM-DD @ <short sha>`.
7. Link the new file from its folder README or `docs/README.md`.
8. Run `make docs-check`. Fix every error. Report the real output.
