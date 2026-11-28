---
title: Documentation standards
type: guide
status: current
owner: premchandkpc
last_verified: 2026-11-27 @ 285a8c0
---

# Documentation standards

How to write, place, review and retire documentation in FlowRulz. Engineers and coding agents both follow this. [AGENTS.md](../AGENTS.md) defines engineering rules; this file defines documentation rules. If they conflict, AGENTS.md wins and this file gets fixed.

The checker `scripts/check_docs.py` enforces the mechanical rules (marked **[checked]**). The rest is review judgment.

## 1. The one rule

**A document may only claim what the code, schemas, migrations, tests or CI prove.** Docs describe the system; they never define it. When a doc and the code disagree, the doc is wrong until proven otherwise.

Consequences:

- Every behavioral claim is either backed by a link to source/tests, or labeled with one of the status words in [section 5](#5-claim-labels).
- Aspirational content lives in a `proposal`, `plan` or `spec`, never in a `reference` or `guide` presented as fact.
- "Works", "supported", "exactly-once", "HA", "production-ready" are claims. Treat them like code: prove them.

## 2. Where does it go

One topic, one canonical home. Link to it; never copy it.

| You are writing | Type | Folder | Template |
|---|---|---|---|
| Required behavior, acceptance criteria | `spec` | `docs/specs/` | [feature-spec-template](specs/feature-spec-template.md) |
| Work sequence for an approved change | `plan` | `docs/plans/` | [change-plan-template](plans/change-plan-template.md) |
| A decision that is hard to reverse | `adr` | `docs/adr/` | [adr](templates/adr.md) |
| Dated findings at a specific commit | `report` | `docs/reports/` (or `docs/audit/` for the 2026-10 snapshots) | [report-template](reports/report-template.md) |
| Not-yet-shipped design | `proposal` | `docs/proposals/` | [design-doc](templates/design-doc.md) |
| Exact facts: fields, flags, endpoints, env vars | `reference` | topic folder or `docs/*.md` | [reference](templates/reference.md) |
| Task-oriented steps to achieve a goal | `guide` | topic folder | [guide](templates/guide.md) |
| On-call procedure for a symptom | `runbook` | `docs/operations/` | [runbook](templates/runbook.md) |
| Sample app or fixture walkthrough | `example` | `docs/examples/` | none |
| Behavior test scenario | spec/testing | `docs/testing/` | [bdd-scenario-template](testing/bdd-scenario-template.md) |

Decision shortcuts:

- Explaining **why** we chose X? ADR. Explaining **how X works today**? reference. Explaining **how to do Y with X**? guide.
- Is the content about to be wrong when the code changes? It belongs next to the code (a Go doc comment or a table generated from schema), not in a hand-written paragraph.
- Do not mix types in one file. A guide that grows a field table gets that table moved to a reference and linked.

Outside `docs/`: only `README.md`, `AGENTS.md`, `CLAUDE.md`, `SECURITY.md`, `.github/` templates and agent skill files. Per-package notes go in Go doc comments (`doc.go`).

## 3. Front matter **[checked]**

Every Markdown file under `docs/` (except `docs/diagrams/` and `docs/templates/`) starts with:

```yaml
---
title: Outbox claim and delivery safety
type: spec            # index | reference | guide | runbook | adr | spec | plan | report | proposal | template | example | diagram-index
status: current       # draft | current | proposed | accepted | superseded | deprecated | historical
owner: premchandkpc   # GitHub handle responsible for accuracy
last_verified: 2026-11-27 @ 285a8c0   # date and commit the claims were checked against, or: never
---
```

Optional keys:

| Key | Use |
|---|---|
| `supersedes` / `superseded_by` | Relative path. Required when `status: superseded`. |
| `source_of_truth` | Paths of the code or schemas that define the behavior described. Strongly recommended for `reference` and `guide`. |
| `baseline` | Full SHA a `report` or `plan` was written against. |
| `tracks` | Issue or PR URL a `plan`/`proposal` is tracked in. |

`status` meaning:

| Status | Meaning | Allowed for |
|---|---|---|
| `draft` | Being written; do not rely on it | any |
| `current` | Matches the code as of `last_verified` | reference, guide, runbook, spec, index, example |
| `proposed` | Design under discussion, not shipped | proposal, adr |
| `accepted` | Decision made (the code may still lag; say so in the doc) | adr |
| `superseded` | Replaced; keep for history | adr, spec, plan |
| `deprecated` | Still true but being removed | reference, guide |
| `historical` | Snapshot at a point in time; never edited for accuracy | report, audit, old plans |

`last_verified: never` is honest and allowed. It means nobody has re-checked this doc against code since it was onboarded to these standards. The checker reports how many docs are `never`; that number is the verification backlog and should trend down.

Never set `last_verified` to a date you did not actually verify. Verifying means: every command run, every path opened, every behavioral claim traced to code or a test, at that commit. Use the `verify-doc` skill for the procedure.

## 4. Structure and style

### Structure

- One H1, equal to `title`. No skipped heading levels.
- Open with 1-3 sentences: what this is, who it is for, and what it is not. A reader must know in 10 seconds whether they are in the right file.
- Put the answer first. Background and rationale after.
- Keep files under about 400 lines. Past that, split by topic and add a folder README as the index.
- Every folder under `docs/` with 3 or more files has a `README.md` index. Every doc is reachable from `docs/README.md` through links **[checked: orphans]**.
- Dated files are named `YYYY-MM-DD-slug.md`. ADRs are `ADR-NNN-slug.md` with a never-reused number.
- Filenames: lowercase kebab-case. No spaces, no `final`, `v2`, `new`, `copy`.
- Do not create a second file for a topic that already has a canonical home. Two files named `contract.md` and `contracts.md` is the anti-example.

### Style

- Present tense, active voice, imperative for steps ("Run `make ci`").
- Concrete over abstract. Prefer the exact env var, endpoint, table and file path to a description of it.
- Use the project's terms exactly and consistently: tenant, partition key, shard, ruleset, rule revision, Domain, Service, execution, outbox, inbox, fencing token, lease. Do not rename one in prose.
- Do not use these words unless proven and linked: "guarantees exactly-once", "highly available", "production-ready", "seamless", "robust", "simply", "just", "obviously".
- No marketing. No filler. No "In this document we will".
- Write for a new engineer on their first day who can read Go but has not seen this repo.

### Formatting

- Code, commands, paths, env vars, field names and HTTP routes in backticks.
- Fenced code blocks always carry a language (`sh`, `go`, `json`, `yaml`, `sql`, `text`, `mermaid`).
- Commands must be copy-pasteable and run from the repo root unless the block says otherwise. State the working directory when it differs.
- Do not put secrets, real hostnames or real tenant data in examples. Use `acme-corp`-style placeholders and `.env.example` values.
- Tables for facts that have the same shape (env vars, endpoints, status codes). Prose for reasoning.
- Relative links only, to the file and, where useful, the heading anchor **[checked: links]**. Link text says what is there, never "click here".

### Diagrams

- Mermaid in a fenced `mermaid` block, or a `.mmd` file under `docs/diagrams/` **[checked: mermaid type]**.
- A diagram carries one idea. If it needs a legend longer than three lines, split it.
- Every diagram has a prose sentence stating what it shows and the doc that owns the detail. The diagram is not the only place a fact lives.
- Number-prefixed `.mmd` files in `docs/diagrams/` are indexed from the doc that explains them. Do not leave orphan diagrams.

## 5. Claim labels

Use these exact words so readers and agents can search for them.

| Label | Meaning | Proof required |
|---|---|---|
| **Implemented** | Code exists on the default branch | link to code |
| **Tested** | A named test exercises it | link to test; say whether it uses real PostgreSQL/NATS or fakes |
| **Verified end-to-end** | Whole path run and durable result observed | command, commit, environment, outcome |
| **Partial** | Some paths work, gaps listed | list the gaps |
| **Planned** | Not built | link to plan or TODO item |
| **Unverified** | Believed true, not checked | none; must not be used to justify a guarantee |

Rules that follow from the product's actual guarantees (see [AGENTS.md section 2](../AGENTS.md#2-product-model-and-event-flow)):

- Delivery is **at-least-once**. Never write "exactly-once" for the pipeline. Effectively-once is allowed only where inbox deduplication plus stable idempotency keys are cited.
- A producer broker ACK is not worker processing. A worker ACK is durable responsibility, not completion of an external effect. Keep those three terms distinct.
- Durable, direct and broker worker modes have different guarantees. Name the mode in every claim about delivery, ordering or recovery.
- Rust runtime claims need a parity test or an explicit "Rust: not verified" note.

## 6. Evidence

Reports, audits, plans closing out work and any "verified" statement record:

1. Commit SHA (full, or at least 7 characters).
2. Exact commands run.
3. Environment: OS, Go version, which services were real and which were fakes.
4. Outcome per command: passed, failed, skipped (with reason), not run.
5. Output excerpt only when it explains a failure.

Use the vocabulary from [AGENTS.md section 17.3](../AGENTS.md#173-ci-and-proof-statements). "All CI passed" is never acceptable for a queued or partial run.

## 7. Lifecycle

| Event | Do |
|---|---|
| Behavior changes in a PR | Update the affected docs in the same PR. Reviewers block on missing doc updates. |
| You read a doc and it is wrong | Fix it in the same PR, or open an issue using the documentation template. Do not leave it. |
| A report or audit is out of date | Leave it. Add a newer report that links back. Historical docs are not edited for accuracy. |
| A decision is reversed | Write a new ADR with `supersedes`. Set the old one to `superseded` with `superseded_by`. Do not edit the old decision text. |
| A proposal ships | Write the reference/guide for the shipped behavior. Set the proposal `status: superseded` pointing to it. |
| A doc is replaced by another | Keep a stub at the old path for one release containing a link and `status: deprecated`. Then delete it. |
| Renaming or moving a file | Use `git mv`, then fix every inbound link. `make docs-check` must pass. |
| Deleting a doc | Only when its content is preserved elsewhere or is provably false. State which in the PR. AGENTS.md forbids deleting substantial documentation to tidy a patch. |

### ADR rules

- One decision per ADR. Number is permanent.
- Sections: Context, Decision, Alternatives considered, Consequences, Rollback.
- Status values only `proposed`, `accepted`, `superseded`, `deprecated`.
- An accepted ADR whose code is not yet merged says so in Consequences, naming the gap.
- Never edit the Decision of an accepted ADR. Supersede it.

### Runbook rules

- Titled by symptom ("Outbox backlog growing"), not by component.
- Sections: Symptom and alert, Impact, Diagnose (commands with expected output), Mitigate, Fix, Verify recovery, Escalate.
- Every command is tested against the local Compose stack or labeled `unverified`.
- Never instruct a destructive action without stating the data-loss consequence first.

## 8. Review checklist

For any PR that touches `docs/`, `README.md`, `AGENTS.md` or `CLAUDE.md`:

- [ ] `make docs-check` passes.
- [ ] Front matter present and honest. `last_verified` only changed if the doc was actually verified.
- [ ] Each behavioral claim is traceable to code/tests or carries a claim label.
- [ ] No guarantee stronger than at-least-once implied.
- [ ] Commands copy-pasted and run, or marked `unverified`.
- [ ] No second copy of something that has a canonical home.
- [ ] New or moved file is linked from its folder README or `docs/README.md`.
- [ ] Related surfaces checked: schema, env example, Compose, Make targets, CI, [AGENTS.md repo map](../AGENTS.md#3-repository-map-and-file-ownership).
- [ ] Terminology matches the project glossary used in code.

## 9. Tooling

| Tool | Purpose |
|---|---|
| `make docs-check` | Runs `scripts/check_docs.py` (links, anchors, front matter, orphans, Mermaid types, placeholders) |
| `python3 scripts/check_docs.py --only links` | Run one check |
| `.github/workflows/docs.yml` | Runs the same check plus markdownlint on every PR that touches docs |
| `.markdownlint-cli2.yaml` | Lint rules for Markdown formatting |
| `.claude/skills/` | Agent skills: `write-doc`, `write-adr`, `verify-doc` |
| `.mcp.json` | Project MCP servers for library docs and GitHub |
| `CLAUDE.md` | Entry point for Claude Code; imports AGENTS.md and points here |

The checker is standard-library Python so CI needs no installs.

## 10. Known debt

Tracked here so it is visible, not hidden.

- Most docs are `last_verified: never`. Verify the ones in `docs/architecture/`, `docs/operations.md`, `docs/api.md` and `docs/worker.md` first; they drive operator decisions.
- Overlapping pairs remain: `contract.md` / `contracts.md`, `rules.md` / `rules/`, `domain.md` / `domain/`, `testing.md` / `testing/`. Each now states its scope and links the other. Consolidation is a follow-up that needs the inbound links and the AGENTS.md map updated together.
- `docs/audit/` and `docs/reports/` both hold dated reviews. New ones go in `docs/reports/`.
- `docs/IMPLEMENTATION_PLAN.md`, `docs/design.md` and `docs/roadmap.md` are old and need a status decision (verify, mark historical, or fold into `docs/TODO.md`).
- `.opencode/skills/` describes Rust VM work (continuations, work-stealing, bytecode versioning). Confirm whether that is still the direction; if not, retire it.
