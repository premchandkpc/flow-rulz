# Engineering reports and audits

Reports are dated evidence snapshots: what was inspected, at which commit, what was observed and what remains uncertain. A report is not a substitute for a spec, plan, issue or test.

## Documents
- [Report template](report-template.md): architecture/security review, incident analysis, PR review or release-readiness assessment.
- [Risk register](risk-register.md): cross-cutting risk areas, evidence and owners.
- [Historical audits](../audit/): preserve as snapshots; revalidate findings against current code before treating them as current.
- [Security findings](../security/): security-specific investigation and remediation evidence.

## Report standards
Include date, source commit, scope, environment, exact commands, findings, evidence, severity rationale, status, owner, recommendation, verification and residual risk. Separate:
- **Observed fact:** supported by source, logs, tests or reproduction.
- **Hypothesis:** plausible but not demonstrated.
- **Recommendation:** proposed next action.
- **Verified fix:** regression test/check passed on a named commit.

## Severity guide
- **Critical:** credible broad compromise, cross-tenant exposure or substantial data loss.
- **High:** major correctness/security failure with realistic production impact.
- **Medium:** meaningful reliability/availability risk or important control missing.
- **Low:** limited impact, hygiene or maintainability issue.
- **Informational:** observation without a demonstrated defect.

Severity depends on likelihood, blast radius and deployment context. Revalidate old severities rather than copying them forward.
