# Engineering reports

Reports are dated evidence snapshots, not living specifications. Preserve original reports as historical records; write a follow-up report when findings change.

## Existing reports
- [Historical audits](../audit/): dated architecture and repository reviews; revalidate findings against current source.
- [Security findings](../security/): security-specific investigations and follow-up evidence.

## Files
- [Report template](report-template.md): format for audits, PR reviews, reliability investigations and release readiness.
- [Risk register](risk-register.md): cross-cutting risks and links to evidence. Change status only when new evidence exists.

Every report should record date, source commit, scope, commands/environment, findings with severity, evidence links, verification status, owner, remediation, and residual risk. Separate observed facts from hypotheses and recommendations.

## Severity guide
- **Critical:** credible path to data loss, cross-tenant exposure, or broad service compromise.
- **High:** major correctness/security failure with realistic production impact.
- **Medium:** meaningful reliability, availability, or maintainability risk with constraints or mitigations.
- **Low:** limited impact, hygiene, or documentation issue.
- **Informational:** observation without a demonstrated defect.

Severity requires evidence and context. Do not copy old severities forward without revalidation.
