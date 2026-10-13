# CodeQL alert 24: MongoDB workflow-definition filter

## Finding

CodeQL reports `go/sql-injection` at the `WorkflowDefinitionRepository.Save`
MongoDB `UpdateOne` call in `internal/adapters/mongo/workflow.go`.

## Security analysis

The operation does not construct SQL, MongoDB query text, or BSON operator
documents from caller-controlled strings. The filter is a fixed ordered BSON
document with exactly these keys:

- `workflow_type`, whose value is a Go `string`
- `version`, whose value is an `int64`

The workflow type is validated before the database call against
`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`. Values such as JSON objects,
`$ne`, `$where`, whitespace, and slashes are rejected. With the MongoDB Go
driver, a Go string is encoded as a BSON string scalar. Text inside that string
is not reinterpreted as a BSON document or a MongoDB query operator. The update
operator `$set` is a literal, developer-authored key, and the value is a typed
`WorkflowDefinitionDoc`.

Therefore, the tainted value reaches a structured query value, not a query
language expression. The alert appears to be a static-analysis false positive
for this MongoDB-driver usage, rather than an exploitable injection path.

## Regression coverage

`internal/adapters/mongo/workflow_security_test.go` verifies rejection of
operator-shaped workflow type values and confirms the accepted workflow type
is serialized as a BSON string under a fixed-key filter.

## Required handling

Do not treat this document or a passing Go test as proof that the alert is
cleared. Re-run the configured CodeQL analysis. If the alert remains, a
repository security administrator must dismiss alert 24 as a false positive
with this reasoning recorded in the alert's dismissal comment. Keep PR #9
unmerged until that formal dismissal or an equivalent CodeQL-cleared result
is visible.
