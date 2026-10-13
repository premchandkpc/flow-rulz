package mongo

// Collection names are persistence-schema identifiers. Centralizing them
// prevents adapter methods from silently writing to misspelled collections.
const (
	collectionEvents              = "events"
	collectionInbox               = "inbox"
	collectionExecutions          = "executions"
	collectionOutboxEffects       = "outbox_effects"
	collectionQuarantine          = "quarantine"
	collectionRuleRevisions       = "rule_revisions"
	collectionRuleActivations     = "rule_activations"
	collectionScheduledEvents     = "scheduled_events"
	collectionShardLeases         = "shard_leases"
	collectionWorkflowInstances   = "workflow_instances"
	collectionWorkflowDefinitions = "workflow_definitions"
	collectionBatchRuns           = "batch_runs"
	collectionContractSchemas     = "contract_schemas"
	collectionTenantDomains       = "tenant_domains"
	collectionDomainServices      = "domain_services"
	collectionTransactionChecks   = "_flowrulz_transaction_checks"
)
