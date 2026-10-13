package contract

import (
	"testing"
)

func TestInboxSuite(t *testing.T) {
	suite := InboxTestSuite{
		NewRepo: func() InboxRepository {
			return newMemInbox()
		},
	}
	suite.RunTests(t)
}

func TestExecutionSuite(t *testing.T) {
	suite := ExecutionTestSuite{
		NewRepo: func() ExecutionRepository {
			return newMemExecution()
		},
	}
	suite.RunTests(t)
}

func TestOutboxSuite(t *testing.T) {
	suite := OutboxTestSuite{
		NewRepo: func() OutboxRepository {
			return newMemOutbox()
		},
	}
	suite.RunTests(t)
}
