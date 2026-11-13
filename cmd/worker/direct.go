package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/flowrulz/flow-rulz/internal/adapters/nats"
	"github.com/flowrulz/flow-rulz/internal/application"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/rules"
	svceffects "github.com/flowrulz/flow-rulz/internal/services/effects"
)

// runDirect composes only local rules, the evaluator, and broker adapters.
func runDirect(ctx context.Context, cfg *config.Config) (runErr error) {
	revisions, err := loadDirectRules(cfg.Worker.RulesDir)
	if err != nil {
		return err
	}
	natsCfg := newNATSConfig(cfg)
	consumer, err := nats.NewDirectConsumer(ctx, natsCfg, cfg.Worker.InputSubject, cfg.Worker.MaxGlobalInFlight)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, consumer.Close()) }()
	publisher, err := nats.NewPublisher(natsCfg)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, publisher.Close()) }()
	commandSender, err := newCommandSender(publisher)
	if err != nil {
		return err
	}
	sender := svceffects.NewRouterWithConfig(publisher, commandSender, eventSubject, cfg.Worker.NumShards)
	worker, err := application.NewDirectWorker(consumer, rules.NewEvaluator(domain.SystemClock{}), sender, revisions, cfg.EventRetryDelay(), cfg.NATSAckWait()/2)
	if err != nil {
		return err
	}
	log.Printf("direct worker started: local_rules=%d input=%s; no database, inbox, outbox, leases or scheduler", len(revisions), cfg.Worker.InputSubject)
	return worker.Run(ctx)
}

// loadDirectRules bounds startup input and compiles the existing JSON rule format.
func loadDirectRules(dir string) ([]*domain.RuleRevision, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 || len(paths) > 100 {
		return nil, errors.New("direct rules directory must contain between 1 and 100 JSON rule files")
	}
	compiler := rules.NewCompiler(rules.DefaultLimits())
	revisions := make([]*domain.RuleRevision, 0, len(paths))
	for _, path := range paths {
		source, err := readDirectRule(path)
		if err != nil {
			return nil, fmt.Errorf("read direct rule %s: %w", path, err)
		}
		revision, err := compiler.Compile(source)
		if err != nil {
			return nil, fmt.Errorf("compile direct rule %s: %w", path, err)
		}
		revisions = append(revisions, revision)
	}
	return revisions, nil
}

func readDirectRule(path string) (source []byte, resultErr error) {
	const maxRuleBytes = 256 * 1024
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, f.Close()) }()
	source, err = io.ReadAll(io.LimitReader(f, maxRuleBytes+1))
	if err != nil {
		return nil, err
	}
	if len(source) > maxRuleBytes {
		return nil, errors.New("rule file exceeds 256 KiB")
	}
	return source, nil
}
