// cmd/codegen is the code generation CLI for FlowRulz contracts.
// It generates typed clients, runtime components, evaluators, and service scaffolding
// from registered contract schemas and rule sets.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	postgresadapter "github.com/flowrulz/flow-rulz/internal/adapters/postgres"
	"github.com/flowrulz/flow-rulz/internal/codegen"
)

const codegenCommandName = "codegen"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", codegenCommandName, err)
		os.Exit(1)
	}
}

// run owns resources so every error path executes deferred cleanup before main exits.
func run() error {
	flags := flag.NewFlagSet(codegenCommandName, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	dsn := flags.String("dsn", "", "PostgreSQL connection string")
	contract := flags.String("contract", "", "Registered contract name")
	version := flags.String("version", "", "Registered contract version")
	target := flags.String("target", codegen.TargetGo, "Target language: "+codegen.SupportedTargetsText)
	output := flags.String("output", "", "Output directory (prints generated files when empty)")
	list := flags.Bool("list", false, "List all registered contracts")
	migrations := flags.String("migrations", "migrations", "Path to migrations directory")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}

	if *dsn == "" {
		*dsn = os.Getenv("DATABASE_URL")
	}
	if *dsn == "" {
		*dsn = "postgres://postgres:postgres@localhost:5432/flowrulz?sslmode=disable"
	}

	ctx := context.Background()
	db, err := postgresadapter.New(ctx, *dsn, *migrations)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	registry := postgresadapter.NewContractRegistry(db.Pool())
	if *list {
		schemas, err := registry.List(ctx, "")
		if err != nil {
			return fmt.Errorf("list contracts: %w", err)
		}
		for _, schema := range schemas {
			if _, err := fmt.Printf("%s@%s\n", schema.Name, schema.Version); err != nil {
				return fmt.Errorf("write contract list: %w", err)
			}
		}
		return nil
	}

	if *contract == "" || *version == "" {
		return errors.New("usage: " + codegenCommandName + " -contract <name> -version <version> [-target " + codegen.SupportedTargetsText + "] [-output directory], or " + codegenCommandName + " -list")
	}

	schema, err := registry.Get(ctx, *contract, *version)
	if err != nil {
		return fmt.Errorf("get contract %s@%s: %w", *contract, *version, err)
	}
	if schema == nil {
		return fmt.Errorf("contract %s@%s not found", *contract, *version)
	}

	generator := codegen.NewGenerator(schema)
	if *output != "" {
		if err := generateTarget(generator, *target, *output); err != nil {
			return fmt.Errorf("generate %s: %w", *target, err)
		}
		_, err := fmt.Printf("Generated %s artifacts in %s\n", *target, *output)
		return err
	}

	tmpDir, err := os.MkdirTemp("", "flowrulz-codegen-*")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	files, err := generateTargetFiles(generator, *target, tmpDir)
	if err != nil {
		return fmt.Errorf("generate %s: %w", *target, err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read generated file %s: %w", file, err)
		}
		if _, err := os.Stdout.Write(data); err != nil {
			return fmt.Errorf("write generated file %s: %w", file, err)
		}
		if len(data) == 0 || data[len(data)-1] != '\n' {
			if _, err := fmt.Fprintln(os.Stdout); err != nil {
				return fmt.Errorf("write generated file separator: %w", err)
			}
		}
	}
	return nil
}

func generateTarget(generator *codegen.Generator, target, outputDir string) error {
	_, err := generateTargetFiles(generator, target, outputDir)
	return err
}

func generateTargetFiles(generator *codegen.Generator, target, outputDir string) ([]string, error) {
	switch target {
	case codegen.TargetGo:
		return generator.GenerateGo(outputDir)
	case codegen.TargetJava:
		return generator.GenerateJava(outputDir)
	case codegen.TargetProtobuf:
		return generator.GenerateProtobuf(outputDir)
	case codegen.TargetJSONSchema:
		return generator.GenerateJSONSchema(outputDir)
	default:
		return nil, fmt.Errorf("unknown target %q (supported: %s)", target, codegen.SupportedTargetsText)
	}
}
