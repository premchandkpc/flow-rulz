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
	"github.com/flowrulz/flow-rulz/internal/ports"
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
	options, err := parseOptions(os.Args[1:])
	if err != nil {
		return err
	}
	ctx := context.Background()
	db, err := postgresadapter.New(ctx, options.databaseURL(), options.migrations)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()
	return executeGeneration(ctx, postgresadapter.NewContractRegistry(db.Pool()), options)
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

type generationOptions struct {
	dsn, contract, version, target, output, migrations string
	list                                               bool
}

func parseOptions(args []string) (generationOptions, error) {
	flags := flag.NewFlagSet(codegenCommandName, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var options generationOptions
	options.bind(flags)
	err := flags.Parse(args)
	return options, err
}

func (o *generationOptions) bind(flags *flag.FlagSet) {
	flags.StringVar(&o.dsn, "dsn", "", "PostgreSQL connection string")
	flags.StringVar(&o.contract, "contract", "", "Registered contract name")
	flags.StringVar(&o.version, "version", "", "Registered contract version")
	flags.StringVar(&o.target, "target", codegen.TargetGo, "Target language: "+codegen.SupportedTargetsText)
	flags.StringVar(&o.output, "output", "", "Output directory (prints generated files when empty)")
	flags.BoolVar(&o.list, "list", false, "List all registered contracts")
	flags.StringVar(&o.migrations, "migrations", "migrations", "Path to migrations directory")
}

func (o generationOptions) databaseURL() string {
	if o.dsn != "" {
		return o.dsn
	}
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@localhost:5432/flowrulz?sslmode=disable"
}

func executeGeneration(ctx context.Context, registry ports.ContractRegistry, options generationOptions) error {
	if options.list {
		return listContracts(ctx, registry)
	}
	generator, err := loadGenerator(ctx, registry, options)
	if err != nil {
		return err
	}
	return options.generate(generator)
}

func listContracts(ctx context.Context, registry ports.ContractRegistry) error {
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

func (o generationOptions) generate(generator *codegen.Generator) error {
	if o.output == "" {
		return printGeneratedTarget(generator, o.target)
	}
	if err := generateTarget(generator, o.target, o.output); err != nil {
		return fmt.Errorf("generate %s: %w", o.target, err)
	}
	_, err := fmt.Printf("Generated %s artifacts in %s\n", o.target, o.output)
	return err
}

func printGeneratedTarget(generator *codegen.Generator, target string) error {
	tmpDir, err := os.MkdirTemp("", "flowrulz-codegen-*")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()
	files, err := generateTargetFiles(generator, target, tmpDir)
	if err != nil {
		return fmt.Errorf("generate %s: %w", target, err)
	}
	return printGeneratedFiles(files)
}

func printGeneratedFiles(files []string) error {
	for _, file := range files {
		if err := printGeneratedFile(file); err != nil {
			return err
		}
	}
	return nil
}

func printGeneratedFile(file string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("read generated file %s: %w", file, err)
	}
	if _, err := os.Stdout.Write(data); err != nil {
		return fmt.Errorf("write generated file %s: %w", file, err)
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return printGeneratedSeparator(file)
	}
	return nil
}

func printGeneratedSeparator(file string) error {
	if _, err := fmt.Fprintln(os.Stdout); err != nil {
		return fmt.Errorf("write generated file separator for %s: %w", file, err)
	}
	return nil
}

func loadGenerator(ctx context.Context, registry ports.ContractRegistry, options generationOptions) (*codegen.Generator, error) {
	if options.contract == "" || options.version == "" {
		return nil, errors.New("usage: " + codegenCommandName + " -contract <name> -version <version> [-target " + codegen.SupportedTargetsText + "] [-output directory], or " + codegenCommandName + " -list")
	}
	schema, err := registry.Get(ctx, options.contract, options.version)
	if err != nil {
		return nil, fmt.Errorf("get contract %s@%s: %w", options.contract, options.version, err)
	}
	if schema == nil {
		return nil, fmt.Errorf("contract %s@%s not found", options.contract, options.version)
	}
	return codegen.NewGenerator(schema), nil
}
