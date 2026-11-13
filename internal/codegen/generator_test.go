package codegen

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestGenerateGoIncludesTypedClient(t *testing.T) {
	outputDir := t.TempDir()
	generator := newOrderCreatedGenerator(map[string]domain.ContractField{
		"order_id": {Type: "string", Required: true},
		"note":     {Type: "string"},
	})
	files, err := generator.GenerateGo(outputDir)
	if err != nil {
		t.Fatalf("GenerateGo() error = %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("GenerateGo() returned %d files, want contract and typed client", len(files))
	}
	assertGeneratedFilesParse(t, files)
	assertGeneratedContractFields(t, filepath.Join(outputDir, TargetGo, "created.go"))
	assertGeneratedTypedClient(t, filepath.Join(outputDir, TargetGo, "created_client.go"))
}

func TestToGoIdentifier(t *testing.T) {
	tests := map[string]string{
		"order_id":  "OrderId",
		"order-id":  "OrderId",
		"1st_order": "Field1stOrder",
		"!!!":       "Field",
	}
	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			if got := toGoIdentifier(input); got != want {
				t.Errorf("toGoIdentifier(%q) = %q, want %q", input, got, want)
			}
		})
	}
}

func TestSanitizeGeneratedOutputDirAllowsSafeAbsolutePath(t *testing.T) {
	want := filepath.Join(t.TempDir(), "orders", "v1")

	got, err := sanitizeGeneratedOutputDir(want)
	if err != nil {
		t.Fatalf("sanitizeGeneratedOutputDir() error = %v", err)
	}
	if got != filepath.Clean(want) {
		t.Fatalf("sanitizeGeneratedOutputDir() = %q, want %q", got, filepath.Clean(want))
	}
}

func TestSanitizeGeneratedOutputDirRejectsParentTraversal(t *testing.T) {
	for _, candidate := range []string{
		filepath.Join("..", "outside"),
		filepath.Join("..", "..", "etc"),
		filepath.Join("orders", "..", "..", "outside"),
	} {
		if _, err := sanitizeGeneratedOutputDir(candidate); err == nil {
			t.Errorf("sanitizeGeneratedOutputDir(%q) accepted parent traversal", candidate)
		}
	}
}

func TestSanitizeGeneratedOutputDirRejectsEmptyPath(t *testing.T) {
	if _, err := sanitizeGeneratedOutputDir(""); err == nil {
		t.Fatal("sanitizeGeneratedOutputDir(\"\") accepted empty path")
	}
}

func TestGenerateMethodsRejectParentTraversal(t *testing.T) {
	generator := newOrderCreatedGenerator(map[string]domain.ContractField{})
	for name, generate := range generateMethods(generator) {
		t.Run(name, func(t *testing.T) {
			if err := generate(filepath.Join("..", "outside")); err == nil {
				t.Fatal("generator accepted an output directory with parent traversal")
			}
		})
	}
}

func TestGenerateGoDoesNotFollowOutputSymlinkOutsideRoot(t *testing.T) {
	outputDir := t.TempDir()
	outsideDir := t.TempDir()
	if err := os.Symlink(outsideDir, filepath.Join(outputDir, TargetGo)); err != nil {
		t.Fatalf("create output symlink: %v", err)
	}
	generator := newOrderCreatedGenerator(map[string]domain.ContractField{})
	if _, err := generator.GenerateGo(outputDir); err == nil {
		t.Fatal("GenerateGo() wrote through a symlink outside the output directory")
	}
	if _, err := os.Stat(filepath.Join(outsideDir, "created.go")); !os.IsNotExist(err) {
		t.Fatalf("generated file exists outside output directory: stat error = %v", err)
	}
}

func TestGenerateGoRejectsUnsafeSchemaDerivedFileNames(t *testing.T) {
	generator := NewGenerator(&domain.ContractSchema{
		Name:    "../../escape",
		Version: "1.0",
		Fields:  map[string]domain.ContractField{},
	})
	if _, err := generator.GenerateGo(t.TempDir()); err == nil {
		t.Fatal("GenerateGo() accepted a schema name that produces an unsafe file name")
	}
}

func TestValidateGeneratedTypeName(t *testing.T) {
	for _, name := range []string{"OrderCreated", "TransactionInitiated"} {
		if err := validateGeneratedTypeName(name); err != nil {
			t.Errorf("validateGeneratedTypeName(%q) error = %v", name, err)
		}
	}
	for _, name := range []string{"../Escape", "Order/Created", "1stOrder", ""} {
		if err := validateGeneratedTypeName(name); err == nil {
			t.Errorf("validateGeneratedTypeName(%q) accepted unsafe name", name)
		}
	}
}

func TestGenerateGoEmitsNestedTypes(t *testing.T) {
	outputDir := t.TempDir()
	generator := newOrderCreatedGenerator(nestedOrderFields())
	files, err := generator.GenerateGo(outputDir)
	if err != nil {
		t.Fatalf("GenerateGo() error = %v", err)
	}
	for _, path := range files {
		if err := parseGeneratedGoFile(path); err != nil {
			t.Fatalf("generated Go file %s does not parse: %v", path, err)
		}
	}
	assertNestedTypeContract(t, outputDir)
}

// newOrderCreatedGenerator builds the generator shared by contract tests.
func newOrderCreatedGenerator(fields map[string]domain.ContractField) *Generator {
	return NewGenerator(&domain.ContractSchema{
		Name:    "order.created",
		Version: "1.0",
		Fields:  fields,
	})
}

// nestedOrderFields is the nested-object contract used by generation tests.
func nestedOrderFields() map[string]domain.ContractField {
	return map[string]domain.ContractField{
		"items": {Type: "array", Items: &domain.ContractField{Type: "object", Properties: map[string]domain.ContractField{
			"product_id": {Type: "string"},
			"quantity":   {Type: "integer"},
		}}},
		"shipping_address": {Type: "object", Properties: map[string]domain.ContractField{
			"city": {Type: "string"},
		}},
		"tags": {Type: "array", Items: &domain.ContractField{Type: "string"}},
	}
}

// generateMethods maps each public generation method to a call against the
// given generator so every target is exercised by one test.
func generateMethods(generator *Generator) map[string]func(string) error {
	return map[string]func(string) error{
		"go":         func(dir string) error { _, err := generator.GenerateGo(dir); return err },
		"java":       func(dir string) error { _, err := generator.GenerateJava(dir); return err },
		"protobuf":   func(dir string) error { _, err := generator.GenerateProtobuf(dir); return err },
		"jsonschema": func(dir string) error { _, err := generator.GenerateJSONSchema(dir); return err },
		"all":        func(dir string) error { _, err := generator.GenerateAll(dir); return err },
		"runtime":    func(dir string) error { _, err := generator.GenerateRuntime(dir); return err },
		"evaluator":  func(dir string) error { _, err := generator.GenerateEvaluator(dir); return err },
		"service":    func(dir string) error { _, err := generator.GenerateService(dir); return err },
	}
}

// parseGeneratedGoFile parses a generated Go file reporting all errors.
func parseGeneratedGoFile(path string) error {
	_, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
	return err
}

// assertGeneratedFilesParse reports generated files that do not parse.
func assertGeneratedFilesParse(t *testing.T, paths []string) {
	t.Helper()
	for _, path := range paths {
		if err := parseGeneratedGoFile(path); err != nil {
			t.Errorf("generated Go file %s does not parse: %v", path, err)
		}
	}
}

// readGeneratedFile reads a generated file, failing the test when unreadable.
func readGeneratedFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated file %s: %v", path, err)
	}
	return string(content)
}

// assertGeneratedContractFields checks the contract exposes the typed
// order_id and note fields with the expected JSON tags.
func assertGeneratedContractFields(t *testing.T, path string) {
	t.Helper()
	content := readGeneratedFile(t, path)
	if !strings.Contains(content, "OrderId string `json:\"order_id\"`") {
		t.Errorf("generated Go contract does not expose the order_id field as OrderId")
	}
	if !strings.Contains(content, "Note string `json:\"note,omitempty\"`") {
		t.Errorf("generated Go contract should mark optional fields omitempty")
	}
}

// assertGeneratedTypedClient checks the generated client for the expected
// typed rule-set methods.
func assertGeneratedTypedClient(t *testing.T, path string) {
	t.Helper()
	content := readGeneratedFile(t, path)
	for _, expected := range []string{
		`const DefaultRuleSet = "order.created"`,
		"func (c *Client) SendToRuleSet(",
		"func (c *Client) SubscribeRuleSet(",
		"opts ...flowrulz.SendOption",
		"c.client.Subscribe(ctx, ruleSet",
	} {
		if !strings.Contains(content, expected) {
			t.Errorf("generated client does not contain %q", expected)
		}
	}
}

// assertNestedTypeContract checks the generated contract for nested struct
// and array field output.
func assertNestedTypeContract(t *testing.T, outputDir string) {
	t.Helper()
	content := readGeneratedFile(t, filepath.Join(outputDir, TargetGo, "created.go"))
	for _, expected := range []string{
		"Items []Item `json:\"items,omitempty\"`",
		"ShippingAddress ShippingAddress `json:\"shipping_address,omitempty\"`",
		"Tags []string `json:\"tags,omitempty\"`",
		"type Item struct",
		"ProductId string `json:\"product_id,omitempty\"`",
		"Quantity int64 `json:\"quantity,omitempty\"`",
		"type ShippingAddress struct",
	} {
		if !strings.Contains(content, expected) {
			t.Errorf("generated contract missing %q:\n%s", expected, content)
		}
	}
}
