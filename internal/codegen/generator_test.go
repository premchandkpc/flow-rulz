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
	generator := NewGenerator(&domain.ContractSchema{
		Name:    "order.created",
		Version: "1.0",
		Fields: map[string]domain.ContractField{
			"order_id": {Type: "string", Required: true},
			"note":     {Type: "string"},
		},
	})

	files, err := generator.GenerateGo(outputDir)
	if err != nil {
		t.Fatalf("GenerateGo() error = %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("GenerateGo() returned %d files, want contract and typed client", len(files))
	}

	for _, path := range files {
		if _, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors); err != nil {
			t.Errorf("generated Go file %s does not parse: %v", path, err)
		}
	}

	contractPath := filepath.Join(outputDir, TargetGo, "created.go")
	contractContent, err := os.ReadFile(contractPath)
	if err != nil {
		t.Fatalf("read generated contract: %v", err)
	}
	if !strings.Contains(string(contractContent), "OrderId string `json:\"order_id\"`") {
		t.Errorf("generated Go contract does not expose the order_id field as OrderId")
	}

	if !strings.Contains(string(contractContent), "Note string `json:\"note,omitempty\"`") {
		t.Errorf("generated Go contract should mark optional fields omitempty")
	}

	clientPath := filepath.Join(outputDir, TargetGo, "created_client.go")
	content, err := os.ReadFile(clientPath)
	if err != nil {
		t.Fatalf("read generated client: %v", err)
	}
	for _, expected := range []string{
		`const DefaultRuleSet = "order.created"`,
		"func (c *Client) SendToRuleSet(",
		"func (c *Client) SubscribeRuleSet(",
		"opts ...flowrulz.SendOption",
		"c.client.Subscribe(ctx, ruleSet",
	} {
		if !strings.Contains(string(content), expected) {
			t.Errorf("generated client does not contain %q", expected)
		}
	}
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
	generator := NewGenerator(&domain.ContractSchema{
		Name:    "order.created",
		Version: "1.0",
		Fields:  map[string]domain.ContractField{},
	})
	tests := map[string]func(string) error{
		"go": func(dir string) error {
			_, err := generator.GenerateGo(dir)
			return err
		},
		"java": func(dir string) error {
			_, err := generator.GenerateJava(dir)
			return err
		},
		"protobuf": func(dir string) error {
			_, err := generator.GenerateProtobuf(dir)
			return err
		},
		"jsonschema": func(dir string) error {
			_, err := generator.GenerateJSONSchema(dir)
			return err
		},
		"all": func(dir string) error {
			_, err := generator.GenerateAll(dir)
			return err
		},
		"runtime": func(dir string) error {
			_, err := generator.GenerateRuntime(dir)
			return err
		},
		"evaluator": func(dir string) error {
			_, err := generator.GenerateEvaluator(dir)
			return err
		},
		"service": func(dir string) error {
			_, err := generator.GenerateService(dir)
			return err
		},
	}

	for name, generate := range tests {
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

	generator := NewGenerator(&domain.ContractSchema{
		Name:    "order.created",
		Version: "1.0",
		Fields:  map[string]domain.ContractField{},
	})
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
	generator := NewGenerator(&domain.ContractSchema{
		Name:    "order.created",
		Version: "1.0",
		Fields: map[string]domain.ContractField{
			"items": {Type: "array", Items: &domain.ContractField{Type: "object", Properties: map[string]domain.ContractField{
				"product_id": {Type: "string"},
				"quantity":   {Type: "integer"},
			}}},
			"shipping_address": {Type: "object", Properties: map[string]domain.ContractField{
				"city": {Type: "string"},
			}},
			"tags": {Type: "array", Items: &domain.ContractField{Type: "string"}},
		},
	})

	files, err := generator.GenerateGo(outputDir)
	if err != nil {
		t.Fatalf("GenerateGo() error = %v", err)
	}
	for _, path := range files {
		if _, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors); err != nil {
			t.Fatalf("generated Go file %s does not parse: %v", path, err)
		}
	}
	content, err := os.ReadFile(filepath.Join(outputDir, TargetGo, "created.go"))
	if err != nil {
		t.Fatalf("read generated contract: %v", err)
	}
	for _, expected := range []string{
		"Items []Item `json:\"items,omitempty\"`",
		"ShippingAddress ShippingAddress `json:\"shipping_address,omitempty\"`",
		"Tags []string `json:\"tags,omitempty\"`",
		"type Item struct",
		"ProductId string `json:\"product_id,omitempty\"`",
		"Quantity int64 `json:\"quantity,omitempty\"`",
		"type ShippingAddress struct",
	} {
		if !strings.Contains(string(content), expected) {
			t.Errorf("generated contract missing %q:\n%s", expected, content)
		}
	}
}
