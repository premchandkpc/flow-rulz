// Package codegen provides code generation for FlowRulz contracts.
// It generates typed clients, runtime components (envelope, publisher, consumer),
// evaluators, and service scaffolding for multiple target languages.
package codegen

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"unicode"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// TargetGo identifies Go code generation.
const TargetGo = "go"

// TargetJava identifies Java code generation.
const TargetJava = "java"

// TargetProtobuf identifies Protocol Buffers code generation.
const TargetProtobuf = "protobuf"

// TargetJSONSchema identifies JSON Schema generation.
const TargetJSONSchema = "jsonschema"

// SupportedTargetsText is the user-facing list of supported generation targets.
const SupportedTargetsText = TargetGo + ", " + TargetJava + ", " + TargetProtobuf + ", " + TargetJSONSchema

const (
	goFileExtension         = ".go"
	javaFileExtension       = ".java"
	protobufFileExtension   = ".proto"
	jsonSchemaFileExtension = ".json"
	runtimeDirectoryName    = "runtime"
)

func sanitizeGeneratedOutputDir(outputDir string) (string, error) {
	if outputDir == "" {
		return "", fmt.Errorf("output directory is required")
	}

	cleaned := filepath.Clean(outputDir)
	for _, part := range strings.Split(cleaned, string(filepath.Separator)) {
		if part == ".." {
			return "", fmt.Errorf("output directory must not contain parent traversal")
		}
	}

	return cleaned, nil
}

func createGeneratedFile(outputDir, relativePath string) (*os.File, string, error) {
	paths, err := resolveGeneratedFilePaths(outputDir, relativePath)
	if err != nil {
		return nil, "", err
	}
	if err := ensureGeneratedOutputDirectory(paths.volumeRoot, paths.relativeOutputDir); err != nil {
		return nil, "", err
	}
	file, err := createFileWithinOutputRoot(paths.absoluteOutputDir, paths.relativePath)
	if err != nil {
		return nil, "", err
	}
	return file, filepath.Join(paths.outputDir, paths.relativePath), nil
}

type generatedFilePaths struct {
	outputDir         string
	absoluteOutputDir string
	relativePath      string
	volumeRoot        string
	relativeOutputDir string
}

func resolveGeneratedFilePaths(outputDir, relativePath string) (generatedFilePaths, error) {
	outputDir, err := sanitizeGeneratedOutputDir(outputDir)
	if err != nil {
		return generatedFilePaths{}, err
	}
	if !filepath.IsLocal(relativePath) {
		return generatedFilePaths{}, fmt.Errorf("generated file path must be local to the output directory")
	}
	relativePath = filepath.Clean(relativePath)
	absoluteOutputDir, err := filepath.Abs(outputDir)
	if err != nil {
		return generatedFilePaths{}, fmt.Errorf("resolve output dir: %w", err)
	}
	volumeRoot := filepath.VolumeName(absoluteOutputDir) + string(filepath.Separator)
	relativeOutputDir, err := filepath.Rel(volumeRoot, absoluteOutputDir)
	if err != nil {
		return generatedFilePaths{}, fmt.Errorf("resolve output dir relative to volume root: %w", err)
	}
	if !filepath.IsLocal(relativeOutputDir) {
		return generatedFilePaths{}, fmt.Errorf("output directory must be local to its volume root")
	}
	return generatedFilePaths{
		outputDir: outputDir, absoluteOutputDir: absoluteOutputDir, relativePath: relativePath,
		volumeRoot: volumeRoot, relativeOutputDir: relativeOutputDir,
	}, nil
}

func ensureGeneratedOutputDirectory(volumeRoot, relativeOutputDir string) error {
	root, err := os.OpenRoot(volumeRoot)
	if err != nil {
		return fmt.Errorf("open volume root: %w", err)
	}
	if err := root.MkdirAll(relativeOutputDir, 0755); err != nil {
		return errors.Join(fmt.Errorf("create output dir: %w", err), root.Close())
	}
	if err := root.Close(); err != nil {
		return fmt.Errorf("close volume root: %w", err)
	}
	return nil
}

func createFileWithinOutputRoot(outputDir, relativePath string) (*os.File, error) {
	root, err := os.OpenRoot(outputDir)
	if err != nil {
		return nil, fmt.Errorf("open output dir: %w", err)
	}
	relativeDir := filepath.Dir(relativePath)
	if relativeDir != "." {
		if err := root.MkdirAll(relativeDir, 0755); err != nil {
			return nil, errors.Join(fmt.Errorf("create generated file directory: %w", err), root.Close())
		}
	}
	file, err := root.Create(relativePath)
	closeErr := root.Close()
	if err != nil {
		return nil, fmt.Errorf("create generated file: %w", err)
	}
	if closeErr != nil {
		_ = file.Close()
		return nil, fmt.Errorf("close output dir: %w", closeErr)
	}
	return file, nil
}

// RuleSet represents a collection of rules for a contract.
type RuleSet struct {
	RuleSet  string `yaml:"rule_set"`
	Revision int    `yaml:"revision"`
	Mode     string `yaml:"mode"`
	Rules    []Rule `yaml:"rules"`
}

// Rule represents a single rule with a condition and actions.
type Rule struct {
	ID          string    `yaml:"id"`
	Name        string    `yaml:"name"`
	Description string    `yaml:"description"`
	Priority    int       `yaml:"priority"`
	When        Condition `yaml:"when"`
	Then        []Action  `yaml:"then"`
}

// Condition represents a rule condition with a path, operator, and value.
type Condition struct {
	Path  string `yaml:"path"`
	Op    string `yaml:"op"`
	Value any    `yaml:"value"`
}

// Action represents a rule action, currently supporting event emission.
type Action struct {
	EmitEvent *EmitEvent `yaml:"emit_event"`
}

// EmitEvent represents an event to be emitted when a rule matches.
type EmitEvent struct {
	Type               string            `yaml:"type"`
	PartitionKeyPolicy string            `yaml:"partition_key_policy"`
	Data               map[string]string `yaml:"data"`
}

// Generator produces code from a contract schema and optional rule set.
type Generator struct {
	schema  *domain.ContractSchema
	ruleSet *RuleSet
}

// NewGenerator creates a new Generator with the given contract schema.
func NewGenerator(schema *domain.ContractSchema) *Generator {
	return &Generator{schema: schema}
}

// NewGeneratorWithRules creates a new Generator with the given contract schema and rule set.
func NewGeneratorWithRules(schema *domain.ContractSchema, ruleSet *RuleSet) *Generator {
	return &Generator{schema: schema, ruleSet: ruleSet}
}

// GenerateGo generates Go contract types, typed client, and runtime components.
func (g *Generator) GenerateGo(outputDir string) ([]string, error) {
	var err error
	outputDir, err = sanitizeGeneratedOutputDir(outputDir)
	if err != nil {
		return nil, err
	}

	files, err := g.generate(outputDir, TargetGo, goTemplate, goFileExtension)
	if err != nil {
		return nil, err
	}
	clientFile, err := g.generateTypedClient(outputDir)
	if err != nil {
		return nil, err
	}
	return append(files, clientFile), nil
}

// GenerateJava generates Java contract classes.
func (g *Generator) GenerateJava(outputDir string) ([]string, error) {
	return g.generate(outputDir, TargetJava, javaTemplate, javaFileExtension)
}

// GenerateProtobuf generates Protocol Buffers message definitions.
func (g *Generator) GenerateProtobuf(outputDir string) ([]string, error) {
	return g.generate(outputDir, TargetProtobuf, protobufTemplate, protobufFileExtension)
}

// GenerateJSONSchema generates a JSON Schema document for the contract.
func (g *Generator) GenerateJSONSchema(outputDir string) ([]string, error) {
	return g.generate(outputDir, TargetJSONSchema, jsonschemaTemplate, jsonSchemaFileExtension)
}

// GenerateAll generates Go types, runtime components, evaluator, and service scaffolding.
func (g *Generator) GenerateAll(outputDir string) ([]string, error) {
	var err error
	outputDir, err = sanitizeGeneratedOutputDir(outputDir)
	if err != nil {
		return nil, err
	}

	var allFiles []string

	// Contract types
	files, err := g.GenerateGo(outputDir)
	if err != nil {
		return nil, err
	}
	allFiles = append(allFiles, files...)

	// Runtime (envelope, publisher, consumer)
	files, err = g.GenerateRuntime(outputDir)
	if err != nil {
		return nil, err
	}
	allFiles = append(allFiles, files...)

	// Evaluator from rules
	if g.ruleSet != nil {
		files, err = g.GenerateEvaluator(outputDir)
		if err != nil {
			return nil, err
		}
		allFiles = append(allFiles, files...)

		// Service bootstrap
		files, err = g.GenerateService(outputDir)
		if err != nil {
			return nil, err
		}
		allFiles = append(allFiles, files...)
	}

	return allFiles, nil
}

// GenerateRuntime generates Go envelope, publisher, and consumer types.
// GenerateRuntime generates envelope, publisher, and consumer runtime components.
func (g *Generator) GenerateRuntime(outputDir string) ([]string, error) {
	var err error
	outputDir, err = sanitizeGeneratedOutputDir(outputDir)
	if err != nil {
		return nil, err
	}

	var allFiles []string

	files, err := g.generateEnvelope(outputDir)
	if err != nil {
		return nil, err
	}
	allFiles = append(allFiles, files...)

	files, err = g.generatePublisher(outputDir)
	if err != nil {
		return nil, err
	}
	allFiles = append(allFiles, files...)

	files, err = g.generateConsumer(outputDir)
	if err != nil {
		return nil, err
	}
	allFiles = append(allFiles, files...)

	return allFiles, nil
}

// GenerateEvaluator generates a compiled rule evaluator for the configured rule set.
// GenerateEvaluator generates a Go evaluator from the rule set.
func (g *Generator) GenerateEvaluator(outputDir string) ([]string, error) {
	outputDir, err := sanitizeGeneratedOutputDir(outputDir)
	if err != nil {
		return nil, err
	}
	if g.ruleSet == nil {
		return nil, fmt.Errorf("rule set required for evaluator generation")
	}

	file, filePath, err := createGeneratedFile(outputDir, filepath.Join(TargetGo, runtimeDirectoryName, "evaluator.go"))
	if err != nil {
		return nil, fmt.Errorf("create evaluator.go: %w", err)
	}
	defer file.Close()

	pkgName := sanitizePackageName(g.schema.Name)
	typeName := toPascalCase(g.schema.Name)
	writeGeneratedEvaluatorHeader(file)
	writeGeneratedEvaluatorTypes(file)
	writeGeneratedEvaluatorConstructor(file, g)
	writeGeneratedEvaluatorMethod(file, pkgName, typeName)
	if err := writeGeneratedEvaluatorHelpers(file); err != nil {
		return nil, fmt.Errorf("write getString: %w", err)
	}
	return []string{filePath}, nil
}

func writeGeneratedEvaluatorHeader(file *os.File) {
	fmt.Fprint(file, "// Code generated by FlowRulz. DO NOT EDIT.\n\n")
	fmt.Fprint(file, "package runtime\n\n")
	fmt.Fprint(file, "import (\n")
	fmt.Fprint(file, "\t\"encoding/json\"\n")
	fmt.Fprint(file, "\t\"fmt\"\n")
	fmt.Fprint(file, "\t\"strings\"\n")
	fmt.Fprint(file, "\t\"time\"\n\n")
	fmt.Fprint(file, "\t\"github.com/flowrulz/flow-rulz/internal/domain\"\n")
	fmt.Fprint(file, ")\n\n")
}

func writeGeneratedEvaluatorTypes(file *os.File) {
	writeGeneratedEffectType(file)
	writeGeneratedEvaluatorType(file)
	writeGeneratedCompiledRuleType(file)
	writeGeneratedConditionType(file)
	writeGeneratedEffectTemplateType(file)
}

func writeGeneratedEffectType(file *os.File) {
	fmt.Fprint(file, "// Effect represents an effect to be emitted\n")
	fmt.Fprint(file, "type Effect struct {\n")
	fmt.Fprint(file, "\tType           string\n")
	fmt.Fprint(file, "\tPayload        map[string]any\n")
	fmt.Fprint(file, "\tPartitionKey   string\n")
	fmt.Fprint(file, "}\n\n")
}

func writeGeneratedEvaluatorType(file *os.File) {
	fmt.Fprint(file, "// Evaluator evaluates rules against a payload\n")
	fmt.Fprint(file, "type Evaluator struct {\n")
	fmt.Fprint(file, "\trules []CompiledRule\n")
	fmt.Fprint(file, "}\n\n")
}

func writeGeneratedCompiledRuleType(file *os.File) {
	fmt.Fprint(file, "// CompiledRule is a pre-compiled rule for fast evaluation\n")
	fmt.Fprint(file, "type CompiledRule struct {\n")
	fmt.Fprint(file, "\tID        string\n")
	fmt.Fprint(file, "\tPriority  int\n")
	fmt.Fprint(file, "\tCondition ConditionFunc\n")
	fmt.Fprint(file, "\tEffects   []EffectTemplate\n")
	fmt.Fprint(file, "}\n\n")
}

func writeGeneratedConditionType(file *os.File) {
	fmt.Fprint(file, "// ConditionFunc evaluates a condition against payload\n")
	fmt.Fprint(file, "type ConditionFunc func(payload map[string]any) bool\n\n")
}

func writeGeneratedEffectTemplateType(file *os.File) {
	fmt.Fprint(file, "// EffectTemplate defines an effect to emit with template substitution\n")
	fmt.Fprint(file, "type EffectTemplate struct {\n")
	fmt.Fprint(file, "\tEventType           string\n")
	fmt.Fprint(file, "\tPartitionKeyPolicy  string\n")
	fmt.Fprint(file, "\tDataTemplate        map[string]string\n")
	fmt.Fprint(file, "}\n\n")
}

func writeGeneratedEvaluatorConstructor(file *os.File, generator *Generator) {
	fmt.Fprint(file, "// NewEvaluator creates a new evaluator with compiled rules\n")
	fmt.Fprint(file, "func NewEvaluator() *Evaluator {\n")
	fmt.Fprint(file, "\treturn &Evaluator{\n")
	fmt.Fprint(file, "\t\trules: []CompiledRule{\n")
	for _, rule := range generator.buildCompiledRules() {
		writeGeneratedCompiledRule(file, rule)
	}
	fmt.Fprint(file, "\t\t},\n")
	fmt.Fprint(file, "\t}\n")
	fmt.Fprint(file, "}\n\n")
}

func writeGeneratedCompiledRule(file *os.File, rule CompiledRuleData) {
	fmt.Fprint(file, "\t\t\t{\n")
	fmt.Fprintf(file, "\t\t\t\tID:       %q,\n", rule.ID)
	fmt.Fprintf(file, "\t\t\t\tPriority: %d,\n", rule.Priority)
	fmt.Fprint(file, "\t\t\t\tCondition: func(payload map[string]any) bool {\n")
	fmt.Fprintf(file, "\t\t\t\t\t%s\n", rule.ConditionCode)
	fmt.Fprint(file, "\t\t\t\t},\n")
	fmt.Fprint(file, "\t\t\t\tEffects: []EffectTemplate{\n")
	for _, effect := range rule.Effects {
		writeGeneratedEffectTemplate(file, effect)
	}
	fmt.Fprint(file, "\t\t\t\t},\n")
	fmt.Fprint(file, "\t\t\t},\n")
}

func writeGeneratedEffectTemplate(file *os.File, effect EffectTemplateData) {
	fmt.Fprint(file, "\t\t\t\t\t{\n")
	fmt.Fprintf(file, "\t\t\t\t\t\tEventType:          %q,\n", effect.EventType)
	fmt.Fprintf(file, "\t\t\t\t\t\tPartitionKeyPolicy: %q,\n", effect.PartitionKeyPolicy)
	fmt.Fprint(file, "\t\t\t\t\t\tDataTemplate: map[string]string{\n")
	for key, value := range effect.DataTemplate {
		fmt.Fprintf(file, "\t\t\t\t\t\t\t%q: %q,\n", key, value)
	}
	fmt.Fprint(file, "\t\t\t\t\t\t},\n")
	fmt.Fprint(file, "\t\t\t\t\t},\n")
}

func writeGeneratedEvaluatorMethod(file *os.File, packageName, typeName string) {
	fmt.Fprint(file, "// Evaluate runs all rules against the payload and returns matching effects\n")
	fmt.Fprintf(file, "func (e *Evaluator) Evaluate(payload *%s.%s) ([]Effect, error) {\n", packageName, typeName)
	fmt.Fprint(file, "\t// Convert payload to map for evaluation\n")
	fmt.Fprint(file, "\tdata, err := json.Marshal(payload)\n")
	fmt.Fprint(file, "\tif err != nil {\n\t\treturn nil, err\n\t}\n")
	fmt.Fprint(file, "\tvar payloadMap map[string]any\n")
	fmt.Fprint(file, "\tif err := json.Unmarshal(data, &payloadMap); err != nil {\n\t\treturn nil, err\n\t}\n\n")
	fmt.Fprint(file, "\tvar effects []Effect\n")
	fmt.Fprint(file, "\tfor _, rule := range e.rules {\n")
	fmt.Fprint(file, "\t\tif rule.Condition(payloadMap) {\n")
	fmt.Fprint(file, "\t\t\tfor _, effTmpl := range rule.Effects {\n")
	fmt.Fprint(file, "\t\t\t\teffect := Effect{\n")
	fmt.Fprint(file, "\t\t\t\t\tType: effTmpl.EventType,\n")
	fmt.Fprint(file, "\t\t\t\t\tPartitionKey: computePartitionKey(payloadMap, effTmpl.PartitionKeyPolicy),\n")
	fmt.Fprint(file, "\t\t\t\t}\n")
	fmt.Fprint(file, "\t\t\t\teffect.Payload = substituteTemplate(effTmpl.DataTemplate, payloadMap)\n")
	fmt.Fprint(file, "\t\t\t\teffects = append(effects, effect)\n")
	fmt.Fprint(file, "\t\t\t}\n\t\t}\n\t}\n")
	fmt.Fprint(file, "\treturn effects, nil\n")
	fmt.Fprint(file, "}\n\n")
}

func writeGeneratedEvaluatorHelpers(file *os.File) error {
	fmt.Fprint(file, "// Helper functions\n")
	writeGeneratedGetFloat64(file)
	if err := writeGeneratedGetString(file); err != nil {
		return err
	}
	writeGeneratedComputePartitionKey(file)
	writeGeneratedSubstituteTemplate(file)
	writeGeneratedSubstituteValue(file)
	return nil
}

func writeGeneratedGetFloat64(file *os.File) {
	fmt.Fprint(file, "func getFloat64(m map[string]any, key string) (float64, bool) {\n")
	fmt.Fprint(file, "\tswitch v := m[key].(type) {\n")
	fmt.Fprint(file, "\tcase float64:\n\t\treturn v, true\n")
	fmt.Fprint(file, "\tcase int:\n\t\treturn float64(v), true\n")
	fmt.Fprint(file, "\tcase int64:\n\t\treturn float64(v), true\n")
	fmt.Fprint(file, "\tcase json.Number:\n\t\tf, _ := v.Float64()\n\t\treturn f, true\n")
	fmt.Fprint(file, "\t}\n\treturn 0, false\n}\n\n")
}

func writeGeneratedGetString(file *os.File) error {
	fmt.Fprint(file, "func getString(m map[string]any, key string) (string, bool) {\n")
	fmt.Fprint(file, "\tv, ok := m[key]\n\tif !ok {\n\t\treturn \"\", false\n\t}\n")
	fmt.Fprint(file, "\tswitch v := v.(type) {\n\tcase string:\n\t\treturn v, true\n")
	fmt.Fprint(file, "\tcase json.Number:\n\t\treturn v.String(), true\n\t}\n")
	if _, err := file.WriteString("\treturn fmt.Sprintf(\"%v\", v), true\n"); err != nil {
		return err
	}
	fmt.Fprint(file, "}\n\n")
	return nil
}

func writeGeneratedComputePartitionKey(file *os.File) {
	fmt.Fprint(file, "func computePartitionKey(payload map[string]any, policy string) string {\n")
	fmt.Fprint(file, "\tswitch policy {\n\tcase \"inherit\":\n")
	fmt.Fprint(file, "\t\tif pk, ok := getString(payload, \"customer_id\"); ok {\n\t\t\treturn pk\n\t\t}\n")
	fmt.Fprint(file, "\t\tif pk, ok := getString(payload, \"partition_key\"); ok {\n\t\t\treturn pk\n\t\t}\n")
	fmt.Fprint(file, "\t\treturn \"default\"\n\tdefault:\n\t\treturn policy\n\t}\n}\n\n")
}

func writeGeneratedSubstituteTemplate(file *os.File) {
	fmt.Fprint(file, "func substituteTemplate(tmpl map[string]string, payload map[string]any) map[string]any {\n")
	fmt.Fprint(file, "\tresult := make(map[string]any)\n")
	fmt.Fprint(file, "\tfor k, v := range tmpl {\n\t\tresult[k] = substituteValue(v, payload)\n\t}\n")
	fmt.Fprint(file, "\treturn result\n}\n\n")
}

func writeGeneratedSubstituteValue(file *os.File) {
	fmt.Fprint(file, "func substituteValue(val string, payload map[string]any) any {\n")
	fmt.Fprint(file, "\tval = strings.TrimSpace(val)\n\n")
	fmt.Fprint(file, "\tif val == \"{{new_id}}\" {\n\t\treturn domain.NewID()\n\t}\n")
	fmt.Fprint(file, "\tif val == \"{{now}}\" {\n\t\treturn time.Now().UTC().Format(time.RFC3339)\n\t}\n\n")
	fmt.Fprint(file, "\tif strings.HasPrefix(val, \"{{$.\") && strings.HasSuffix(val, \"}}\") {\n")
	fmt.Fprint(file, "\t\tkey := strings.TrimPrefix(val, \"{{$.\")\n")
	fmt.Fprint(file, "\t\tkey = strings.TrimSuffix(key, \"}}\")\n")
	fmt.Fprint(file, "\t\tif v, ok := payload[key]; ok {\n\t\t\treturn v\n\t\t}\n\t\treturn nil\n\t}\n\n")
	fmt.Fprint(file, "\tif strings.HasPrefix(val, \"{{\") && strings.HasSuffix(val, \"}}\") {\n")
	fmt.Fprint(file, "\t\tkey := strings.TrimPrefix(val, \"{{\")\n")
	fmt.Fprint(file, "\t\tkey = strings.TrimSuffix(key, \"}}\")\n")
	fmt.Fprint(file, "\t\tif v, ok := payload[key]; ok {\n\t\t\treturn v\n\t\t}\n\t\treturn nil\n\t}\n\n")
	fmt.Fprint(file, "\treturn val\n}\n")
}

// GenerateService generates a service that wires together consumer, evaluator, and publishers.
// GenerateService generates a Go service bootstrap from the rule set.
func (g *Generator) GenerateService(outputDir string) ([]string, error) {
	var err error
	outputDir, err = sanitizeGeneratedOutputDir(outputDir)
	if err != nil {
		return nil, err
	}

	if g.ruleSet == nil {
		return nil, fmt.Errorf("rule set required for service generation")
	}

	pkgName := sanitizePackageName(g.schema.Name)
	f, filePath, err := createGeneratedFile(outputDir, filepath.Join(TargetGo, runtimeDirectoryName, "service.go"))
	if err != nil {
		return nil, fmt.Errorf("create service.go: %w", err)
	}
	defer f.Close()

	typeName := toPascalCase(g.schema.Name)

	// Collect unique output event types from rules
	outputEvents := make(map[string]bool)
	for _, rule := range g.ruleSet.Rules {
		for _, action := range rule.Then {
			if action.EmitEvent != nil {
				outputEvents[action.EmitEvent.Type] = true
			}
		}
	}

	// Convert to sorted slice
	var outputEventTypes []string
	for k := range outputEvents {
		outputEventTypes = append(outputEventTypes, k)
	}

	data := struct {
		Schema       *domain.ContractSchema
		TypeName     string
		PackageName  string
		EventName    string
		ContractPkg  string
		ContractType string
		RuleSet      *RuleSet
		OutputEvents []string
		NumShards    uint32
	}{
		Schema:       g.schema,
		TypeName:     typeName,
		PackageName:  pkgName,
		EventName:    g.schema.Name,
		ContractPkg:  pkgName,
		ContractType: typeName,
		RuleSet:      g.ruleSet,
		OutputEvents: outputEventTypes,
		NumShards:    4096,
	}

	if err := serviceTemplate.Execute(f, data); err != nil {
		return nil, fmt.Errorf("execute service template: %w", err)
	}

	return []string{filePath}, nil
}

func (g *Generator) generate(outputDir, subdir string, tmpl *template.Template, ext string) ([]string, error) {
	var err error
	outputDir, err = sanitizeGeneratedOutputDir(outputDir)
	if err != nil {
		return nil, err
	}

	typeName := toPascalCase(g.schema.Name)
	if err := validateGeneratedTypeName(typeName); err != nil {
		return nil, err
	}
	fileName := strings.ToLower(typeName) + ext
	f, filePath, err := createGeneratedFile(outputDir, filepath.Join(subdir, fileName))
	if err != nil {
		return nil, fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	data := struct {
		Schema        *domain.ContractSchema
		TypeName      string
		PackageName   string
		GoPackageName string
		Namespace     string
		Fields        []FieldInfo
		NestedTypes   []NestedType
	}{
		Schema:        g.schema,
		TypeName:      typeName,
		PackageName:   strings.ToLower(g.schema.Name),
		GoPackageName: sanitizePackageName(g.schema.Name),
		Namespace:     g.schema.Namespace,
	}
	data.Fields, data.NestedTypes = g.buildFieldsAndNestedTypes()

	if err := tmpl.Execute(f, data); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}

	return []string{filePath}, nil
}

var generatedTypeNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)

// validateGeneratedTypeName ensures schema-derived names cannot become path components.
func validateGeneratedTypeName(name string) error {
	if !generatedTypeNamePattern.MatchString(name) {
		return fmt.Errorf("invalid generated type name %q: expected ASCII letters and digits, starting with a letter", name)
	}
	return nil
}

func (g *Generator) generateTypedClient(outputDir string) (string, error) {
	var err error
	outputDir, err = sanitizeGeneratedOutputDir(outputDir)
	if err != nil {
		return "", err
	}

	typeName := toPascalCase(g.schema.Name)
	if err := validateGeneratedTypeName(typeName); err != nil {
		return "", err
	}
	file, filePath, err := createGeneratedFile(outputDir, filepath.Join(TargetGo, strings.ToLower(typeName)+"_client.go"))
	if err != nil {
		return "", fmt.Errorf("create Go client file: %w", err)
	}
	defer file.Close()

	data := struct {
		Schema        *domain.ContractSchema
		TypeName      string
		GoPackageName string
	}{
		Schema:        g.schema,
		TypeName:      typeName,
		GoPackageName: sanitizePackageName(g.schema.Name),
	}
	if err := typedClientTemplate.Execute(file, data); err != nil {
		return "", fmt.Errorf("execute Go client template: %w", err)
	}
	return filePath, nil
}

// FieldInfo represents a field for code generation templates.
type FieldInfo struct {
	Name        string
	GoName      string
	GoType      string
	JavaType    string
	ProtoType   string
	JSONSchema  map[string]interface{}
	Required    bool
	Description string
}

// NestedType is a Go struct generated for an object-typed contract property.
type NestedType struct {
	Name   string
	Fields []FieldInfo
}

func (g *Generator) buildFieldsAndNestedTypes() ([]FieldInfo, []NestedType) {
	var nested []NestedType
	fields := g.fieldInfos(g.schema.Fields, &nested)
	return fields, nested
}

// goFieldType maps a contract field to a Go type, emitting nested structs for object properties.
func (g *Generator) goFieldType(name string, field domain.ContractField, nested *[]NestedType) string {
	switch {
	case field.Type == "object" && len(field.Properties) > 0:
		typeName := toGoIdentifier(name)
		*nested = append(*nested, NestedType{Name: typeName})
		index := len(*nested) - 1
		children := g.fieldInfos(field.Properties, nested)
		(*nested)[index].Fields = children
		return typeName
	case field.Type == "array" && field.Items != nil:
		return "[]" + g.goFieldType(strings.TrimSuffix(name, "s"), *field.Items, nested)
	default:
		return g.mapGoType(field.Type)
	}
}

func (g *Generator) fieldInfos(source map[string]domain.ContractField, nested *[]NestedType) []FieldInfo {
	var fields []FieldInfo
	names := make([]string, 0, len(source))
	for name := range source {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		field := source[name]
		f := FieldInfo{
			Name:        name,
			GoName:      toGoIdentifier(name),
			GoType:      g.goFieldType(name, field, nested),
			JavaType:    g.mapJavaType(field.Type),
			ProtoType:   g.mapProtoType(field.Type),
			JSONSchema:  g.mapJSONSchema(field),
			Required:    field.Required,
			Description: field.Description,
		}
		fields = append(fields, f)
	}
	return fields
}

func (g *Generator) mapGoType(t string) string {
	switch t {
	case "string":
		return "string"
	case "integer", "int", "int64":
		return "int64"
	case "number", "float", "double":
		return "float64"
	case "boolean", "bool":
		return "bool"
	case "object":
		return "map[string]interface{}"
	case "array":
		return "[]interface{}"
	default:
		return "interface{}"
	}
}

func (g *Generator) mapJavaType(t string) string {
	switch t {
	case "string":
		return "String"
	case "integer", "int", "int64":
		return "Long"
	case "number", "float", "double":
		return "Double"
	case "boolean", "bool":
		return "Boolean"
	case "object":
		return "Map<String, Object>"
	case "array":
		return "List<Object>"
	default:
		return "Object"
	}
}

func (g *Generator) mapProtoType(t string) string {
	switch t {
	case "string":
		return "string"
	case "integer", "int", "int64":
		return "int64"
	case "number", "float", "double":
		return "double"
	case "boolean", "bool":
		return "bool"
	case "object":
		return "google.protobuf.Struct"
	case "array":
		return "repeated google.protobuf.Value"
	default:
		return "string"
	}
}

func (g *Generator) mapJSONSchema(field domain.ContractField) map[string]interface{} {
	schema := map[string]interface{}{
		"type": field.Type,
	}
	if field.Description != "" {
		schema["description"] = field.Description
	}
	if field.Format != "" {
		schema["format"] = field.Format
	}
	if len(field.Enum) > 0 {
		schema["enum"] = field.Enum
	}
	if field.Default != nil {
		schema["default"] = field.Default
	}
	if field.Items != nil {
		schema["items"] = field.Items
	}
	if field.Properties != nil {
		props := make(map[string]interface{})
		for k, v := range field.Properties {
			props[k] = g.mapJSONSchema(v)
		}
		schema["properties"] = props
	}
	return schema
}

func toPascalCase(s string) string {
	parts := strings.Split(s, ".")
	last := parts[len(parts)-1]
	segments := strings.Split(last, "_")
	for i, seg := range segments {
		if len(seg) > 0 {
			segments[i] = strings.ToUpper(seg[:1]) + seg[1:]
		}
	}
	return strings.Join(segments, "")
}

var goTemplate = template.Must(template.New(TargetGo).Parse(`// Code generated by FlowRulz. DO NOT EDIT.

package {{.GoPackageName}}

{{if .Schema.Description}}// {{.Schema.Description}}
{{end}}// {{.TypeName}} represents the {{.Schema.Name}} contract v{{.Schema.Version}}
type {{.TypeName}} struct {
{{- range .Fields}}
		{{.GoName}} {{.GoType}} ` + "`" + `json:"{{.Name}}{{if not .Required}},omitempty{{end}}"` + "`" + `
{{- end}}
}
{{range .NestedTypes}}
// {{.Name}} represents a nested type in {{$.Schema.Name}}
type {{.Name}} struct {
{{- range .Fields}}
		{{.GoName}} {{.GoType}} ` + "`" + `json:"{{.Name}}{{if not .Required}},omitempty{{end}}"` + "`" + `
{{- end}}
}
{{end}}`))

var typedClientTemplate = template.Must(template.New("typed-client").Parse(`// Code generated by FlowRulz. DO NOT EDIT.

package {{.GoPackageName}}

import (
		"context"
		"encoding/json"
		"fmt"

		flowrulz "github.com/flowrulz/flow-rulz-sdk/go/flowrulz"
)

// DefaultRuleSet is the rule set associated with the {{.Schema.Name}} contract.
const DefaultRuleSet = {{printf "%q" .Schema.Name}}

// Client provides typed event methods for the {{.Schema.Name}} contract.
type Client struct {
		client *flowrulz.Client
}

// NewClient wraps a configured FlowRulz SDK client.
func NewClient(client *flowrulz.Client) *Client {
		return &Client{client: client}
}

// Send sends a typed payload using the contract's default rule set.
func (c *Client) Send(ctx context.Context, payload *{{.TypeName}}, opts ...flowrulz.SendOption) error {
		return c.SendToRuleSet(ctx, DefaultRuleSet, payload, opts...)
}

// SendToRuleSet sends a typed payload using the supplied rule set.
func (c *Client) SendToRuleSet(ctx context.Context, ruleSet string, payload *{{.TypeName}}, opts ...flowrulz.SendOption) error {
		if c == nil || c.client == nil {
				return flowrulz.ErrInvalidArgument("client is required")
		}
		return c.client.Send(ctx, ruleSet, payload, opts...)
}

// Subscribe receives live events for the contract's default rule set.
func (c *Client) Subscribe(ctx context.Context, handler func(context.Context, *{{.TypeName}}, *flowrulz.Envelope) error) error {
		return c.SubscribeRuleSet(ctx, DefaultRuleSet, handler)
}

// SubscribeRuleSet receives live events for the supplied rule set.
func (c *Client) SubscribeRuleSet(ctx context.Context, ruleSet string, handler func(context.Context, *{{.TypeName}}, *flowrulz.Envelope) error) error {
		if c == nil || c.client == nil {
				return flowrulz.ErrInvalidArgument("client is required")
		}
		if handler == nil {
				return flowrulz.ErrInvalidArgument("event handler is required")
		}
		return c.client.Subscribe(ctx, ruleSet, func(ctx context.Context, envelope *flowrulz.Envelope) error {
				if envelope == nil {
						return flowrulz.ErrInvalidArgument("event envelope is required")
				}
				var payload {{.TypeName}}
				if err := json.Unmarshal(envelope.Data, &payload); err != nil {
						return fmt.Errorf("decode %s payload: %w", envelope.Type, err)
				}
				return handler(ctx, &payload, envelope)
		})
}
`))

var javaTemplate = template.Must(template.New(TargetJava).Parse(`// Code generated by FlowRulz. DO NOT EDIT.

package {{.PackageName}};

import com.fasterxml.jackson.annotation.JsonProperty;
{{if .Schema.Description}}/**
* {{.Schema.Description}}
*/
{{end}}public class {{.TypeName}} {
{{- range .Fields}}
	@JsonProperty("{{.Name}}")
	private {{.JavaType}} {{.Name}};
{{- end}}

	// Getters and setters
{{- range .Fields}}
	public {{.JavaType}} get{{.Name}}() {
		return {{.Name}};
	}

	public void set{{.Name}}({{.JavaType}} {{.Name}}) {
		this.{{.Name}} = {{.Name}};
	}
{{- end}}
}
`))

var protobufTemplate = template.Must(template.New(TargetProtobuf).Funcs(template.FuncMap{
	"add1": func(i int) int {
		return i + 1
	},
}).Parse(`// Code generated by FlowRulz. DO NOT EDIT.

syntax = "proto3";

package {{.PackageName}};

option go_package = "{{.Namespace}}/{{.PackageName}}";

{{if .Schema.Description}}// {{.Schema.Description}}
{{end}}message {{.TypeName}} {
{{- range $i, $field := .Fields}}
	{{.ProtoType}} {{.Name}} = {{add1 $i}};
{{- end}}
}
`))

var jsonschemaTemplate = template.Must(template.New(TargetJSONSchema).Funcs(template.FuncMap{
	"toJSON": func(v interface{}) string {
		b, _ := json.MarshalIndent(v, "", "  ")
		return string(b)
	},
	"sub": func(a, b int) int {
		return a - b
	},
	"add1": func(i int) int {
		return i + 1
	},
}).Parse(`{
"$schema": "http://json-schema.org/draft-07/schema#",
"$id": "https://flowrulz.io/schemas/{{.Schema.Name}}-v{{.Schema.Version}}.json",
"title": "{{.Schema.Name}}",
"type": "object",
{{if .Schema.Description}}"description": "{{.Schema.Description}}",{{end}}
"properties": {
{{- range $i, $field := .Fields}}
	"{{.Name}}": {{toJSON .JSONSchema}}{{if ne $i (sub (len $.Fields) 1)}},{{end}}
{{- end}}
},
"required": [
{{- range $i, $field := .Fields}}
	{{if .Required}}"{{.Name}}"{{if ne $i (sub (len $.Fields) 1)}},{{end}}{{end}}
{{- end}}
],
"additionalProperties": false
}
`))

var envelopeTemplate = template.Must(template.New("envelope").Parse(`// Code generated by FlowRulz. DO NOT EDIT.

package runtime

import (
		"encoding/json"
		"time"

		"github.com/flowrulz/flow-rulz-sdk/go/runtime"
		"github.com/google/uuid"
)

// Envelope wraps a typed payload with metadata for event processing
type Envelope struct {
		ID           string            ` + "`" + `json:"id"` + "`" + `
		Type         string            ` + "`" + `json:"type"` + "`" + `
		TenantID     string            ` + "`" + `json:"tenant_id"` + "`" + `
		PartitionKey string            ` + "`" + `json:"partition_key"` + "`" + `
		WorkflowID   string            ` + "`" + `json:"workflow_id,omitempty"` + "`" + `
		OccurredAt   time.Time         ` + "`" + `json:"occurred_at"` + "`" + `
		Data         json.RawMessage   ` + "`" + `json:"data"` + "`" + `
		Headers      map[string]string ` + "`" + `json:"headers,omitempty"` + "`" + `
}

// NewEnvelope creates an envelope for the {{.EventName}} event
func NewEnvelope(payload *{{.ContractPkg}}.{{.ContractType}}, tenantID, partitionKey string) *Envelope {
		data, _ := json.Marshal(payload)
		return &Envelope{
				ID:           uuid.New().String(),
				Type:         "{{.EventName}}",
				TenantID:     tenantID,
				PartitionKey: partitionKey,
				OccurredAt:   time.Now().UTC(),
				Data:         data,
		}
}

// Payload unmarshals the envelope data into the typed payload
func (e *Envelope) Payload() (*{{.ContractPkg}}.{{.ContractType}}, error) {
		var payload {{.ContractPkg}}.{{.ContractType}}
		if err := json.Unmarshal(e.Data, &payload); err != nil {
				return nil, err
		}
		return &payload, nil
}

// Shard computes the virtual shard for this envelope
func (e *Envelope) Shard(numShards uint32) uint32 {
		return runtime.ComputeShard(e.TenantID, e.PartitionKey, numShards)
}

// Marshal serializes the envelope to JSON
func (e *Envelope) Marshal() ([]byte, error) {
		return json.Marshal(e)
}
`))

var publisherTemplate = template.Must(template.New("publisher").Parse(`// Code generated by FlowRulz. DO NOT EDIT.

package runtime

import (
		"context"
		"encoding/json"

		"github.com/nats-io/nats.go/jetstream"
)

// Publisher handles publishing {{.EventName}} events
type Publisher struct {
		js        jetstream.JetStream
		stream    string
		numShards uint32
}

// NewPublisher creates a new publisher for {{.EventName}}
func NewPublisher(js jetstream.JetStream, stream string, numShards uint32) *Publisher {
		return &Publisher{
				js:        js,
				stream:    stream,
				numShards: numShards,
		}
}

// Publish publishes a {{.EventName}} event
func (p *Publisher) Publish(ctx context.Context, payload *{{.ContractPkg}}.{{.ContractType}}, tenantID, partitionKey string) error {
		env := NewEnvelope(payload, tenantID, partitionKey)
		data, err := json.Marshal(env)
		if err != nil {
				return err
		}

		shard := env.Shard(p.numShards)
		subject := "events.shard." + string(rune('0'+shard%10))

		_, err = p.js.Publish(ctx, subject, data)
		return err
}

// PublishToShard publishes to a specific shard
func (p *Publisher) PublishToShard(ctx context.Context, payload *{{.ContractPkg}}.{{.ContractType}}, tenantID, partitionKey string, shard uint32) error {
		env := NewEnvelope(payload, tenantID, partitionKey)
		data, err := json.Marshal(env)
		if err != nil {
				return err
		}

		subject := "events.shard." + string(rune('0'+shard%10))
		_, err = p.js.Publish(ctx, subject, data)
		return err
}
`))

var consumerTemplate = template.Must(template.New("consumer").Parse(`// Code generated by FlowRulz. DO NOT EDIT.

package runtime

import (
		"context"
		"encoding/json"
		"time"

		"github.com/nats-io/nats.go/jetstream"
)

// HandlerFunc is the function signature for handling {{.EventName}} events
type HandlerFunc func(ctx context.Context, payload *{{.ContractPkg}}.{{.ContractType}}, envelope *Envelope) error

// Consumer handles consuming {{.EventName}} events
type Consumer struct {
		js        jetstream.JetStream
		stream    string
		consumer  string
		handler   HandlerFunc
		numShards uint32
}

// NewConsumer creates a new consumer for {{.EventName}}
func NewConsumer(js jetstream.JetStream, stream, consumer string, numShards uint32, handler HandlerFunc) *Consumer {
		return &Consumer{
				js:        js,
				stream:    stream,
				consumer:  consumer,
				handler:   handler,
				numShards: numShards,
		}
}

// Start begins consuming events
func (c *Consumer) Start(ctx context.Context) error {
		jsConsumer, err := c.js.Consumer(ctx, c.stream, c.consumer)
		if err != nil {
				return err
		}

		for {
				select {
				case <-ctx.Done():
						return ctx.Err()
				default:
						batch, err := jsConsumer.Fetch(10, jetstream.FetchMaxWait(5*time.Second))
						if err != nil {
								continue
						}

						for msg := range batch.Messages() {
								var env Envelope
								if err := json.Unmarshal(msg.Data(), &env); err != nil {
										msg.Nak()
										continue
								}

								payload, err := env.Payload()
								if err != nil {
										msg.Nak()
										continue
								}

								if err := c.handler(ctx, payload, &env); err != nil {
										msg.Nak()
										continue
								}

								msg.Ack()
						}
				}
		}
}
`))

var serviceTemplate = template.Must(template.New("service").Parse(`// Code generated by FlowRulz. DO NOT EDIT.

package runtime

import (
		"context"
		"fmt"

		"github.com/nats-io/nats.go/jetstream"
)

// Service wires together consumer, evaluator, and publishers for {{.EventName}}
type Service struct {
		consumer   *Consumer
		evaluator  *Evaluator
		publishers map[string]*Publisher
		js         jetstream.JetStream
		stream     string
		numShards  uint32
)

// ServiceConfig holds configuration for the service
type ServiceConfig struct {
		JS        jetstream.JetStream
		Stream    string
		Consumer  string
		NumShards uint32
		Handler   func(ctx context.Context, eventType string, payload any, envelope *Envelope) error
}

// NewService creates a new service for {{.EventName}}
func NewService(config ServiceConfig) *Service {
		if config.NumShards == 0 {
				config.NumShards = 4096
		}

		s := &Service{
				consumer:   NewConsumer(config.JS, config.Stream, config.Consumer, config.NumShards, s.handleEvent),
				evaluator:  NewEvaluator(),
				publishers: make(map[string]*Publisher),
				js:         config.JS,
				stream:     config.Stream,
				numShards:  config.NumShards,
		}

		// Create publishers for all output event types
{{- range .OutputEvents}}
		s.publishers["{{.}}"] = NewPublisher(config.JS, config.Stream, config.NumShards)
{{- end}}

		return s
}

// Start begins processing events
func (s *Service) Start(ctx context.Context) error {
		return s.consumer.Start(ctx)
}

// handleEvent processes incoming events through the evaluator and publishes effects
func (s *Service) handleEvent(ctx context.Context, payload *{{.ContractPkg}}.{{.ContractType}}, envelope *Envelope) error {
		effects, err := s.evaluator.Evaluate(payload)
		if err != nil {
				return fmt.Errorf("evaluate: %w", err)
		}

		for _, effect := range effects {
				publisher, ok := s.publishers[effect.Type]
				if !ok {
						// Publisher not configured for this effect type
						continue
				}

				partitionKey := effect.PartitionKey
				if partitionKey == "" && envelope != nil {
						partitionKey = envelope.PartitionKey
				}

				if err := publisher.Publish(ctx, effect.Payload, envelope.TenantID, partitionKey); err != nil {
						return fmt.Errorf("publish %s: %w", effect.Type, err)
				}
		}

		return nil
}
`))

func sanitizePackageName(name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, "-", "_")
	return strings.ToLower(name)
}

func toGoIdentifier(name string) string {
	var identifier strings.Builder
	capitalize := true
	for _, char := range name {
		if !unicode.IsLetter(char) && !unicode.IsDigit(char) {
			capitalize = true
			continue
		}
		if identifier.Len() == 0 && unicode.IsDigit(char) {
			identifier.WriteString("Field")
		}
		if capitalize {
			char = unicode.ToUpper(char)
			capitalize = false
		}
		identifier.WriteRune(char)
	}
	if identifier.Len() == 0 {
		return "Field"
	}
	return identifier.String()
}

// CompiledRuleData represents a compiled rule for code generation templates.
type CompiledRuleData struct {
	ID            string
	Priority      int
	ConditionCode string
	Effects       []EffectTemplateData
}

// EffectTemplateData represents an effect template for code generation.
type EffectTemplateData struct {
	EventType          string
	PartitionKeyPolicy string
	DataTemplate       map[string]string
}

func (g *Generator) buildCompiledRules() []CompiledRuleData {
	if g.ruleSet == nil {
		return nil
	}

	var compiled []CompiledRuleData
	for _, rule := range g.ruleSet.Rules {
		conditionCode := g.buildConditionCode(rule.When)

		var effects []EffectTemplateData
		for _, action := range rule.Then {
			if action.EmitEvent != nil {
				effects = append(effects, EffectTemplateData{
					EventType:          action.EmitEvent.Type,
					PartitionKeyPolicy: action.EmitEvent.PartitionKeyPolicy,
					DataTemplate:       action.EmitEvent.Data,
				})
			}
		}

		compiled = append(compiled, CompiledRuleData{
			ID:            rule.ID,
			Priority:      rule.Priority,
			ConditionCode: conditionCode,
			Effects:       effects,
		})
	}

	// Sort by priority descending
	for i := 0; i < len(compiled)-1; i++ {
		for j := i + 1; j < len(compiled); j++ {
			if compiled[i].Priority < compiled[j].Priority {
				compiled[i], compiled[j] = compiled[j], compiled[i]
			}
		}
	}

	return compiled
}

func (g *Generator) buildConditionCode(cond Condition) string {
	path := strings.TrimPrefix(cond.Path, "$.")

	var op string
	switch cond.Op {
	case "gt":
		op = ">"
	case "gte":
		op = ">="
	case "lt":
		op = "<"
	case "lte":
		op = "<="
	case "eq":
		op = "=="
	case "neq":
		op = "!="
	case "contains":
		return fmt.Sprintf("strings.Contains(getString(payload, %q), %v)", path, cond.Value)
	case "in":
		return fmt.Sprintf("containsString(payload, %q, %v)", path, cond.Value)
	default:
		op = "=="
	}

	// For numeric comparisons
	if isNumeric(cond.Value) {
		return fmt.Sprintf("val, ok := getFloat64(payload, %q); ok && val %s %v", path, op, cond.Value)
	}

	// For string comparisons
	return fmt.Sprintf("val, ok := getString(payload, %q); ok && val %s %q", path, op, cond.Value)
}

func isNumeric(v any) bool {
	switch v.(type) {
	case int, int64, float64, float32:
		return true
	}
	if s, ok := v.(string); ok {
		_, err := fmt.Sscanf(s, "%f", new(float64))
		return err == nil
	}
	return false
}

func (g *Generator) generateEnvelope(outputDir string) ([]string, error) {
	pkgName := sanitizePackageName(g.schema.Name)
	f, filePath, err := createGeneratedFile(outputDir, filepath.Join(TargetGo, runtimeDirectoryName, "envelope.go"))
	if err != nil {
		return nil, fmt.Errorf("create envelope.go: %w", err)
	}
	defer f.Close()

	typeName := toPascalCase(g.schema.Name)

	data := struct {
		Schema       *domain.ContractSchema
		TypeName     string
		PackageName  string
		EventName    string
		ContractPkg  string
		ContractType string
	}{
		Schema:       g.schema,
		TypeName:     typeName,
		PackageName:  pkgName,
		EventName:    g.schema.Name,
		ContractPkg:  pkgName,
		ContractType: typeName,
	}

	if err := envelopeTemplate.Execute(f, data); err != nil {
		return nil, fmt.Errorf("execute envelope template: %w", err)
	}

	return []string{filePath}, nil
}

func (g *Generator) generatePublisher(outputDir string) ([]string, error) {
	pkgName := sanitizePackageName(g.schema.Name)
	f, filePath, err := createGeneratedFile(outputDir, filepath.Join(TargetGo, runtimeDirectoryName, "publisher.go"))
	if err != nil {
		return nil, fmt.Errorf("create publisher.go: %w", err)
	}
	defer f.Close()

	typeName := toPascalCase(g.schema.Name)

	data := struct {
		Schema       *domain.ContractSchema
		TypeName     string
		PackageName  string
		EventName    string
		ContractPkg  string
		ContractType string
	}{
		Schema:       g.schema,
		TypeName:     typeName,
		PackageName:  pkgName,
		EventName:    g.schema.Name,
		ContractPkg:  pkgName,
		ContractType: typeName,
	}

	if err := publisherTemplate.Execute(f, data); err != nil {
		return nil, fmt.Errorf("execute publisher template: %w", err)
	}

	return []string{filePath}, nil
}

func (g *Generator) generateConsumer(outputDir string) ([]string, error) {
	pkgName := sanitizePackageName(g.schema.Name)
	f, filePath, err := createGeneratedFile(outputDir, filepath.Join(TargetGo, runtimeDirectoryName, "consumer.go"))
	if err != nil {
		return nil, fmt.Errorf("create consumer.go: %w", err)
	}
	defer f.Close()

	typeName := toPascalCase(g.schema.Name)

	data := struct {
		Schema       *domain.ContractSchema
		TypeName     string
		PackageName  string
		EventName    string
		ContractPkg  string
		ContractType string
	}{
		Schema:       g.schema,
		TypeName:     typeName,
		PackageName:  pkgName,
		EventName:    g.schema.Name,
		ContractPkg:  pkgName,
		ContractType: typeName,
	}

	if err := consumerTemplate.Execute(f, data); err != nil {
		return nil, fmt.Errorf("execute consumer template: %w", err)
	}

	return []string{filePath}, nil
}
