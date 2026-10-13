export interface FlowRulzBranch { id: string; emits: string[]; }
export type FlowReachability = "entry" | "rule-set" | "unreachable";
export interface FlowNode { ruleSet: string; level: number; branches: FlowRulzBranch[]; reachability: FlowReachability; }

// The rule graph only proves rule-set-to-rule-set edges. External service subscriptions
// are resolved separately from the domain/service registry and must never be inferred here.
export function layoutRuleFlow(parsed: Map<string, FlowRulzBranch[]>, startRuleSet: string): FlowNode[] {
  if (!parsed.has(startRuleSet)) return [];
  const levels = new Map<string, number>([[startRuleSet, 0]]);
  const queue = [startRuleSet];
  for (let cursor = 0; cursor < queue.length; cursor++) {
    const name = queue[cursor];
    for (const branch of parsed.get(name) ?? []) {
      for (const next of branch.emits) {
        if (parsed.has(next) && !levels.has(next)) {
          levels.set(next, levels.get(name)! + 1);
          queue.push(next);
        }
      }
    }
  }
  const reached = Math.max(...levels.values());
  const nodes: FlowNode[] = [...levels.entries()].map(([ruleSet, level]) => ({
    ruleSet, level, branches: parsed.get(ruleSet) ?? [],
    reachability: level === 0 ? "entry" : "rule-set",
  }));
  for (const [ruleSet, branches] of parsed) {
    if (!levels.has(ruleSet)) nodes.push({ ruleSet, level: reached + 1, branches, reachability: "unreachable" });
  }
  return nodes.sort((a, b) => a.level - b.level || a.ruleSet.localeCompare(b.ruleSet));
}

type UnknownRecord = Record<string, unknown>;
const isRecord = (value: unknown): value is UnknownRecord => typeof value === "object" && value !== null && !Array.isArray(value);
const asRecord = (value: unknown): UnknownRecord => isRecord(value) ? value : {};
const asArray = (value: unknown): unknown[] => Array.isArray(value) ? value : [];

// Parse stored revisions defensively; malformed rows do not crash the visualization.
export function branchesFromRevision(revision: unknown): FlowRulzBranch[] {
  const root = asRecord(revision);
  const source = asRecord(root.source);
  const rules = Array.isArray(source.rules) ? source.rules : asArray(root.compiled);
  return rules.filter(isRecord).map((rule, index) => {
    const actions = asArray(rule.then).filter(isRecord);
    const emits = actions.flatMap(action => {
      const emitEvent = asRecord(action.emit_event);
      const emit = asRecord(action.emit);
      const candidate = emitEvent.type ?? emit.type;
      return typeof candidate === "string" && candidate.trim() ? [candidate.trim()] : [];
    });
    const id = typeof rule.id === "string" || typeof rule.id === "number" ? String(rule.id) : `rule-${index + 1}`;
    return { id, emits: [...new Set(emits)] };
  });
}
