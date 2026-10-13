import assert from "node:assert/strict";
import test from "node:test";
import { branchesFromRevision, layoutRuleFlow } from "./flow-layout.ts";

test("layout marks disconnected rule sets unreachable, not service-triggered", () => {
  const graph = new Map([
    ["entry", [{ id: "a", emits: ["child", "external.event"] }]],
    ["child", [{ id: "b", emits: ["entry"] }]],
    ["orphan", [{ id: "c", emits: [] }]],
  ]);
  const nodes = layoutRuleFlow(graph, "entry");
  assert.deepEqual(nodes.map(node => [node.ruleSet, node.reachability]), [
    ["entry", "entry"],
    ["child", "rule-set"],
    ["orphan", "unreachable"],
  ]);
  assert.equal(layoutRuleFlow(graph, "missing").length, 0);
});

test("revision parser accepts both supported emit shapes and removes duplicates", () => {
  const branches = branchesFromRevision({
    source: { rules: [{ id: "r1", then: [
      { emit_event: { type: "child.event" } },
      { emit: { type: "child.event" } },
      { emit_event: { type: " " } },
      null,
    ] }, { then: [{ emit: { type: "next.event" } }] }] },
    compiled: [{ id: "ignored", then: [] }],
  });
  assert.deepEqual(branches, [
    { id: "r1", emits: ["child.event"] },
    { id: "rule-2", emits: ["next.event"] },
  ]);
});

test("revision parser safely handles malformed and compiled-only payloads", () => {
  assert.deepEqual(branchesFromRevision(null), []);
  assert.deepEqual(branchesFromRevision({ source: { rules: "invalid" }, compiled: [null, { id: 7, then: "invalid" }] }), [
    { id: "7", emits: [] },
  ]);
});
