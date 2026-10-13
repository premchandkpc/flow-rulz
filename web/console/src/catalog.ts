// Example domain definitions bundled at build time. They are only seed sources for "Save to database":
// the console writes them to the FlowRulz database through the management API,
// and the catalog page reads the stored copies back from the API.
export { branchesFromRevision, layoutRuleFlow } from "./flow-layout";
export type { FlowNode, FlowReachability, FlowRulzBranch } from "./flow-layout";

export type CatalogKind = "contract" | "rule";
export interface CatalogItem { domain: string; kind: CatalogKind; name: string; source: string; }

const files = import.meta.glob("../../../domains/*/{contracts,rules}/*.yaml", {
  query: "?raw", import: "default", eager: true,
}) as Record<string, string>;

export const DOMAIN_LABELS: Record<string, string> = {
  ecommerce: "E-commerce", banking: "Banking", "food-delivery": "Food delivery",
};

export const CATALOG: CatalogItem[] = Object.entries(files).flatMap(([path, source]) => {
  const match = path.match(/domains\/([^/]+)\/(contracts|rules)\/(.+)\.yaml$/);
  if (!match) return [];
  return [{ domain: match[1], kind: match[2] === "contracts" ? "contract" : "rule", name: match[3], source } as CatalogItem];
}).sort((a, b) => a.domain.localeCompare(b.domain) || a.kind.localeCompare(b.kind) || a.name.localeCompare(b.name));

