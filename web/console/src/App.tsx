import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import "./styles.css";
import { CATALOG, DOMAIN_LABELS, branchesFromRevision, layoutRuleFlow, type FlowRulzBranch } from "./catalog";

type Settings = { apiBase: string; tenant: string; token: string; workerMetrics: string; lokiBase: string; lokiQuery: string };
type WorkerSnapshot = { url: string; name: string; online: boolean; checkedAt: number; inFlight: number | null; activeKeys: number | null; shards: number | null; executions: Array<{ ruleSet: string; status: string; count: number }> ; error?: string };
type NavKey = "overview" | "simulations" | "catalog" | "api" | "codegen" | "workers" | "rules" | "logs";
type SimulationScenario = "all" | "ecommerce" | "banking" | "food-delivery";
type TenantDomain = { id: string; name: string; event_types: string[] };
type DomainService = { id: string; name: string; nats_subject: string; domain_ids: string[]; active: boolean };
const SOURCE_RULE_BY_SCENARIO: Record<Exclude<SimulationScenario, "all">, string> = {
  ecommerce: "order.created",
  banking: "transaction.initiated",
  "food-delivery": "order.placed",
};
const EXAMPLE_SOURCE_RULES = [
  { scenario: "ecommerce", label: "E-commerce" },
  { scenario: "banking", label: "Banking" },
  { scenario: "food-delivery", label: "Food delivery" },
] as const;
type TraceNode = {
  event_id: string;
  type: string;
  partition_key: string;
  shard: number;
  status: string;
  execution?: { rule_set: string; revision: number; status: string; created_at?: string; completed_at?: string };
  effects: Array<{
    effect_id: string;
    effect_type: string;
    name: string;
    destination: string;
    status: string;
    child?: TraceNode;
  }>;
  truncated?: boolean;
};

type SimulationLogEntry = {
  timestamp: string;
  level: string;
  service: string;
  message: string;
  labels: Record<string, string>;
  eventId?: string;
  scenario?: string;
};
type SimulationEvent = {
  id: string;
  type: string;
  scenario: Exclude<SimulationScenario, "all">;
  partitionKey: string;
  shard: number;
  summary: string;
  sentAt: string;
  trace?: TraceNode;
};

const sumGauge = (workers: WorkerSnapshot[], pick: (w: WorkerSnapshot) => number | null) => {
  const values = workers.map(pick).filter((v): v is number => v !== null);
  return values.length === 0 ? "—" : values.reduce((n, v) => n + v, 0);
};
const coverageHint = (workers: WorkerSnapshot[], pick: (w: WorkerSnapshot) => number | null, base: string) => {
  const missing = workers.filter(w => pick(w) === null).length;
  return missing === 0 ? base : `Incomplete: ${missing} of ${workers.length} workers did not report`;
};
const DEFAULT_SETTINGS: Settings = { apiBase: "/api", tenant: "development-tenant", token: "", workerMetrics: "/worker-metrics/metrics", lokiBase: "/", lokiQuery: '{service_name=~"api|worker|console|postgres|nats|redis|loki|promtail"}' };
const NAV: Array<{ id: NavKey; label: string; icon: string }> = [
  { id: "overview", label: "Overview", icon: "◫" }, { id: "simulations", label: "Simulations", icon: "⇢" },
  { id: "catalog", label: "Domain catalog", icon: "▤" },
  { id: "api", label: "API explorer", icon: "⌘" },
  { id: "codegen", label: "Code generation", icon: "⟐" }, { id: "workers", label: "Workers & rules", icon: "▦" },
  { id: "rules", label: "Rule management", icon: "◎" }, { id: "logs", label: "Logs", icon: "≋" },
];
const ROUTES = [
  { method: "GET", path: "/health", description: "API liveness" },
  { method: "GET", path: "/ready", description: "Dependency readiness (database, NATS)" },
  { method: "GET", path: "/v1/contracts", description: "List registered contracts" },
  { method: "POST", path: "/v1/contracts", description: "Register a contract" },
  { method: "GET", path: "/v1/rules/active", description: "List active rules for the tenant" },
  { method: "GET", path: "/v1/rules/{ruleSet}/revisions", description: "List revisions for a rule set" },
  { method: "POST", path: "/v1/rules/{ruleSet}/revisions", description: "Compile and activate a rule revision" },
  { method: "GET", path: "/v1/workflows", description: "List workflows" },
  { method: "GET", path: "/v1/workflows/definitions", description: "List workflow definitions" },
  { method: "GET", path: "/v1/events/{eventID}/trace", description: "Trace event executions and emitted events" },
  { method: "GET", path: "/v1/domains", description: "List tenant event domains" },
  { method: "POST", path: "/v1/domains", description: "Create a tenant event domain" },
  { method: "GET", path: "/v1/services", description: "List tenant event-consuming services" },
  { method: "POST", path: "/v1/services", description: "Create a service and domain subscriptions" },
  { method: "GET", path: "/v1/batches", description: "List batches" },
  { method: "GET", path: "/v1/quarantine", description: "List quarantined work" },
] as const;

function readSettings(): Settings {
  try { return { ...DEFAULT_SETTINGS, ...JSON.parse(localStorage.getItem("flowrulz-console-settings") || "{}") }; }
  catch { return DEFAULT_SETTINGS; }
}
function normalizeBase(value: string) { return value.trim().replace(/\/+$/, ""); }
function observeTrace(node: TraceNode | undefined, seen: FlowObservation) {
  if (!node) return;
  if (node.execution) {
    const emitted = seen.ran.get(node.execution.rule_set) ?? new Set<string>();
    for (const effect of node.effects) {
      for (const label of [effect.child?.type, effect.name, effect.effect_type]) if (label) emitted.add(label);
    }
    seen.ran.set(node.execution.rule_set, emitted);
  }
  seen.types.add(node.type);
  node.effects.forEach(effect => observeTrace(effect.child, seen));
}


const RULE_TEMPLATE = {
  rule_set: "transaction.initiated", revision: 1, mode: "all_matches",
  rules: [{
    id: "high_value", name: "High value", priority: 100,
    when: { path: "$.amount", op: "gte", value: 1000000 },
    then: [{ emit_event: { type: "compliance.check", partition_key_policy: "inherit", data: { transaction_id: "{{$.transaction_id}}", amount: "{{$.amount}}", requested_at: "{{now}}" } } }],
  }],
};
function sampleForContract(contract: any): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  const build = (fields: Record<string, any>) => {
    const obj: Record<string, unknown> = {};
    for (const [name, field] of Object.entries(fields ?? {})) {
      if (field.type === "number" || field.type === "integer") obj[name] = 1000;
      else if (field.type === "boolean") obj[name] = true;
      else if (field.type === "array") obj[name] = [];
      else if (field.type === "object") obj[name] = field.fields ? build(field.fields) : {};
      else if (field.format === "date-time") obj[name] = new Date().toISOString();
      else obj[name] = `${name}-1`;
    }
    return obj;
  };
  return Object.assign(out, build(contract?.fields ?? {}));
}
type RuleCheck = { valid: boolean; error?: string; rule_set?: string; revision?: number; match_mode?: string; rule_count: number; evaluation_error?: string;
  decision?: { matched_rules: string[]; effects: Array<{ name: string; destination: string; effect_type: string; payload: any }>; explanations?: Array<{ rule_id: string; rule_name?: string; matched: boolean; priority: number; reason: string; actions?: string[] }> } };

function RuleStudio({ settings, activeRules, contracts: allContracts, onChanged }: { settings: Settings; activeRules: any[]; contracts: any[]; onChanged: () => Promise<void> }) {
  const [domains, setDomains] = useState<any[]>([]);
  const [domainName, setDomainName] = useState("");
  const [ruleSet, setRuleSet] = useState("");
  const [source, setSource] = useState("");
  const [sample, setSample] = useState('{\n  "transaction_id": "txn-1",\n  "amount": 2500000\n}');
  const [check, setCheck] = useState<RuleCheck | null>(null);
  const [checking, setChecking] = useState(false);
  const [checkedKey, setCheckedKey] = useState("");
  const checkSeq = useRef(0);
  const [message, setMessage] = useState<{ tone: "ok" | "error"; text: string } | null>(null);
  const [revisions, setRevisions] = useState<any[]>([]);
  const [busy, setBusy] = useState(false);

  const domain = domains.find(d => d.name === domainName);
  const eventTypes: string[] = useMemo(() => domain?.event_types ?? [], [domain]);
  const contracts = useMemo(() => allContracts.filter(c => eventTypes.includes(c.name)), [allContracts, eventTypes]);
  const activeIds = new Set(activeRules.map(r => r.rule_set || r.rule_id));
  const domainRules = activeRules.filter(r => eventTypes.includes(r.rule_set || r.rule_id));
  const unruled = eventTypes.filter(t => !activeIds.has(t));

  useEffect(() => {
    apiRequest(settings, "/v1/domains").then(r => { const list = asArray(r); setDomains(list); setDomainName(prev => prev || list[0]?.name || ""); }).catch(() => setDomains([]));
  }, [settings]);

  const sourceDoc = useMemo(() => { try { return JSON.parse(source); } catch { return null; } }, [source]);
  const sampleError = useMemo(() => { try { JSON.parse(sample); return ""; } catch (e) { return e instanceof Error ? e.message : "Invalid JSON"; } }, [sample]);

  const checkKey = JSON.stringify({ source, sample, ruleSet: ruleSet.trim(), tenant: settings.tenant.trim() });

  const runCheck = useCallback(async () => {
    const seq = ++checkSeq.current;
    const key = checkKey;
    setChecking(true); setMessage(null);
    try {
      const body: Record<string, unknown> = { source, event_type: ruleSet.trim() || sourceDoc?.rule_set };
      if (!sampleError && sample.trim()) body.event = JSON.parse(sample);
      const result = unwrap(await apiRequest(settings, "/v1/rules/check", { method: "POST", body: JSON.stringify(body) })) as RuleCheck;
      if (seq !== checkSeq.current) return;
      setCheck(result); setCheckedKey(key);
    } catch (e) { if (seq === checkSeq.current) { setCheck(null); setMessage({ tone: "error", text: e instanceof Error ? e.message : String(e) }); } }
    finally { if (seq === checkSeq.current) setChecking(false); }
  }, [settings, source, sample, sampleError, ruleSet, sourceDoc, checkKey]);

  useEffect(() => {
    if (!source.trim()) return;
    const timer = setTimeout(() => void runCheck(), 700);
    return () => clearTimeout(timer);
  }, [source, sample, ruleSet]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadRevisions = useCallback(async (name: string) => {
    try { setRevisions(asArray(await apiRequest(settings, `/v1/rules/${encodeURIComponent(name)}/revisions`)).sort((a, b) => b.revision - a.revision)); } catch { setRevisions([]); }
  }, [settings]);

  const openRevision = async (name: string, revision: number, asNext: boolean) => {
    setMessage(null);
    try {
      const rev = unwrap(await apiRequest(settings, `/v1/rules/${encodeURIComponent(name)}/revisions/${revision}`));
      const doc = { ...(rev.source ?? {}) };
      if (asNext) {
        const latest = Math.max(revision, ...revisions.map(r => r.revision), 0);
        doc.revision = latest + 1;
      }
      setRuleSet(name); setSource(JSON.stringify(doc, null, 2));
      const contract = contracts.find(c => c.name === name);
      if (contract) setSample(JSON.stringify(sampleForContract(contract), null, 2));
      await loadRevisions(name);
      setMessage({ tone: "ok", text: asNext ? `Loaded ${name} revision ${revision} as the base for revision ${doc.revision}.` : `Loaded ${name} revision ${revision}.` });
    } catch (e) { setMessage({ tone: "error", text: e instanceof Error ? e.message : String(e) }); }
  };

  useEffect(() => {
    setCheck(null); setRevisions([]); setMessage(null);
    const first = domainRules[0];
    if (first) void openRevision(first.rule_set || first.rule_id, first.revision, false);
    else { setRuleSet(""); setSource(""); }
  }, [domainName]); // eslint-disable-line react-hooks/exhaustive-deps

  const startNew = (eventType: string) => {
    const contract = contracts.find(c => c.name === eventType);
    setRevisions([]); setCheck(null); setRuleSet(eventType);
    setSource(JSON.stringify({ rule_set: eventType, revision: 1, mode: "all_matches", rules: [{ id: "first_rule", name: "First rule", priority: 100, when: { path: `$.${Object.keys(contract?.fields ?? { id: 1 })[0]}`, op: "exists" }, then: [{ emit_event: { type: eventTypes.find(t => t !== eventType) ?? eventType, partition_key_policy: "inherit", data: {} } }] }] }, null, 2));
    setSample(JSON.stringify(sampleForContract(contract), null, 2));
  };

  const format = () => { if (sourceDoc) setSource(JSON.stringify(sourceDoc, null, 2)); };
  const fillSample = () => {
    const name = ruleSet.trim() || sourceDoc?.rule_set;
    const contract = contracts.find(c => c.name === name);
    if (!contract) { setMessage({ tone: "error", text: `No contract named "${name}" is stored. Save the domain from the Domain catalog first.` }); return; }
    setSample(JSON.stringify(sampleForContract(contract), null, 2));
  };
  const deploy = async () => {
    const name = ruleSet.trim() || sourceDoc?.rule_set;
    setBusy(true); setMessage(null);
    try {
      await apiRequest(settings, `/v1/rules/${encodeURIComponent(name)}/revisions`, { method: "POST", headers: { "Content-Type": "application/yaml" }, body: source });
      setMessage({ tone: "ok", text: `${name} revision ${sourceDoc?.revision} is compiled and active.` });
      await onChanged(); await loadRevisions(name);
    } catch (e) { setMessage({ tone: "error", text: e instanceof Error ? e.message : String(e) }); }
    finally { setBusy(false); }
  };
  const deactivate = async (name: string, revision: number) => {
    if (!window.confirm(`Deactivate ${name} revision ${revision}?`)) return;
    try { await apiRequest(settings, `/v1/rules/${encodeURIComponent(name)}/revisions/${revision}`, { method: "DELETE" }); await onChanged(); setMessage({ tone: "ok", text: `${name} deactivated.` }); }
    catch (e) { setMessage({ tone: "error", text: e instanceof Error ? e.message : String(e) }); }
  };

  const stale = check !== null && checkedKey !== checkKey;
  const jsonProblem = source.trim() && !sourceDoc ? "The editor holds JSON. Fix the syntax before checking." : "";
  const outOfDomain = !!sourceDoc?.rule_set && !eventTypes.includes(sourceDoc.rule_set);
  const mismatch = !!ruleSet && !!sourceDoc?.rule_set && sourceDoc.rule_set !== ruleSet;
  const canDeploy = !!check?.valid && !stale && !busy && !jsonProblem && !outOfDomain && !mismatch && !!ruleSet;
  const pathWarnings = useMemo(() => {
    const warnings: string[] = [];
    const name = ruleSet.trim() || sourceDoc?.rule_set;
    const contract = contracts.find(c => c.name === name);
    if (!sourceDoc?.rules) return warnings;
    const known = new Set(Object.keys(contract?.fields ?? {}));
    const visit = (p: any) => { if (!p || !contract) return; if (p.path) { const top = String(p.path).replace(/^\$\./, "").split(".")[0]; if (!known.has(top)) warnings.push(`${p.path} is not a field of contract ${contract.name} ${contract.version}`); } (p.all ?? p.any ?? []).forEach(visit); if (p.not) visit(p.not); };
    sourceDoc.rules.forEach((r: any) => {
      visit(r.when);
      (r.then ?? []).forEach((act: any) => { const t = act.emit_event?.type ?? act.emit?.type; if (t && !eventTypes.includes(t)) warnings.push(`${r.id} emits ${t}, which is not an event type of the ${domainName} domain`); });
    });
    return [...new Set(warnings)];
  }, [sourceDoc, ruleSet, contracts, eventTypes, domainName]);

  return <section className="page-content rule-studio">
    <div className="page-intro"><h2>Rule studio</h2><p>Rules belong to a domain. Pick a domain, open one of its rule sets, check it against sample event data, then activate it as a new revision. Nothing is saved until you activate.</p></div>
    <div className="studio-domains" role="tablist" aria-label="Domain">
      {domains.map(d => { const n = activeRules.filter(r => (d.event_types ?? []).includes(r.rule_set || r.rule_id)).length; return <button key={d.id} role="tab" aria-selected={d.name === domainName} className={d.name === domainName ? "active" : ""} onClick={() => setDomainName(d.name)}><strong>{d.name}</strong><small>{n} rule sets · {(d.event_types ?? []).length} event types</small></button>; })}
      {domains.length === 0 && <p className="studio-muted">No domains yet. Save an example domain from the Domain catalog.</p>}
    </div>
    {message && <div className={`notice ${message.tone === "error" ? "notice-error" : ""}`} role="status">{message.text}</div>}
    <div className="studio-grid">
      <aside className="panel studio-list">
        <div className="panel-heading"><div><h3>{domainName || "Domain"} rule sets</h3><p>{domainRules.length} active</p></div></div>
        {domainRules.length === 0 ? <Empty title="No active rule sets" detail="Add one for an event type below."/> :
          <ul className="studio-rules">{domainRules.map(item => { const id = item.rule_set || item.rule_id; return <li key={id} className={ruleSet === id ? "selected" : ""}>
            <button className="studio-rule-open" onClick={() => void openRevision(id, item.revision, false)}><strong>{id}</strong><small>revision {item.revision}</small></button>
            <button className="text-button danger-text" onClick={() => void deactivate(id, item.revision)}>Deactivate</button>
          </li>; })}</ul>}
        {unruled.length > 0 && <><h4 className="studio-sub">Event types without rules</h4><ul className="studio-rules">{unruled.map(t => <li key={t}><span className="studio-event">{t}</span><button className="text-button" onClick={() => startNew(t)}>+ Add rules</button></li>)}</ul></>}
        {revisions.length > 0 && <><h4 className="studio-sub">Revisions of {ruleSet}</h4><ul className="studio-rules">{revisions.map(r => <li key={r.revision}><button className="studio-rule-open" onClick={() => void openRevision(ruleSet, r.revision, false)}><strong>Revision {r.revision}</strong><small>{r.match_mode} · {(r.compiled ?? []).length} rules</small></button><button className="text-button" onClick={() => void openRevision(ruleSet, r.revision, true)}>Edit as new</button></li>)}</ul></>}
      </aside>
      <section className="panel studio-editor">
        <div className="panel-heading"><div><h3>{ruleSet || sourceDoc?.rule_set || "New rule set"}</h3><p>Revision {sourceDoc?.revision ?? "—"} · {sourceDoc?.mode ?? "first_match"}</p></div>
          <div className="studio-actions"><button className="text-button" onClick={format} disabled={!sourceDoc}>Format</button><button className="button" onClick={() => void runCheck()} disabled={checking}>{checking ? "Checking…" : "Check"}</button><button className="button primary" onClick={() => void deploy()} disabled={!canDeploy} title={canDeploy ? "" : "Check the rule successfully first"}>{busy ? "Activating…" : "Activate revision"}</button></div></div>
        <label>Event type ({domainName})<select value={ruleSet} onChange={e => e.target.value ? startNew(e.target.value) : setRuleSet("")}><option value="">Select an event type…</option>{eventTypes.map(t => <option key={t} value={t}>{t}</option>)}</select></label>
        {outOfDomain && <div className="notice notice-error">{sourceDoc?.rule_set} is not an event type of the {domainName} domain.</div>}
        {mismatch && !outOfDomain && <div className="notice notice-error">The source says rule_set {sourceDoc?.rule_set}, but {ruleSet} is selected.</div>}
        <label>Rule source (JSON)<textarea className="code-editor rule-editor" value={source} onChange={e => setSource(e.target.value)} spellCheck={false}/></label>
        {jsonProblem && <div className="notice notice-error">{jsonProblem}</div>}
        <div className="panel-heading studio-sample-head"><div><h3>Sample event data</h3><p>Evaluated against the rule on every check</p></div><button className="text-button" onClick={fillSample}>Fill from contract</button></div>
        <textarea className="code-editor studio-sample" value={sample} onChange={e => setSample(e.target.value)} spellCheck={false}/>
        {sampleError && <div className="notice notice-error">Sample is not valid JSON: {sampleError}</div>}
      </section>
      <section className="panel studio-result">
        <div className="panel-heading"><div><h3>Check result</h3><p>{checking ? "Checking…" : stale ? "Out of date — edit pending" : check ? "Up to date" : "Not checked yet"}</p></div>{check && <Status value={check.valid ? "Valid" : "Invalid"}/>}</div>
        {!check && <Empty title="Nothing checked" detail="Edit the rule or sample; the check runs automatically."/>}
        {check && !check.valid && <div className="notice notice-error studio-error">{check.error}</div>}
        {check?.valid && <>
          <dl className="studio-facts"><div><dt>Rules</dt><dd>{check.rule_count}</dd></div><div><dt>Mode</dt><dd>{check.match_mode}</dd></div><div><dt>Revision</dt><dd>{check.revision}</dd></div></dl>
          {pathWarnings.length > 0 && <div className="notice notice-warn"><strong>Contract warnings</strong><ul>{pathWarnings.map(w => <li key={w}>{w}</li>)}</ul></div>}
          {check.evaluation_error && <div className="notice notice-error">Evaluation failed: {check.evaluation_error}</div>}
          {check.decision && <>
            <h4 className="studio-sub">Rule by rule</h4>
            <ul className="studio-explain">{(check.decision.explanations ?? []).map(x => <li key={x.rule_id} className={x.matched ? "matched" : "skipped"}><span className="studio-mark">{x.matched ? "✓" : "–"}</span><div><strong>{x.rule_name || x.rule_id}</strong><small>priority {x.priority} · {x.reason}</small>{x.actions?.map(act => <code key={act}>{act}</code>)}</div></li>)}</ul>
            <h4 className="studio-sub">Effects ({check.decision.effects.length})</h4>
            {check.decision.effects.length === 0 ? <p className="studio-muted">No rule matched this sample, so nothing would be emitted.</p> : check.decision.effects.map((fx, i) => <div className="studio-effect" key={i}><header><strong>{fx.name}</strong><small>{fx.effect_type} → {fx.destination}</small></header><pre className="response-box">{JSON.stringify(fx.payload, null, 2)}</pre></div>)}
          </>}
        </>}
      </section>
    </div>
  </section>;
}

function RuleFlowMap({ settings, scenario, startRuleSet, events, running }: { settings: Settings; scenario: string; startRuleSet: string; events: SimulationEvent[]; running: boolean }) {
  const [stored, setStored] = useState<Map<string, FlowRulzBranch[]>>(new Map());
  const [serviceEventTypes, setServiceEventTypes] = useState<Set<string>>(new Set());
  const [loadError, setLoadError] = useState("");
  useEffect(() => {
    let cancelled = false;
    // Clear tenant-scoped graph data immediately so a settings change cannot briefly show
    // the previous tenant's graph or service subscriptions while the new request is pending.
    setStored(new Map());
    setServiceEventTypes(new Set());
    setLoadError("");
    (async () => {
      try {
        const [domainsResponse, activeResponse, servicesResponse] = await Promise.all([
          apiRequest(settings, "/v1/domains"),
          apiRequest(settings, "/v1/rules/active"),
          apiRequest(settings, "/v1/services"),
        ]);
        const domainRows = asArray(domainsResponse);
        const serviceRows = asArray(servicesResponse) as DomainService[];
        const subscribedTypes = new Set<string>();
        for (const domain of domainRows) {
          if (!Array.isArray(domain.event_types)) continue;
          const subscribed = serviceRows.some(service =>
            service.active && Array.isArray(service.domain_ids) && service.domain_ids.includes(String(domain.id))
          );
          if (subscribed) domain.event_types.forEach((type: unknown) => {
            if (typeof type === "string" && type.trim()) subscribedTypes.add(type.trim());
          });
        }
        const domain = domainRows.find(d => d.name === scenario);
        const types = new Set<string>(Array.isArray(domain?.event_types) ? domain.event_types : []);
        const names = asArray(activeResponse)
          .map(r => ({ name: String(r.rule_set ?? ""), revision: Number(r.revision) }))
          .filter(r => r.name && Number.isFinite(r.revision) && r.revision > 0 && types.has(r.name));
        const entries = await Promise.all(names.map(async ({ name, revision }) => [
          name,
          branchesFromRevision(unwrap(await apiRequest(settings, `/v1/rules/${encodeURIComponent(name)}/revisions/${revision}`))),
        ] as const));
        if (!cancelled) {
          setStored(new Map(entries));
          setServiceEventTypes(subscribedTypes);
          setLoadError("");
        }
      } catch (e) {
        if (!cancelled) setLoadError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { cancelled = true; };
  }, [settings, scenario]);
  const nodes = useMemo(() => layoutRuleFlow(stored, startRuleSet), [stored, startRuleSet]);
  const seen = useMemo(() => {
    const result: FlowObservation = { ran: new Map(), types: new Set() };
    events.filter(event => event.scenario === scenario).forEach(event => observeTrace(event.trace, result));
    return result;
  }, [events, scenario]);
  const levels = [...new Set(nodes.map(node => node.level))];
  return <section className="panel rule-flow-panel">
    <div className="panel-heading"><div><h3>{DOMAIN_LABELS[scenario] || scenario} rule-set flow</h3><p>Each column is one hop. A rule set emits an event; when that event type has its own rule set, processing continues there, otherwise it leaves for a subscribed service.</p></div></div>
    {loadError && <div className="notice" role="alert">{loadError}</div>}
    {!loadError && nodes.length === 0 && <Empty title="No rule sets in the database" detail={`Save the ${DOMAIN_LABELS[scenario] || scenario} domain from the Domain catalog page to see its flow.`} />}
    <div className="rule-flow-map">{levels.map(level => <div className="rule-flow-column" key={level}>
      <div className="rule-flow-level">{level === 0 ? "Entry" : nodes.some(n => n.level === level && n.reachability === "unreachable") ? "Unreachable from entry" : `Hop ${level}`}</div>
      {nodes.filter(node => node.level === level).map(node => {
        const emitted = seen.ran.get(node.ruleSet);
        return <div className={`rule-flow-node ${emitted ? "flow-complete" : "flow-waiting"}${emitted && running ? " flow-live" : ""}`} key={node.ruleSet}>
          <header><span className="rule-symbol">R</span><strong>{node.ruleSet}</strong><small>{emitted ? "ran" : "not seen yet"}</small></header>
          {node.branches.map(branch => <div className="rule-flow-branch" key={branch.id}>
            <span className="rule-flow-branch-id">{branch.id}</span>
            {branch.emits.map(type => {
              const next = stored.has(type);
              const subscribed = serviceEventTypes.has(type);
              const taken = emitted?.has(type) ?? false;
              const destination = next && subscribed ? "rule set + subscribed service" : next ? "next rule set" : subscribed ? "subscribed service" : "terminal / unhandled";
              return <span className={`rule-flow-emit ${next ? "to-rule" : subscribed ? "to-service" : "to-terminal"}${taken ? " taken" : ""}`} key={type} title={destination}>→ {type}<em>{destination}</em></span>;
            })}
          </div>)}
        </div>;
      })}
    </div>)}</div>
  </section>;
}

function countTraceNodes(node?: TraceNode): number {
  if (!node) return 0;
  return 1 + node.effects.reduce((count, effect) => count + countTraceNodes(effect.child), 0);
}
function countDeliveredEffects(node?: TraceNode): number {
  if (!node) return 0;
  return node.effects.reduce((count, effect) => count + (effect.status === "delivered" ? 1 : 0) + countDeliveredEffects(effect.child), 0);
}
function unwrap(value: any): any { return value && typeof value === "object" && "data" in value ? value.data : value; }
function asArray(value: any): any[] { const v = unwrap(value); if (Array.isArray(v)) return v; if (!v || typeof v !== "object") return []; for (const key of ["active_rules", "revisions", "contracts", "schemas", "workflows", "events", "batches", "domains", "services", "items", "data"]) if (Array.isArray(v[key])) return v[key]; return []; }
function fmt(value: unknown) { return typeof value === "string" ? value : JSON.stringify(value, null, 2) ?? String(value); }
function metricValue(text: string, name: string): number | null { const line = text.split("\n").find((row) => row.startsWith(name + " ") || row.startsWith(name + "{")); if (!line) return null; const n = Number(line.trim().split(/\s+/).pop()); return Number.isFinite(n) ? n : null; }
function labeledMetrics(text: string, metric: string) {
  return text.split("\n").flatMap((line) => {
    if (!line.startsWith(metric + "{")) return [];
    const match = line.match(/^([^\s]+)\s+([\d.eE+-]+)$/); if (!match) return [];
    const labels: Record<string, string> = {};
    const open = line.indexOf("{");
    const close = line.indexOf("}", open + 1);
    const labelText = open >= 0 && close > open ? line.slice(open + 1, close) : "";
    let cursor = 0;
    while (cursor < labelText.length) {
      while (cursor < labelText.length && (labelText[cursor] === "," || /\s/.test(labelText[cursor]))) cursor++;
      const keyStart = cursor;
      while (cursor < labelText.length && /[A-Za-z0-9_]/.test(labelText[cursor])) cursor++;
      const key = labelText.slice(keyStart, cursor);
      if (!key || labelText[cursor] !== "=") {
        cursor++;
        continue;
      }
      cursor++;
      if (labelText[cursor] !== '"') {
        cursor++;
        continue;
      }
      cursor++;
      let value = "";
      while (cursor < labelText.length) {
        const char = labelText[cursor++];
        if (char === '"') break;
        if (char === "\\" && cursor < labelText.length) {
          const escaped = labelText[cursor++];
          value += escaped === "n" ? "\n" : escaped;
        } else {
          value += char;
        }
      }
      labels[key] = value;
      while (cursor < labelText.length && labelText[cursor] !== ",") cursor++;
      if (labelText[cursor] === ",") cursor++;
    }
    return [{ labels, count: Number(match[2]) }];
  });
}
async function readResponse(response: Response) {
  const text = await response.text(); let payload: any = text;
  try { payload = text ? JSON.parse(text) : null; } catch { /* text response */ }
  if (!response.ok) throw new Error(payload?.error?.message || payload?.message || payload?.error || `${response.status} ${response.statusText}: ${String(text).slice(0, 240)}`);
  return payload;
}
async function apiRequest(settings: Settings, path: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers);
  headers.set("X-Tenant-ID", settings.tenant.trim());
  if (settings.token.trim()) headers.set("Authorization", `Bearer ${settings.token.trim()}`);
  if (init.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  return readResponse(await fetch(normalizeBase(settings.apiBase) + path, { ...init, headers }));
}
const TERMINAL_EFFECT = new Set(["delivered", "quarantined"]);
const isTerminalTrace = (node?: TraceNode): boolean => !!node?.execution && node.execution.status === "completed" && node.effects.every(e => TERMINAL_EFFECT.has(e.status.toLowerCase()) && (!e.child || isTerminalTrace(e.child)));
function Status({ value }: { value: string }) { const key = value.toLowerCase().replace(/[^a-z0-9]+/g, "-"); return <span className={`status status-${key}`}><i />{value}</span>; }
function MetricCard({ label, value, hint, icon }: { label: string; value: string | number; hint: string; icon: string }) { return <article className="metric-card"><div className="metric-top"><span>{label}</span><span className="metric-icon">{icon}</span></div><strong>{value}</strong><small>{hint}</small></article>; }
function Empty({ title, detail }: { title: string; detail: string }) { return <div className="empty-state"><div className="empty-icon">⌁</div><strong>{title}</strong><p>{detail}</p></div>; }
function TraceBranch({ node, depth = 0 }: { node: TraceNode; depth?: number }) {
  return <div className={`trace-branch${depth > 0 ? " trace-branch-child" : ""}`}>
    <div className="trace-event">
      <div className="trace-event-top"><strong>{node.type || "Event waiting for worker"}</strong><Status value={node.status}/></div>
      <div className="trace-event-meta"><span>id {node.event_id.slice(0, 12)}</span>{node.partition_key && <span>key {node.partition_key}</span>}{node.type && <span>events.shard.{node.shard}</span>}</div>
      {node.execution && <small className="trace-execution">Rule set {node.execution.rule_set} · revision {node.execution.revision}</small>}
    </div>
    {node.effects.map(effect => <div className="trace-effect" key={effect.effect_id}>
      <div className="trace-effect-line"><span className={`effect-mark effect-${effect.effect_type.replace(/_/g, "-")}`}>{effect.effect_type === "emit_event" ? "↗" : "⇢"}</span><div><strong>{effect.name}</strong><small>{effect.destination} · {effect.status}</small></div><Status value={effect.status}/></div>
      {effect.child && depth < 5 && <TraceBranch node={effect.child} depth={depth + 1}/>}
    </div>)}
    {node.truncated && <div className="trace-truncated">Trace shortened at the display limit.</div>}
  </div>;
}

export default function App() {
  const [settings, setSettings] = useState<Settings>(readSettings);
  const [nav, setNav] = useState<NavKey>("overview");
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [dependencies, setDependencies] = useState<Record<string, { status: string }> | null>(null);
  const [health, setHealth] = useState<"checking" | "online" | "offline">("checking");
  const [activeRules, setActiveRules] = useState<any[]>([]);
  const [contracts, setContracts] = useState<any[]>([]);
  const [workers, setWorkers] = useState<WorkerSnapshot[]>([]);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [apiPath, setApiPath] = useState("/v1/rules/active");
  const [apiMethod, setApiMethod] = useState("GET");
  const [apiBody, setApiBody] = useState('{"rule_set":"transaction.initiated","revision":1}');
  const [apiResult, setApiResult] = useState("");
  const [apiBusy, setApiBusy] = useState(false);
  const [contractName, setContractName] = useState("");
  const [contractVersion, setContractVersion] = useState("v1");
  const [contractSchema, setContractSchema] = useState('{"name":"transaction.initiated","version":"v1","fields":[]}');
  const [selectedContract, setSelectedContract] = useState("");
  const [language, setLanguage] = useState("go");
  const [generated, setGenerated] = useState<any>(null);
  const [codegenBusy, setCodegenBusy] = useState(false);
  const [ruleSet, setRuleSet] = useState("");
  const [ruleSource, setRuleSource] = useState('rule_set: transaction.initiated\nrevision: 1\nrules: []\n');
  const [ruleResult, setRuleResult] = useState("");
  const [logs, setLogs] = useState<any[]>([]);
  const [logsBusy, setLogsBusy] = useState(false);
  const [logsError, setLogsError] = useState("");
  const [simulationLogs, setSimulationLogs] = useState<SimulationLogEntry[]>([]);
  const [simulationLogsBusy, setSimulationLogsBusy] = useState(false);
  const [simulationLogsError, setSimulationLogsError] = useState("");
  const [tenantDomains, setTenantDomains] = useState<TenantDomain[]>([]);
  const [storedContracts, setStoredContracts] = useState<any[]>([]);
  const [storedRules, setStoredRules] = useState<any[]>([]);
  const [catalogBusy, setCatalogBusy] = useState("");
  const [catalogMessage, setCatalogMessage] = useState("");
  const [catalogView, setCatalogView] = useState<{ title: string; body: string } | null>(null);
  const [simulationScenario, setSimulationScenario] = useState<SimulationScenario>("all");
  const [simulationPace, setSimulationPace] = useState(4000);
  const [simulationEvents, setSimulationEvents] = useState<SimulationEvent[]>([]);
  const [simulationRunning, setSimulationRunning] = useState(false);
  const [simulationStopping, setSimulationStopping] = useState(false);
  const [simulationError, setSimulationError] = useState("");
  const [simulationTraceError, setSimulationTraceError] = useState("");
  const [simulationNotice, setSimulationNotice] = useState("");
  const simulationStopRequestedRef = useRef(false);
  const simulationWaitRef = useRef<AbortController | null>(null);
  const simulationEventsRef = useRef<SimulationEvent[]>([]);
  const simulationTraceInFlightRef = useRef(new Set<string>());

  const saveSettings = (next: Settings) => { setSettings(next); localStorage.setItem("flowrulz-console-settings", JSON.stringify(next)); setSettingsOpen(false); setHealth("checking"); };
  const loadDomainServices = useCallback(async () => {
    try { setTenantDomains(asArray(await apiRequest(settings, "/v1/domains")) as TenantDomain[]); } catch { /* domains are optional context */ }
  }, [settings]);
  useEffect(() => {
  }, [nav, loadDomainServices]);
  const loadCatalog = useCallback(async () => {
    setCatalogBusy("load");
    try {
      const [contracts, activeRules] = await Promise.all([apiRequest(settings, "/v1/contracts"), apiRequest(settings, "/v1/rules/active")]);
      setStoredContracts(asArray(contracts));
      setStoredRules(asArray(activeRules));
      setCatalogMessage("");
    } catch (e) {
      setCatalogMessage(e instanceof Error ? e.message : String(e));
    } finally {
      setCatalogBusy("");
    }
  }, [settings]);
  useEffect(() => {
    if (nav === "catalog") void loadCatalog();
  }, [nav, loadCatalog]);
  const loadDomainIntoDatabase = async (domainKey: string) => {
    setCatalogBusy(domainKey);
    setCatalogMessage("");
    const items = CATALOG.filter(item => item.domain === domainKey);
    let completed = 0;
    let currentItem = "";
    try {
      if (items.length === 0) throw new Error(`No bundled contracts or rules found for domain "${domainKey}".`);
      for (const item of items) {
        currentItem = `${item.kind} "${item.name}"`;
        if (item.kind === "contract") {
          await apiRequest(settings, "/v1/contracts", {
            method: "POST", headers: { "Content-Type": "application/yaml" }, body: item.source,
          });
        } else {
          await apiRequest(settings, `/v1/rules/${encodeURIComponent(item.name)}/revisions`, {
            method: "POST", headers: { "Content-Type": "application/yaml" }, body: item.source,
          });
        }
        completed++;
      }
      setCatalogMessage(`${DOMAIN_LABELS[domainKey] ?? domainKey}: saved ${completed} contracts/rules to tenant ${settings.tenant}.`);
      await loadCatalog();
    } catch (e) {
      const reason = e instanceof Error ? e.message : String(e);
      const partial = completed > 0
        ? ` Import stopped after ${completed} of ${items.length} items; previous writes were not rolled back.`
        : "";
      setCatalogMessage(`Failed while saving ${currentItem || "domain catalog"}: ${reason}.${partial}`);
    } finally {
      setCatalogBusy("");
    }
  };
  const refresh = useCallback(async () => {
    setLoading(true); setError("");
    const checks = await Promise.allSettled([
      apiRequest(settings, "/health"),
      apiRequest(settings, "/v1/rules/active"),
      apiRequest(settings, "/v1/contracts"),
    ]);
    if (checks[0].status === "fulfilled") setHealth("online"); else setHealth("offline");
    const failures = checks.flatMap((check, index) => check.status === "rejected"
      ? [[ "API health", "Active rules", "Contracts" ][index] + ": " + (check.reason instanceof Error ? check.reason.message : String(check.reason))]
      : []);
    setError(failures.join(" · "));
    if (checks[1].status === "fulfilled") setActiveRules(asArray(checks[1].value)); else setActiveRules([]);
    if (checks[2].status === "fulfilled") setContracts(asArray(checks[2].value)); else setContracts([]);
    try {
      const body = await apiRequest(settings, "/ready");
      setDependencies(body?.data?.dependencies ?? null);
    } catch { setDependencies(null); }
    const endpoints = settings.workerMetrics.split(/[\n,]+/).map((s) => s.trim()).filter(Boolean);
    const workerData = await Promise.all(endpoints.map(async (url, index): Promise<WorkerSnapshot> => {
      try {
        const response = await fetch(url);
        const text = await response.text(); if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const execs = labeledMetrics(text, "flowrulz_executions_total");
        const grouped = new Map<string, number>();
        execs.forEach((row) => { const key = row.labels.rule_set || "unknown"; grouped.set(key, (grouped.get(key) || 0) + row.count); });
        const shardRows = labeledMetrics(text, "flowrulz_shard_ownership");
        return { url, name: new URL(url, window.location.origin).hostname + (endpoints.length > 1 ? ` · worker ${index + 1}` : ""), online: true, checkedAt: Date.now(), inFlight: metricValue(text, "flowrulz_global_in_flight"), activeKeys: metricValue(text, "flowrulz_active_keys"), shards: shardRows.filter((row) => row.count > 0).length, executions: [...grouped].map(([ruleSet, count]) => ({ ruleSet, status: "observed", count })).sort((a,b) => b.count-a.count) };
      } catch (e) { return { url, name: url, online: false, checkedAt: Date.now(), inFlight: null, activeKeys: null, shards: null, executions: [], error: e instanceof Error ? e.message : String(e) }; }
    }));
    setWorkers(workerData); setLastRefresh(new Date()); setLoading(false);
  }, [settings]);
  useEffect(() => { void refresh(); const id = window.setInterval(() => void refresh(), 10000); return () => window.clearInterval(id); }, [refresh]);

  const ruleCount = activeRules.length;
  const executionTotal = workers.reduce((sum, worker) => sum + worker.executions.reduce((n, item) => n + item.count, 0), 0);
  const onlineWorkers = workers.filter((worker) => worker.online).length;
  const selectedContractParts = selectedContract.split("@");
  const selectedContractName = selectedContractParts[0] || "";
  const selectedContractVersion = selectedContractParts[1] || "";
  const simulationProcessed = simulationEvents.filter(event => event.trace?.execution?.status === "completed").length;
  const simulationDelivered = simulationEvents.reduce((total, event) => total + countDeliveredEffects(event.trace), 0);
  const entryRuleFor = (scenario: Exclude<SimulationScenario, "all">) => SOURCE_RULE_BY_SCENARIO[scenario];
  const selectedSourceRules = EXAMPLE_SOURCE_RULES.filter(item => simulationScenario === "all" || item.scenario === simulationScenario).map(item => ({ ...item, rule: entryRuleFor(item.scenario) }));
  const latestSimulationEvent = simulationEvents[0];
  const liveFlowStage = !latestSimulationEvent ? -1
    : !latestSimulationEvent.trace?.execution ? 1
      : latestSimulationEvent.trace.effects.length === 0 ? 2
        : latestSimulationEvent.trace.effects.every(e => e.status.toLowerCase() === "delivered") ? 4 : 3;
  const latestEffectStatuses = (latestSimulationEvent?.trace?.effects ?? []).map(e => e.status.toLowerCase());
  const outboxFailed = latestEffectStatuses.some(s => s === "failed" || s === "quarantined");
  const flowStepState = (step: number) => step === 3 && outboxFailed ? "failed" : liveFlowStage < 0 ? "waiting"
    : step < liveFlowStage ? "complete"
      : step === liveFlowStage ? simulationRunning ? "live" : "current" : "waiting";
  const flowConnectorState = (connector: number) => connector === 2 && outboxFailed ? "failed" : liveFlowStage > connector ? "complete"
    : liveFlowStage === connector ? simulationRunning ? "live" : "current" : "waiting";

  const runApi = async () => {
    setApiBusy(true); setApiResult("");
    try {
      let requestPath = apiPath.startsWith("/") ? apiPath : `/${apiPath}`;
      const parameters = [...requestPath.matchAll(/\{([A-Za-z][A-Za-z0-9_]*)\}/g)];
      for (const [, name] of parameters) {
        const value = window.prompt(`Value for path parameter "${name}"`);
        if (value === null) return;
        if (!value.trim()) throw new Error(`Path parameter "${name}" cannot be empty.`);
        requestPath = requestPath.replace(`{${name}}`, encodeURIComponent(value.trim()));
      }
      const body = apiMethod === "GET" || apiMethod === "DELETE" ? undefined : JSON.stringify(JSON.parse(apiBody));
      const result = await apiRequest(settings, requestPath, { method: apiMethod, body });
      setApiResult(JSON.stringify(result, null, 2));
    } catch (e) {
      setApiResult(e instanceof Error ? e.message : String(e));
    } finally { setApiBusy(false); }
  };
  const createContract = async () => {
    setCodegenBusy(true); setGenerated(null);
    try {
      const payload = JSON.parse(contractSchema);
      payload.name = contractName.trim() || payload.name; payload.version = contractVersion.trim() || payload.version;
      const result = await apiRequest(settings, "/v1/contracts", { method: "POST", body: JSON.stringify(payload) });
      setGenerated({ operation: "Contract registered", result }); await refresh();
    } catch (e) { setGenerated({ error: e instanceof Error ? e.message : String(e) }); } finally { setCodegenBusy(false); }
  };
  const generate = async () => {
    if (!selectedContractName || !selectedContractVersion) return;
    setCodegenBusy(true); setGenerated(null);
    try { const result = await apiRequest(settings, `/v1/contracts/${encodeURIComponent(selectedContractName)}/${encodeURIComponent(selectedContractVersion)}/generate`, { method: "POST", body: JSON.stringify({ language }) }); setGenerated(result); }
    catch (e) { setGenerated({ error: e instanceof Error ? e.message : String(e) }); } finally { setCodegenBusy(false); }
  };
  const deployRule = async () => {
    setLoading(true); setRuleResult("");
    try { if (!ruleSet.trim()) throw new Error("Enter the rule set used by the source document."); const result = await apiRequest(settings, `/v1/rules/${encodeURIComponent(ruleSet.trim())}/revisions`, { method: "POST", headers: { "Content-Type": "application/yaml" }, body: ruleSource }); setRuleResult(JSON.stringify(result, null, 2)); await refresh(); }
    catch (e) { setRuleResult(e instanceof Error ? e.message : String(e)); } finally { setLoading(false); }
  };
  const deactivateRule = async (item: any) => {
    const id = item.rule_id || item.rule_set || item.ruleSet || item.id;
    const revision = item.revision;
    if (!id) return;
    if (!window.confirm(`Deactivate rule set "${id}"?`)) return;
    try { await apiRequest(settings, `/v1/rules/${encodeURIComponent(id)}/revisions/${encodeURIComponent(String(revision || 1))}`, { method: "DELETE" }); await refresh(); }
    catch (e) { setError(e instanceof Error ? e.message : String(e)); }
  };
  useEffect(() => { simulationEventsRef.current = simulationEvents; }, [simulationEvents]);
  useEffect(() => () => {
    simulationStopRequestedRef.current = true;
    simulationWaitRef.current?.abort();
  }, []);
  const [simulationTraceRefreshedAt, setSimulationTraceRefreshedAt] = useState("");
  const refreshSimulationTrace = useCallback(async (event: SimulationEvent) => {
    if (simulationTraceInFlightRef.current.has(event.id)) return;
    simulationTraceInFlightRef.current.add(event.id);
    try {
      const response = unwrap(await apiRequest(settings, `/v1/events/${encodeURIComponent(event.id)}/trace`));
      const trace = response as TraceNode;
      setSimulationEvents(current => current.map(item => item.id === event.id ? {
        ...item,
        trace: { ...trace, effects: trace.effects || [], type: trace.type || event.type, partition_key: trace.partition_key || event.partitionKey, shard: event.shard },
      } : item));
      setSimulationTraceError("");
    } catch (e) {
      setSimulationTraceError(e instanceof Error ? e.message : String(e));
    } finally {
      simulationTraceInFlightRef.current.delete(event.id);
    }
  }, [settings]);
  useEffect(() => {
    let stopped = false;
    let failures = 0;
    let timer = 0;
    const poll = async () => {
      const latest = simulationEventsRef.current.slice(0, 8).filter(event =>
        !isTerminalTrace(event.trace) && !simulationTraceInFlightRef.current.has(event.id)
      );
      let failed = false;
      await Promise.all(latest.map(async event => {
        if (simulationTraceInFlightRef.current.has(event.id)) return;
        simulationTraceInFlightRef.current.add(event.id);
        try {
          const response = unwrap(await apiRequest(settings, `/v1/events/${encodeURIComponent(event.id)}/trace`));
          if (!stopped) {
            const trace = response as TraceNode;
            setSimulationEvents(current => current.map(item => item.id === event.id ? {
              ...item,
              trace: { ...trace, effects: trace.effects || [], type: trace.type || event.type, partition_key: trace.partition_key || event.partitionKey, shard: event.shard },
            } : item));
            setSimulationTraceError("");
            setSimulationTraceRefreshedAt(new Date().toLocaleTimeString());
          }
        } catch (e) {
          failed = true;
          if (!stopped) setSimulationTraceError(`${e instanceof Error ? e.message : String(e)} — showing the last known trace; retrying with backoff`);
        } finally {
          simulationTraceInFlightRef.current.delete(event.id);
        }
      }));
      failures = failed ? Math.min(failures + 1, 5) : 0;
    };
    const loop = async () => {
      await poll();
      if (stopped) return;
      const delay = Math.min(3000 * 2 ** failures, 60000) * (1 + Math.random() * 0.2);
      timer = window.setTimeout(() => void loop(), delay);
    };
    timer = window.setTimeout(() => void loop(), 3000);
    return () => { stopped = true; window.clearTimeout(timer); };
  }, [settings]);
  const startSimulation = () => {
    setSimulationError("");
    setSimulationNotice("Business-event publishing is intentionally disabled in the control-plane console. Publish through the Go SDK with FLOWRULZ_TRANSPORT=nats; use this console for configuration and runtime observability.");
  };
  const stopSimulation = () => {
    simulationStopRequestedRef.current = true;
    setSimulationStopping(true);
    simulationWaitRef.current?.abort();
  };
  const loadLogs = async () => {
    if (!settings.lokiBase.trim()) { setLogsError("Configure a Loki-compatible query URL in Settings to browse runtime logs. The API currently does not expose a log-tail endpoint."); setLogs([]); return; }
    setLogsBusy(true); setLogsError("");
    try {
      const end = Date.now() * 1_000_000; const start = end - 60 * 60 * 1_000_000_000;
      const url = new URL(normalizeBase(settings.lokiBase) + "/loki/api/v1/query_range", window.location.origin);
      url.searchParams.set("query", settings.lokiQuery || '{service_name="flowrulz-worker"}'); url.searchParams.set("start", String(start)); url.searchParams.set("end", String(end)); url.searchParams.set("limit", "200"); url.searchParams.set("direction", "backward");
      const result = await readResponse(await fetch(url.toString(), { headers: settings.token ? { Authorization: `Bearer ${settings.token}` } : {} }));
      const streams = result?.data?.result || [];
      setLogs(streams.flatMap((stream: any) => (stream.values || []).map((entry: any[]) => ({ ts: Number(entry[0]), timestamp: new Date(Number(entry[0]) / 1_000_000).toLocaleString(), labels: stream.stream, line: entry[1] }))).sort((a: any,b: any) => b.ts - a.ts));
    } catch (e) { setLogsError(e instanceof Error ? e.message : String(e)); } finally { setLogsBusy(false); }
  };

  const loadSimulationMetrics = async () => {
    if (!settings.workerMetrics.trim()) {
      setSimulationLogsError("Configure worker metrics endpoint in Settings to view simulation metrics.");
      return;
    }
    try {
      const endpoints = settings.workerMetrics.split(/[\n,]+/).map((s) => s.trim()).filter(Boolean);
      const allMetrics: string[] = [];
      for (const url of endpoints) {
        try {
          const response = await fetch(url);
          if (response.ok) {
            const text = await response.text();
            allMetrics.push(text);
          }
        } catch { /* ignore individual worker failures */ }
      }
      const combined = allMetrics.join("\n");
      // Parse metrics for current simulation scenarios
      const scenarioMetrics: Record<string, any> = {};
      const scenarios = simulationScenario === "all" ? ["ecommerce", "banking", "food-delivery"] : [simulationScenario];
      for (const scenario of scenarios) {
        const sourceRule = SOURCE_RULE_BY_SCENARIO[scenario as Exclude<SimulationScenario, "all">];
        if (!sourceRule) continue;
        scenarioMetrics[scenario] = {
          eventsReceived: parseMetric(combined, `flowrulz_events_received_total{rule_set="${sourceRule}"}`),
          executionsTotal: parseMetric(combined, `flowrulz_executions_total{rule_set="${sourceRule}"}`),
          executionDuration: parseMetric(combined, `flowrulz_execution_duration_seconds_sum{rule_set="${sourceRule}"}`),
          executionCount: parseMetric(combined, `flowrulz_execution_duration_seconds_count{rule_set="${sourceRule}"}`),
          outboxPending: parseMetric(combined, `flowrulz_outbox_effects_pending{rule_set="${sourceRule}"}`),
        };
      }
      return scenarioMetrics;
    } catch (e) {
      setSimulationLogsError(e instanceof Error ? e.message : String(e));
      return null;
    }
  };

  const loadSimulationLogs = async () => {
    if (!settings.lokiBase.trim()) {
      setSimulationLogsError("Configure a Loki-compatible query URL in Settings to browse simulation logs.");
      return;
    }
    setSimulationLogsBusy(true);
    setSimulationLogsError("");
    try {
      const end = Date.now() * 1_000_000;
      const start = end - 10 * 60 * 1_000_000_000; // Last 10 minutes
      const scenarios = simulationScenario === "all" ? ["ecommerce", "banking", "food-delivery"] : [simulationScenario];
      const scenarioFilter = scenarios.map(s => `scenario="${s}"`).join("|");
      const query = `{service_name=~"flowrulz-.*"} |~ "${scenarioFilter}" | json`;
      const url = new URL(normalizeBase(settings.lokiBase) + "/loki/api/v1/query_range", window.location.origin);
      url.searchParams.set("query", query);
      url.searchParams.set("start", String(start));
      url.searchParams.set("end", String(end));
      url.searchParams.set("limit", "200");
      url.searchParams.set("direction", "backward");
      const result = await readResponse(await fetch(url.toString(), { headers: settings.token ? { Authorization: `Bearer ${settings.token}` } : {} }));
      const streams = result?.data?.result || [];
      const logs = streams.flatMap((stream: any) => (stream.values || []).map((entry: any[]) => ({
        ts: Number(entry[0]),
        timestamp: new Date(Number(entry[0]) / 1_000_000).toLocaleString(),
        level: parseLogLevel(entry[1]),
        service: stream.stream?.service_name || "unknown",
        message: parseLogMessage(entry[1]),
        labels: stream.stream || {},
        eventId: parseEventId(entry[1]),
        scenario: parseScenario(entry[1]),
      }))).sort((a: any, b: any) => b.ts - a.ts);
      setSimulationLogs(logs);
    } catch (e) {
      setSimulationLogsError(e instanceof Error ? e.message : String(e));
    } finally {
      setSimulationLogsBusy(false);
    }
  };

  const parseMetric = (text: string, metricName: string): number => {
    const lines = text.split("\n");
    for (const line of lines) {
      if (line.startsWith(metricName + " ") || line.startsWith(metricName + "{")) {
        const parts = line.trim().split(/\s+/);
        const value = Number(parts[parts.length - 1]);
        return Number.isFinite(value) ? value : 0;
      }
    }
    return 0;
  };

  const parseLogLevel = (line: string): string => {
    const levelMatch = line.match(/"level"\s*:\s*"(\w+)"/);
    return levelMatch ? levelMatch[1].toUpperCase() : "INFO";
  };

  const parseLogMessage = (line: string): string => {
    const msgMatch = line.match(/"msg"\s*:\s*"([^"]*)"/);
    return msgMatch ? msgMatch[1] : line.slice(0, 200);
  };

  const parseEventId = (line: string): string | undefined => {
    const match = line.match(/"event_id"\s*:\s*"([^"]*)"/);
    return match ? match[1] : undefined;
  };

  const parseScenario = (line: string): string | undefined => {
    const match = line.match(/"scenario"\s*:\s*"([^"]*)"/);
    return match ? match[1] : undefined;
  };

  // ===== NEW SIMULATION COMPONENTS =====

  function SimulationMetricsPanel({ settings, scenario, running }: { settings: Settings; scenario: SimulationScenario; running: boolean }) {
    const [metrics, setMetrics] = useState<Record<string, any> | null>(null);
    const [error, setError] = useState("");
    const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

    const fetchMetrics = useCallback(async () => {
      if (!settings.workerMetrics.trim()) return;
      try {
        const endpoints = settings.workerMetrics.split(/[\n,]+/).map((s) => s.trim()).filter(Boolean);
        let combined = "";
        for (const url of endpoints) {
          try {
            const response = await fetch(url);
            if (response.ok) combined += await response.text();
          } catch { /* ignore */ }
        }
        const scenarioMetrics: Record<string, any> = {};
        const scenarios = scenario === "all" ? ["ecommerce", "banking", "food-delivery"] : [scenario];
        for (const sc of scenarios) {
          const sourceRule = SOURCE_RULE_BY_SCENARIO[sc as Exclude<SimulationScenario, "all">];
          if (!sourceRule) continue;
          scenarioMetrics[sc] = {
            eventsReceived: parseMetric(combined, `flowrulz_events_received_total{rule_set="${sourceRule}"}`),
            eventsRejected: parseMetric(combined, `flowrulz_events_received_total{rule_set="${sourceRule}",status="rejected"}`),
            executionsTotal: parseMetric(combined, `flowrulz_executions_total{rule_set="${sourceRule}"}`),
            executionsSuccess: parseMetric(combined, `flowrulz_executions_total{rule_set="${sourceRule}",status="completed"}`),
            executionsFailed: parseMetric(combined, `flowrulz_executions_total{rule_set="${sourceRule}",status="failed"}`),
            avgExecDuration: calculateAvg(combined, sourceRule),
            outboxPending: parseMetric(combined, `flowrulz_outbox_effects_pending{rule_set="${sourceRule}"}`),
            outboxDelivered: parseMetric(combined, `flowrulz_outbox_retries_total{destination=".*",effect_type="emit_event"}`),
            outboxRetries: parseMetric(combined, `flowrulz_outbox_retries_total{rule_set="${sourceRule}"}`),
            quarantineTotal: parseMetric(combined, `flowrulz_quarantine_total{rule_set="${sourceRule}"}`),
          };
        }
        setMetrics(scenarioMetrics);
        setLastRefresh(new Date());
        setError("");
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      }
    }, [settings, scenario]);

    useEffect(() => {
      if (!running) return;
      fetchMetrics();
      const interval = window.setInterval(fetchMetrics, 5000);
      return () => window.clearInterval(interval);
    }, [running, fetchMetrics]);

    useEffect(() => {
      fetchMetrics();
    }, [fetchMetrics]);

    if (!settings.workerMetrics.trim()) {
      return <section className="panel metrics-panel"><div className="panel-heading"><h3>Simulation Metrics</h3></div><div className="metrics-empty">Configure worker metrics endpoint in Settings to see real-time metrics.</div></section>;
    }

    const scenarios = scenario === "all" ? ["ecommerce", "banking", "food-delivery"] : [scenario];

    return <section className="panel metrics-panel">
      <div className="panel-heading">
        <div><h3>Real-time Metrics</h3><p>Per-scenario Prometheus metrics from worker fleet</p></div>
        <div className="metrics-actions">
          <small className="last-refresh">{lastRefresh ? `Updated ${lastRefresh.toLocaleTimeString()}` : "Loading…"}</small>
          <button className="text-button" onClick={fetchMetrics} disabled={!running}>↻ Refresh</button>
        </div>
      </div>
      {error && <div className="notice notice-error">{error}</div>}
      <div className="metrics-grid">
        {scenarios.map(sc => {
          const m = metrics?.[sc] || {};
          const successRate = m.executionsTotal > 0 ? ((m.executionsSuccess / m.executionsTotal) * 100).toFixed(1) : "—";
          return <article className="metric-card scenario-card" key={sc}>
            <div className="metric-top"><span className="scenario-label">{sc === "food-delivery" ? "Food Delivery" : sc}</span><span className="metric-icon">📊</span></div>
            <div className="metrics-detail">
              <div className="metric-row"><span>Events received</span><strong>{m.eventsReceived || 0}</strong></div>
              <div className="metric-row"><span>Events rejected</span><strong style={{color: m.eventsRejected > 0 ? "var(--red)" : "inherit"}}>{m.eventsRejected || 0}</strong></div>
              <div className="metric-row"><span>Executions</span><strong>{m.executionsTotal || 0}</strong></div>
              <div className="metric-row"><span>Success rate</span><strong>{successRate}%</strong></div>
              <div className="metric-row"><span>Avg duration</span><strong>{m.avgExecDuration ? m.avgExecDuration.toFixed(3) + "s" : "—"}</strong></div>
              <div className="metric-row"><span>Outbox pending</span><strong>{m.outboxPending || 0}</strong></div>
              <div className="metric-row"><span>Outbox retries</span><strong>{m.outboxRetries || 0}</strong></div>
              <div className="metric-row"><span>Quarantined</span><strong style={{color: m.quarantineTotal > 0 ? "var(--red)" : "inherit"}}>{m.quarantineTotal || 0}</strong></div>
            </div>
          </article>;
        })}
      </div>
    </section>;
  }

  function SimulationLogsPanel({ settings, scenario, running, logs, busy, error, onRefresh }: { settings: Settings; scenario: SimulationScenario; running: boolean; logs: SimulationLogEntry[]; busy: boolean; error: string; onRefresh: () => void }) {
    const [filterLevel, setFilterLevel] = useState<string>("all");
    const [filterText, setFilterText] = useState("");
    const [autoRefresh, setAutoRefresh] = useState(running);

    useEffect(() => {
      if (!running || !autoRefresh) return;
      onRefresh();
      const interval = window.setInterval(onRefresh, 10000);
      return () => window.clearInterval(interval);
    }, [running, autoRefresh, onRefresh]);

    const filteredLogs = logs.filter(log => {
      if (filterLevel !== "all" && log.level.toLowerCase() !== filterLevel.toLowerCase()) return false;
      if (filterText && !log.message.toLowerCase().includes(filterText.toLowerCase()) && !log.eventId?.toLowerCase().includes(filterText.toLowerCase())) return false;
      return true;
    });

    return <section className="panel logs-panel">
      <div className="panel-heading">
        <div><h3>Simulation Logs</h3><p>Loki logs filtered by scenario, correlated with event traces</p></div>
        <div className="logs-actions">
          <label className="filter-label">
            Level
            <select value={filterLevel} onChange={e => setFilterLevel(e.target.value)} style={{marginLeft: 8}}>
              <option value="all">All</option>
              <option value="error">Error</option>
              <option value="warn">Warn</option>
              <option value="info">Info</option>
              <option value="debug">Debug</option>
            </select>
          </label>
          <label className="filter-label">
            Search
            <input type="text" value={filterText} onChange={e => setFilterText(e.target.value)} placeholder="event ID, message..." style={{marginLeft: 8, width: 200}} />
          </label>
          <label className="filter-label">
            <input type="checkbox" checked={autoRefresh} onChange={e => setAutoRefresh(e.target.checked)} />
            <span style={{marginLeft: 4}}>Auto-refresh ({running ? "live" : "paused"})</span>
          </label>
          <button className="text-button" onClick={onRefresh} disabled={busy}>{busy ? "Loading…" : "↻ Refresh"}</button>
        </div>
      </div>
      {error && <div className="notice notice-error">{error}</div>}
      {!settings.lokiBase.trim() && <div className="metrics-empty">Configure Loki endpoint in Settings to see simulation logs.</div>}
      <div className="logs-list">
        {filteredLogs.length === 0 ? (
          <div className="logs-empty">No logs found. Start a simulation and ensure Loki is configured.</div>
        ) : (
          filteredLogs.slice(0, 50).map((log, idx) => (
            <div className={`log-row log-${log.level.toLowerCase()}`} key={idx}>
              <time>{log.timestamp}</time>
              <span className="log-service">{log.service}</span>
              <span className={`log-level log-level-${log.level.toLowerCase()}`}>{log.level}</span>
              {log.scenario && <span className="log-scenario">{log.scenario}</span>}
              {log.eventId && <span className="log-event-id" title={log.eventId}>{log.eventId.slice(0, 12)}…</span>}
              <code className="log-message">{log.message}</code>
            </div>
          ))
        )}
      </div>
    </section>;
  }

  function EnhancedTraceBranch({ node, depth = 0 }: { node: TraceNode; depth?: number }) {
    const execution = node.execution;
    const isCompleted = execution?.status === "completed";
    const hasErrors = node.effects.some(e => e.status.toLowerCase() === "failed" || e.status.toLowerCase() === "quarantined");

    // Collect all emitted event types from effects
    const emittedEventTypes = node.effects
      .filter(e => e.effect_type === "emit_event")
      .map(e => e.name)
      .filter((v, i, a) => a.indexOf(v) === i);

    // Collect external commands
    const externalCommands = node.effects
      .filter(e => e.effect_type === "external_command" || e.effect_type === "command")
      .map(e => ({ name: e.name, destination: e.destination }));

    // Collect service events
    const serviceEvents = node.effects
      .filter(e => e.effect_type === "service_event")
      .map(e => e.name);

    return <div className={`trace-branch${depth > 0 ? " trace-branch-child" : ""}`}>
      <div className="trace-event">
        <div className="trace-event-top">
          <div className="trace-event-info">
            <strong>{node.type || "Event waiting for worker"}</strong>
            <span className="trace-scenario-badge">{node.partition_key ? `partition: ${node.partition_key}` : ""}</span>
            <span className="trace-shard-badge">{node.shard ? `shard: ${node.shard}` : ""}</span>
          </div>
          <Status value={node.status}/>
        </div>
        <div className="trace-event-meta">
          <span>id {node.event_id.slice(0, 12)}</span>
          {node.partition_key && <span>key {node.partition_key}</span>}
          {node.shard && <span>events.shard.{node.shard}</span>}
          {execution?.created_at && <span>created {new Date(execution.created_at).toLocaleTimeString()}</span>}
          {execution?.completed_at && <span>completed {new Date(execution.completed_at).toLocaleTimeString()}</span>}
        </div>
        {execution && (
          <div className="trace-execution-detail">
            <small className="trace-execution">Rule set: <strong>{execution.rule_set}</strong> · revision {execution.revision} · status: {execution.status}</small>
            {execution.created_at && execution.completed_at && (
              <small className="trace-duration">
                Duration: {((new Date(execution.completed_at).getTime() - new Date(execution.created_at).getTime()) / 1000).toFixed(3)}s
              </small>
            )}
          </div>
        )}
        {(emittedEventTypes.length > 0 || externalCommands.length > 0 || serviceEvents.length > 0) && (
          <div className="trace-output-summary">
            {emittedEventTypes.length > 0 && (
              <span className="output-badge emit-event">
                📤 Emits: {emittedEventTypes.join(", ")}
              </span>
            )}
            {externalCommands.length > 0 && (
              <span className="output-badge external-command">
                ⚡ Commands: {externalCommands.map(c => `${c.name}→${c.destination}`).join(", ")}
              </span>
            )}
            {serviceEvents.length > 0 && (
              <span className="output-badge service-event">
                📬 Service events: {serviceEvents.join(", ")}
              </span>
            )}
          </div>
        )}
      </div>
      {node.effects.map(effect => {
        const isEmitEvent = effect.effect_type === "emit_event";
        const isExternal = effect.effect_type === "external_command" || effect.effect_type === "command";
        const childStatus = effect.child?.status;
        const childExecution = effect.child?.execution;
        return <div className="trace-effect" key={effect.effect_id}>
          <div className="trace-effect-line">
            <span className={`effect-mark effect-${effect.effect_type.replace(/_/g, "-")}`}>
              {isEmitEvent ? "↗" : isExternal ? "⇢" : "📬"}
            </span>
            <div className="effect-detail">
              <div className="effect-header">
                <strong>{effect.name}</strong>
                <small>{effect.effect_type} → {effect.destination}</small>
              </div>
              <div className="effect-status-row">
                <Status value={effect.status}/>
                {effect.child && <span className="effect-child-status">→ child: {effect.child.type} ({childStatus})</span>}
              </div>
            </div>
          </div>
          {effect.child && depth < 5 && (
            <div className="trace-effect-child">
              <EnhancedTraceBranch node={effect.child} depth={depth + 1} />
            </div>
          )}
          {isEmitEvent && childExecution && (
            <div className="effect-decision">
              <small>📋 Rule <strong>{childExecution.rule_set}</strong> evaluated this child event</small>
            </div>
          )}
          {isExternal && (
            <div className="effect-external">
              <small>📤 External command sent to <strong>{effect.destination}</strong> — receiver must deduplicate by Idempotency-Key</small>
            </div>
          )}
          {hasErrors && effect.status.toLowerCase() === "failed" && (
            <div className="effect-error">
              <small>⚠️ Delivery failed — will retry per outbox policy; may be quarantined after max retries</small>
            </div>
          )}
          {hasErrors && effect.status.toLowerCase() === "quarantined" && (
            <div className="effect-quarantined">
              <small>🔒 Quarantined — manual intervention required; check quarantine API</small>
            </div>
          )}
        </div>;
      })}
      {node.truncated && <div className="trace-truncated">Trace shortened at the display limit (max 100 nodes, 5 levels deep).</div>}
    </div>;
  }

  const calculateAvg = (text: string, ruleSet: string): number | null => {
    const sum = parseMetric(text, `flowrulz_execution_duration_seconds_sum{rule_set="${ruleSet}"}`);
    const count = parseMetric(text, `flowrulz_execution_duration_seconds_count{rule_set="${ruleSet}"}`);
    return count > 0 ? sum / count : null;
  };

  // ===== NEW: Rule Chain Flow with Contracts =====
  interface ContractSummary {
    name: string;
    version: string;
    fields: string[];
    requiredFields: string[];
  }

  interface RuleSetNode {
    ruleSet: string;
    revision: number;
    matchMode: string;
    inputContract?: ContractSummary;
    outputContracts: ContractSummary[];
    externalCommands: Array<{ name: string; destination: string }>;
    branches: RuleBranch[];
    isEntry?: boolean;
  }

  interface RuleBranch {
    id: string;
    emits: Array<{ type: string; destination: string }>;
  }

  function RuleChainFlow({ settings, scenario, events, running }: { settings: Settings; scenario: string; events: SimulationEvent[]; running: boolean }) {
    const [chain, setChain] = useState<RuleSetNode[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

    useEffect(() => {
      let cancelled = false;
      setLoading(true);
      setError("");

      async function loadChain() {
        try {
          // Get all domains for this tenant
          const domainsResp = await apiRequest(settings, "/v1/domains");
          const domains = asArray(domainsResp);
          const domain = domains.find((d: any) => d.name === scenario);
          if (!domain) throw new Error(`Domain ${scenario} not found`);

          // Get all active rules
          const activeResp = await apiRequest(settings, "/v1/rules/active");
          const activeRules = asArray(activeResp);

          // Get all contracts
          const contractsResp = await apiRequest(settings, "/v1/contracts");
          const contracts = asArray(contractsResp);

          // Build rule set nodes for this domain
          const domainEventTypes = domain.event_types || [];
          const ruleSets = activeRules
            .filter((r: any) => domainEventTypes.includes(r.rule_set || r.rule_id))
            .map((r: any) => ({ name: r.rule_set || r.rule_id, revision: r.revision }));

          // For each rule set, get its revision details and contracts
          const nodes: RuleSetNode[] = [];
          for (const rs of ruleSets) {
            try {
              const revResp = await apiRequest(settings, `/v1/rules/${encodeURIComponent(rs.name)}/revisions/${rs.revision}`);
              const revision = unwrap(revResp);
              
              // Parse input contract
              let inputContract: ContractSummary | undefined;
              if (revision.input_contract) {
                const contract = contracts.find((c: any) => c.name === revision.input_contract.name && c.version === revision.input_contract.version);
                if (contract) {
                  const fields = Object.keys(contract.fields || {});
                  const requiredFields = Object.entries(contract.fields || {})
                    .filter(([, f]: [string, any]) => f.required)
                    .map(([k]) => k);
                  inputContract = {
                    name: contract.name,
                    version: contract.version,
                    fields,
                    requiredFields
                  };
                }
              }

              // Parse output contracts from emit_event actions
              const outputContracts: ContractSummary[] = [];
              const externalCommands: Array<{ name: string; destination: string }> = [];
              const emittedTypes = new Set<string>();

              if (revision.source) {
                try {
                  const source = JSON.parse(revision.source);
                  if (source.rules) {
                    for (const rule of source.rules) {
                      if (rule.then) {
                        for (const action of rule.then) {
                          if (action.emit_event) {
                            emittedTypes.add(action.emit_event.type);
                          }
                          if (action.command) {
                            externalCommands.push({
                              name: action.command.name,
                              destination: action.command.destination
                            });
                          }
                        }
                      }
                      if (rule.otherwise) {
                        for (const action of rule.otherwise) {
                          if (action.emit_event) {
                            emittedTypes.add(action.emit_event.type);
                          }
                          if (action.command) {
                            externalCommands.push({
                              name: action.command.name,
                              destination: action.command.destination
                            });
                          }
                        }
                      }
                    }
                  }
                } catch { /* ignore parse errors */ }
              }

              // Match emitted types to contracts
              for (const type of emittedTypes) {
                const contract = contracts.find((c: any) => c.name === type);
                if (contract) {
                  const fields = Object.keys(contract.fields || {});
                  const requiredFields = Object.entries(contract.fields || {})
                    .filter(([, f]: [string, any]) => f.required)
                    .map(([k]) => k);
                  outputContracts.push({
                    name: contract.name,
                    version: contract.version,
                    fields,
                    requiredFields
                  });
                }
              }

              // Build branches for flow visualization
              const branches: RuleBranch[] = [];
              if (revision.source) {
                try {
                  const source = JSON.parse(revision.source);
                  if (source.rules) {
                    for (const rule of source.rules) {
                      const emits: Array<{ type: string; destination: string }> = [];
                      if (rule.then) {
                        for (const action of rule.then) {
                          if (action.emit_event) {
                            emits.push({ type: action.emit_event.type, destination: "next rule set" });
                          }
                          if (action.command) {
                            emits.push({ type: action.command.name, destination: action.command.destination });
                          }
                        }
                      }
                      if (emits.length > 0) {
                        branches.push({ id: rule.id, emits });
                      }
                    }
                  }
                } catch { /* ignore */ }
              }

              nodes.push({
                ruleSet: rs.name,
                revision: rs.revision,
                matchMode: revision.match_mode || "all_matches",
                inputContract,
                outputContracts,
                externalCommands,
                branches,
                isEntry: SOURCE_RULE_BY_SCENARIO[scenario as Exclude<SimulationScenario, "all">] === rs.name
              });
            } catch (e) {
              console.warn(`Failed to load revision for ${rs.name}:`, e);
            }
          }

          // Sort: entry first, then by dependency order
          nodes.sort((a, b) => {
            if (a.isEntry && !b.isEntry) return -1;
            if (!a.isEntry && b.isEntry) return 1;
            return 0;
          });

          if (!cancelled) {
            setChain(nodes);
            setLoading(false);
          }
        } catch (e) {
          if (!cancelled) {
            setError(e instanceof Error ? e.message : String(e));
            setLoading(false);
          }
        }
      }

      loadChain();
      return () => { cancelled = true; };
    }, [settings, scenario, running]);

    if (loading) return <section className="panel rule-chain-panel"><div className="panel-heading"><h3>Rule Chain Flow</h3><p>Loading chain for {DOMAIN_LABELS[scenario] || scenario}...</p></div><div className="chain-loading">Building contract graph...</div></section>;

    if (error) return <section className="panel rule-chain-panel"><div className="panel-heading"><h3>Rule Chain Flow</h3></div><div className="notice notice-error">{error}</div></section>;

    if (chain.length === 0) return <section className="panel rule-chain-panel"><div className="panel-heading"><h3>Rule Chain Flow</h3></div><Empty title="No active rule sets" detail={`Save the ${DOMAIN_LABELS[scenario] || scenario} domain from the Domain catalog to see its flow.`}/></section>;

    // Check which nodes have been observed in traces
    const observedRuleSets = new Set<string>();
    events.filter(e => e.scenario === scenario).forEach(e => {
      if (e.trace?.execution?.rule_set) observedRuleSets.add(e.trace.execution.rule_set);
      e.trace?.effects.forEach(fx => {
        if (fx.child?.execution?.rule_set) observedRuleSets.add(fx.child.execution.rule_set);
      });
    });

    return <section className="panel rule-chain-panel">
      <div className="panel-heading">
        <div><h3>{DOMAIN_LABELS[scenario] || scenario} Rule Chain</h3><p>Complete flow from entry rule through all downstream rule sets, showing input/output contracts at each hop.</p></div>
        <span className="chain-badge">{chain.length} rule set{chain.length !== 1 ? "s" : ""} · {observedRuleSets.size} observed in traces</span>
      </div>
      <div className="rule-chain-flow">
        {chain.map((node, idx) => (
          <div className="rule-chain-node" key={node.ruleSet}>
            {idx > 0 && <div className="chain-connector">↓</div>}
            <div className={`chain-node-card ${node.isEntry ? "entry-node" : ""} ${observedRuleSets.has(node.ruleSet) ? "observed" : ""}`}>
              <div className="chain-node-header">
                <span className="chain-node-badge">{node.isEntry ? "ENTRY" : `HOP ${idx}`}</span>
                <strong>{node.ruleSet}</strong>
                <small>rev {node.revision} · {node.matchMode}</small>
                {observedRuleSets.has(node.ruleSet) && <span className="observed-badge">✓ Observed</span>}
              </div>

              {/* Input Contract */}
              <div className="contract-section input-contract">
                <h5>📥 Input Contract</h5>
                {node.inputContract ? (
                  <div className="contract-detail">
                    <div className="contract-name">{node.inputContract.name} v{node.inputContract.version}</div>
                    <div className="contract-fields">
                      <span className="field-count">{node.inputContract.fields.length} fields</span>
                      {node.inputContract.requiredFields.length > 0 && (
                        <span className="required-fields">
                          {node.inputContract.requiredFields.slice(0, 5).map(f => <code key={f}>{f}</code>)}
                          {node.inputContract.requiredFields.length > 5 && <span>+{node.inputContract.requiredFields.length - 5} more</span>}
                        </span>
                      )}
                    </div>
                  </div>
                ) : (
                  <div className="contract-none">No input contract specified (accepts any event matching rule set)</div>
                )}
              </div>

              {/* Processing */}
              <div className="contract-section processing">
                <h5>⚙ Processing</h5>
                <div className="processing-detail">
                  <div className="branch-list">
                    {node.branches.length > 0 ? (
                      node.branches.map(branch => (
                        <div className="branch-item" key={branch.id}>
                          <span className="branch-id">{branch.id}</span>
                          {branch.emits.map((emit, i) => (
                            <span className="branch-emit" key={i}>
                              → <code>{emit.type}</code> <span className="emit-dest">({emit.destination})</span>
                            </span>
                          ))}
                        </div>
                      ))
                    ) : (
                      <span className="no-branches">No emit actions defined</span>
                    )}
                  </div>
                  {node.externalCommands.length > 0 && (
                    <div className="external-commands">
                      <h6>External Commands:</h6>
                      {node.externalCommands.map(cmd => (
                        <div className="cmd-item" key={cmd.name}>
                          <code>{cmd.name}</code> → <span>{cmd.destination}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>

              {/* Output Contracts */}
              <div className="contract-section output-contracts">
                <h5>📤 Output Contracts</h5>
                {node.outputContracts.length > 0 ? (
                  <div className="contract-grid">
                    {node.outputContracts.map((contract, i) => (
                      <div className="contract-card" key={i}>
                        <div className="contract-name">{contract.name} v{contract.version}</div>
                        <div className="contract-fields">
                          <span className="field-count">{contract.fields.length} fields</span>
                          {contract.requiredFields.length > 0 && (
                            <span className="required-fields">
                              {contract.requiredFields.slice(0, 4).map(f => <code key={f}>{f}</code>)}
                              {contract.requiredFields.length > 4 && <span>+{contract.requiredFields.length - 4} more</span>}
                            </span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="contract-none">No output contracts (terminal or emits without contracts)</div>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>;
  }

  return <div className="app-shell">
    <aside className="sidebar">
      <div className="brand"><div className="brand-mark">F</div><div><strong>FlowRulz</strong><small>CONTROL PLANE</small></div></div>
      <div className="workspace-label">WORKSPACE</div>
      <div className="workspace"><span className="workspace-dot" />{settings.tenant || "No tenant"}<span className="chevron">⌄</span></div>
      <nav>{NAV.map(item => <button key={item.id} className={nav === item.id ? "nav-item selected" : "nav-item"} onClick={() => setNav(item.id)} title={item.label} aria-label={item.label} aria-current={nav === item.id ? "page" : undefined}><span className="nav-icon" aria-hidden="true">{item.icon}</span>{item.label}{item.id === "workers" && workers.length > 0 && <span className="nav-count">{onlineWorkers}/{workers.length}</span>}</button>)}</nav>
      <div className="sidebar-bottom"><div className="connection-line"><span className={health === "online" ? "dot green" : health === "offline" ? "dot red" : "dot amber"} />API {health}</div><button className="settings-link" onClick={() => setSettingsOpen(true)}>⚙ Settings</button><div className="version-label">FlowRulz Console · React</div></div>
    </aside>
    <main className="main-area">
      <header className="topbar"><div><div className="breadcrumbs">FlowRulz <span>/</span> {NAV.find(item => item.id === nav)?.label}</div><h1>{({ overview: "Runtime overview", simulations: "Domain simulations", api: "API explorer", codegen: "Contract code generation", workers: "Workers & rules", rules: "Rule management", logs: "Runtime logs" } as Record<NavKey,string>)[nav]}</h1></div><div className="top-actions"><span className="live-indicator"><i /> AUTO REFRESH · 10S</span><button className="icon-button" title="Refresh data" onClick={() => void refresh()} disabled={loading}>↻</button><button className="button secondary" onClick={() => setSettingsOpen(true)}>⚙ Settings</button><div className="avatar">FR</div></div></header>
      {error && <div className="alert error"><strong>API connection issue</strong><span>{error}</span><button onClick={() => setError("")}>×</button></div>}
      {nav === "overview" && <section className="page-content">
        <div className="welcome-row"><div><div className="eyebrow">SYSTEM SNAPSHOT</div><h2>Runtime at a glance</h2><p>Rules, workers, and contract generation in one place.</p></div><div className="updated">Last refreshed <strong>{lastRefresh?.toLocaleTimeString() || "—"}</strong></div></div>
        <div className="metric-grid"><MetricCard label="API status" value={health === "online" ? "Online" : health === "offline" ? "Offline" : "Checking"} hint={dependencies ? Object.entries(dependencies).map(([k, v]) => `${k} ${v.status}`).join(" · ") : `${normalizeBase(settings.apiBase)} · dependency state unknown`} icon="⌘"/><MetricCard label="Active rules" value={ruleCount} hint="From the tenant-scoped rules API" icon="◎"/><MetricCard label="Workers online" value={workers.length ? `${onlineWorkers}/${workers.length}` : "Not configured"} hint="Worker Prometheus endpoints" icon="▦"/><MetricCard label="Rule executions" value={workers.length ? executionTotal.toLocaleString() : "—"} hint="Cumulative counters from configured workers" icon="↗"/></div>
        <div className="content-grid"><section className="panel"><div className="panel-heading"><div><h3>Worker fleet</h3><p>Process-level telemetry, refreshed every 10 seconds</p></div><button className="text-button" onClick={() => setNav("workers")}>View workers ↗</button></div>{workers.length === 0 ? <Empty title="No worker endpoints configured" detail="Open Settings and add one private /metrics URL per worker process. The UI does not invent worker health."/> : <div className="worker-list">{workers.slice(0,4).map(worker => <div className="worker-row" key={worker.url}><div className="worker-symbol">▦</div><div className="worker-main"><strong>{worker.name}</strong><small>{worker.url}</small></div><Status value={worker.online ? "Online" : "Unreachable"}/><div className="worker-stat"><strong>{worker.inFlight ?? "—"}</strong><small>in flight</small></div><div className="worker-stat"><strong>{worker.shards ?? "—"}</strong><small>shards owned</small></div></div>)}</div>}</section>
        <section className="panel"><div className="panel-heading"><div><h3>Active rule sets</h3><p>Current activation state from the API</p></div><button className="text-button" onClick={() => setNav("rules")}>Manage ↗</button></div>{activeRules.length === 0 ? <Empty title="No active rules returned" detail={health === "online" ? "No active rules were found for this tenant." : "Connect to the API to load active rules."}/> : <div className="rule-mini-list">{activeRules.slice(0,6).map((rule,i) => <div className="rule-mini" key={rule.rule_id || rule.rule_set || rule.id || i}><div className="rule-symbol">R</div><div className="rule-main"><strong>{rule.rule_id || rule.rule_set || rule.ruleSet || rule.id || `Rule set ${i+1}`}</strong><small>{tenantDomains.find(d => (d.event_types ?? []).includes(rule.rule_set || rule.rule_id))?.name ?? "no domain"} · revision {rule.revision ?? rule.active_revision ?? "—"}</small></div><Status value={rule.status || "Active"}/></div>)}</div>}</section></div>
        <section className="panel"><div className="panel-heading"><div><h3>Quick actions</h3><p>Jump directly to a common operation</p></div></div><div className="quick-grid"><button onClick={() => setNav("api")}><span>⌘</span><strong>Explore API</strong><small>Inspect routes and send requests</small><b>→</b></button><button onClick={() => setNav("codegen")}><span>⟐</span><strong>Generate a client</strong><small>Go, Java, Protobuf, JSON Schema</small><b>→</b></button><button onClick={() => setNav("rules")}><span>◎</span><strong>Deploy a rule</strong><small>Validate and activate a revision</small><b>→</b></button><button onClick={() => setNav("logs")}><span>≋</span><strong>Inspect logs</strong><small>Query your configured Loki source</small><b>→</b></button></div></section>
      </section>}
      {nav === "simulations" && <section className="page-content simulation-page">
        <div className="simulation-hero">
          <div className="simulation-hero-copy"><div className="eyebrow">LIVE EVENT FLOW</div><h2>Send a service event through the real worker.</h2><p>The simulator publishes the existing example-domain events to this tenant. Follow the actual shard, active rule set, execution, and outbox destinations below.</p></div>
          <div className="simulation-controls">
            <label>Domain<select value={simulationScenario} disabled={simulationRunning} onChange={event => setSimulationScenario(event.target.value as SimulationScenario)}><option value="all">Rotate all example domains</option><option value="ecommerce">E-commerce</option><option value="banking">Banking</option><option value="food-delivery">Food delivery</option></select></label>
            <label>Refresh pace<select value={simulationPace} disabled={simulationRunning} onChange={event => setSimulationPace(Number(event.target.value))}><option value={4000}>Slow · every 4 seconds</option><option value={7000}>Very slow · every 7 seconds</option><option value={10000}>Extra slow · every 10 seconds</option></select></label>
            {simulationRunning
              ? <button className="button stop-button" onClick={stopSimulation} disabled={simulationStopping}>{simulationStopping ? "Stopping…" : "■ Stop simulation"}</button>
              : <button className="button primary" onClick={startSimulation}>NATS producer required</button>}
          </div>
        </div>
        {simulationNotice && <div className="alert warning"><strong>Rule availability</strong><span>{simulationNotice}</span></div>}
        {simulationError && <div className="alert error"><strong>Simulation stopped</strong><span>{simulationError}</span><button onClick={() => setSimulationError("")}>×</button></div>}
        {simulationTraceRefreshedAt && !simulationTraceError && <small className="trace-refreshed">Traces last refreshed {simulationTraceRefreshedAt}</small>}
        {simulationTraceError && <div className="alert warning"><strong>Trace temporarily unavailable</strong><span>{simulationTraceError}</span></div>}
        
        {/* Step-by-step flow visualization */}
        <div className="simulation-flow" aria-label="Live event processing path">
          <div className={`simulation-flow-step flow-${flowStepState(0)}`}><span className="flow-step-icon">A</span><span><strong>Source service</strong><small>{latestSimulationEvent ? `${latestSimulationEvent.scenario} · ${latestSimulationEvent.type}` : "Example producer sends the event"}</small></span></div><span className={`flow-connector flow-${flowConnectorState(0)}`}>→</span>
          <div className={`simulation-flow-step flow-${flowStepState(1)}`}><span className="flow-step-icon">N</span><span><strong>NATS JetStream</strong><small>{latestSimulationEvent ? `events.shard.${latestSimulationEvent.shard}` : "Shared tenant shard routing"}</small></span></div><span className={`flow-connector flow-${flowConnectorState(1)}`}>→</span>
          <div className={`simulation-flow-step flow-${flowStepState(2)}`}><span className="flow-step-icon">W</span><span><strong>Worker + rule set</strong><small>{latestSimulationEvent?.trace?.execution ? `${latestSimulationEvent.trace.execution.rule_set} · ${latestSimulationEvent.trace.execution.status}` : "Waiting for matching rule set"}</small></span></div><span className={`flow-connector flow-${flowConnectorState(2)}`}>→</span>
          <div className={`simulation-flow-step flow-${flowStepState(3)}`}><span className="flow-step-icon">B</span><span><strong>Outbox destination</strong><small>{latestSimulationEvent?.trace?.effects[0] ? `${latestSimulationEvent.trace.effects[0].destination} · ${latestSimulationEvent.trace.effects[0].status}` : "Child event or configured service"}</small></span></div>
          <div className="simulation-flow-status" aria-live="polite"><i className={simulationRunning ? "flow-status-live" : ""}/><span>{simulationRunning ? `Live · publishing every ${simulationPace / 1000}s until stopped` : latestSimulationEvent ? `Latest event · ${latestSimulationEvent.trace?.status || "accepted"}` : "Ready · start a simulation to follow a live event"}</span></div>
        </div>
        
        {/* Real-time metrics panel */}
        <SimulationMetricsPanel 
          settings={settings} 
          scenario={simulationScenario} 
          running={simulationRunning}
        />
        
        <div className="metric-grid simulation-metrics">
          <MetricCard label="Simulation" value={simulationRunning ? simulationStopping ? "Stopping" : "Running" : "Stopped"} hint={`Tenant · ${settings.tenant || "not configured"}`} icon={simulationRunning ? "●" : "○"}/>
          <MetricCard label="Events tracked" value={simulationEvents.length} hint="Recent simulator events, newest first" icon="↗"/>
          <MetricCard label="Worker processed" value={simulationProcessed} hint="Completed executions in current event list" icon="⚙"/>
          <MetricCard label="Effects delivered" value={simulationDelivered} hint="Outbox deliveries in observed traces" icon="⇢"/>
        </div>
        
        {/* Logs panel */}
        <SimulationLogsPanel 
          settings={settings} 
          scenario={simulationScenario} 
          running={simulationRunning}
          logs={simulationLogs}
          busy={simulationLogsBusy}
          error={simulationLogsError}
          onRefresh={loadSimulationLogs}
        />
        
        {/* Rule Chain Flow with Contracts */}
        {selectedSourceRules.length === 1 && (
          <RuleChainFlow 
            settings={settings} 
            scenario={selectedSourceRules[0].scenario} 
            events={simulationEvents} 
            running={simulationRunning}
          />
        )}
        {selectedSourceRules.length > 1 && (
          <div className="panel rule-chain-multi">
            <div className="panel-heading"><h3>Rule Chain Flow</h3><p>Select a single domain above to view the complete rule chain with input/output contracts.</p></div>
          </div>
        )}
        
        {selectedSourceRules.map(({ scenario, label, rule }) => <div className="entry-rule-picker" key={`pick-${scenario}`}><span>{label} source event type</span><code>{rule}</code></div>)}
        {selectedSourceRules.map(({ scenario, rule }) => <RuleFlowMap key={scenario} settings={settings} scenario={scenario} startRuleSet={rule} events={simulationEvents} running={simulationRunning}/>)}
        <section className="panel source-rules-panel">
          <div className="panel-heading"><div><h3>Source rule sets for this tenant</h3><p>Events are still published when a source rule is missing, but no rule execution can be shown for that event.</p></div><button className="text-button" onClick={() => void refresh()}>Refresh rules ↻</button></div>
          <div className="source-rule-list">{selectedSourceRules.map(({ label, rule }) => {
            const active = activeRules.some(item => (item.rule_set || item.rule_id || item.ruleSet) === rule);
            return <div className="source-rule" key={rule}><span className="rule-symbol">R</span><span><strong>{label}</strong><small>{rule}</small></span><Status value={active ? "Active" : "Missing"}/></div>;
          })}</div>
        </section>
        <section className="panel observed-flows-panel">
          <div className="panel-heading"><div><h3>Observed service-to-worker flows</h3><p>Each row is a real event sent to the API. Expand its trace to see which worker rule ran and where the outbox routed the result.</p></div><span className="tracked-count">{simulationEvents.length} tracked</span></div>
          {simulationEvents.length === 0
            ? <Empty title="No simulation events yet" detail="Choose a domain and start the simulation. This view will populate as the API accepts events and the worker records executions."/>
            : <div className="simulation-event-list">{simulationEvents.slice(0, 12).map(event => <details className="simulation-event" key={event.id} open={simulationEvents.indexOf(event) === 0}>
              <summary><span className={`scenario-badge scenario-${event.scenario}`}>{event.scenario === "food-delivery" ? "Delivery" : event.scenario}</span><span className="simulation-event-main"><strong>{event.summary}</strong><small>Source service → {event.type} · partition {event.partitionKey} · shard {event.shard}</small></span><Status value={event.trace?.status || "queued"}/><time>{new Date(event.sentAt).toLocaleTimeString()}</time></summary>
              <div className="trace-content"><div className="trace-legend"><span>Source event</span><span>→ actual worker execution</span><span>→ outbox route / next service</span></div>{event.trace
                ? <EnhancedTraceBranch node={event.trace}/>
                : <div className="trace-pending">Event accepted by the API. Waiting for the worker's inbox and execution record…</div>}</div>
            </details>)}</div>}
        </section>
        <p className="simulation-footnote">Outbox destinations are shown exactly as configured. Emitted child events are followed to their next worker/rule-set execution; external service commands appear with their configured destination.</p>
      </section>}
      {nav === "catalog" && <section className="page-content registry-page">
        <div className="page-intro page-intro-row"><div><h2>Domain catalog</h2><p>Contracts and rules for each example domain, as stored in the FlowRulz database for tenant {settings.tenant}.</p></div><button className="button" onClick={() => void loadCatalog()} disabled={catalogBusy !== ""}>{catalogBusy === "load" ? "Refreshing…" : "Refresh"}</button></div>
        {catalogMessage && <div className="notice" role="status">{catalogMessage}</div>}
        {Object.keys(DOMAIN_LABELS).map(domainKey => {
          const items = CATALOG.filter(item => item.domain === domainKey);
          const storedContractNames = new Set(storedContracts.map((c: any) => c.name));
          const storedRuleNames = new Set(storedRules.map((r: any) => r.rule_set));
          const isStored = (item: typeof items[number]) => item.kind === "contract" ? storedContractNames.has(item.name) : storedRuleNames.has(item.name);
          const bundledNumber = (item: typeof items[number], key: string) => item.source.match(new RegExp(`^${key}:\\s*["']?([\\w.]+)`, "m"))?.[1];
          const catalogState = (item: typeof items[number]) => {
            if (!isStored(item)) return "Not stored";
            if (item.kind === "contract") {
              const want = bundledNumber(item, "version"); const have = String(storedContracts.find((c: any) => c.name === item.name)?.version ?? "");
              return want && have && want !== have ? `Version ${have} stored, bundle ${want}` : `Stored · v${have}`;
            }
            const want = bundledNumber(item, "revision"); const have = String(storedRules.find((r: any) => r.rule_set === item.name)?.revision ?? "");
            return want && have && want !== have ? `Active revision ${have}, bundle ${want}` : `Active revision ${have}`;
          };
          const missing = items.filter(item => !isStored(item)).length;
          return <section className="panel" key={domainKey}>
            <div className="panel-heading"><div><h3>{DOMAIN_LABELS[domainKey]}</h3><p>{items.length - missing} of {items.length} definitions in the database</p></div><button className="button primary" onClick={() => void loadDomainIntoDatabase(domainKey)} disabled={catalogBusy !== ""}>{catalogBusy === domainKey ? "Saving…" : missing ? "Save to database" : "Save again"}</button></div>
            <div className="table-wrap"><table><thead><tr><th>Type</th><th>Name</th><th>Database</th><th></th></tr></thead><tbody>
              {items.map(item => { const stored = isStored(item); const detail = item.kind === "contract" ? storedContracts.find((c: any) => c.name === item.name) : storedRules.find((r: any) => r.rule_set === item.name); return <tr key={item.kind + item.name}><td>{item.kind}</td><td>{item.name}</td><td><Status value={catalogState(item)}/></td><td><button className="text-button" onClick={() => setCatalogView({ title: `${item.kind} ${item.name}`, body: stored ? JSON.stringify(detail, null, 2) : item.source })}>{stored ? "View stored" : "View source"}</button></td></tr>; })}
            </tbody></table></div>
          </section>;
        })}
        {catalogView && <section className="panel"><div className="panel-heading"><div><h3>{catalogView.title}</h3></div><button className="text-button" onClick={() => setCatalogView(null)}>Close</button></div><pre className="response-box">{catalogView.body}</pre></section>}
      </section>}
      {nav === "api" && <section className="page-content"><div className="page-intro"><h2>Explore the management API</h2><p>Send requests against the running FlowRulz API. Mutating requests use your configured tenant and token.</p></div><div className="api-layout"><section className="panel endpoint-panel"><div className="panel-heading"><div><h3>Known endpoints</h3><p>Routes registered by the current API</p></div></div>{ROUTES.map(route => <button className={apiPath === route.path ? "endpoint selected-endpoint" : "endpoint"} key={route.method+route.path} onClick={() => {setApiPath(route.path);setApiMethod(route.method);}}><span className={`method method-${route.method.toLowerCase()}`}>{route.method}</span><span><strong>{route.path}</strong><small>{route.description}</small></span></button>)}</section><section className="panel request-panel"><div className="panel-heading"><div><h3>Request builder</h3><p>Base URL: {normalizeBase(settings.apiBase)}</p></div></div><div className="form-row"><label>Method<select value={apiMethod} onChange={e=>setApiMethod(e.target.value)}>{["GET","POST","DELETE","PUT","PATCH"].map(m=><option key={m}>{m}</option>)}</select></label><label className="grow">Path<input value={apiPath} onChange={e=>setApiPath(e.target.value)} placeholder="/v1/rules/active"/></label></div><label>JSON body {apiMethod === "GET" || apiMethod === "DELETE" ? <span className="muted">(not sent for this method)</span> : null}<textarea className="code-editor request-editor" value={apiBody} onChange={e=>setApiBody(e.target.value)} spellCheck={false}/></label><div className="request-actions"><button className="button primary" onClick={() => void runApi()} disabled={apiBusy}>{apiBusy ? "Sending…" : "▶ Send request"}</button><span className="muted">X-Tenant-ID is included automatically</span></div><div className="response-heading"><h4>Response</h4>{apiResult && <button className="text-button" onClick={() => navigator.clipboard?.writeText(apiResult)}>Copy</button>}</div><pre className="response-box">{apiResult || "Send a request to inspect its response."}</pre></section></div></section>}
      {nav === "codegen" && <section className="page-content"><div className="page-intro"><h2>Contracts → generated artifacts</h2><p>Register a schema, select a stored version, and invoke the existing server-side generator.</p></div><div className="content-grid codegen-grid"><section className="panel"><div className="panel-heading"><div><h3>Register contract</h3><p>Requires contracts:admin outside local development</p></div></div><label>Contract name<input value={contractName} onChange={e=>setContractName(e.target.value)} placeholder="transaction.initiated"/></label><label>Version<input value={contractVersion} onChange={e=>setContractVersion(e.target.value)} placeholder="v1"/></label><label>Schema payload (JSON)<textarea className="code-editor schema-editor" value={contractSchema} onChange={e=>setContractSchema(e.target.value)} spellCheck={false}/></label><button className="button primary" disabled={codegenBusy} onClick={() => void createContract()}>{codegenBusy ? "Working…" : "＋ Register contract"}</button></section><section className="panel"><div className="panel-heading"><div><h3>Generate code</h3><p>Generated artifacts are written on the API server</p></div></div><label>Registered contract<select value={selectedContract} onChange={e=>setSelectedContract(e.target.value)}><option value="">Select a contract version…</option>{contracts.map((c,i)=>{const name=c.name || c.contract_name || "unnamed"; const version=c.version || "v1";return <option key={name+version+i} value={name+"@"+version}>{name} · {version}</option>;})}</select></label><label>Target language<select value={language} onChange={e=>setLanguage(e.target.value)}>{[["go","Go"],["java","Java"],["protobuf","Protocol Buffers"],["jsonschema","JSON Schema"]].map(([v,l])=><option key={v} value={v}>{l}</option>)}</select></label><div className="info-callout"><strong>Server-side output</strong><p>For safety, output stays inside the API server's managed temporary directory. This API returns generated file paths, not file contents or a download archive. To write typed code into an example, run <code>make generate-domain DOMAIN=banking</code> (or ecommerce, food-delivery); output lands in domain-builds/&lt;domain&gt;, which the example services import.</p></div><button className="button primary" disabled={codegenBusy || !selectedContract} onClick={() => void generate()}>{codegenBusy ? "Generating…" : "⚡ Generate artifacts"}</button>{generated && <div className="result-panel"><div className="result-heading"><strong>{generated.error ? "Operation failed" : generated.operation || "Generation result"}</strong><Status value={generated.error ? "Error" : "Complete"}/></div><pre className="response-box">{JSON.stringify(generated.error ? {error:generated.error} : generated, null, 2)}</pre></div>}</section></div></section>}
      {nav === "workers" && <section className="page-content"><div className="page-intro page-intro-row"><div><h2>Worker fleet & rule throughput</h2><p>Each worker is scraped independently. Rule counts are cumulative process counters, not in-flight rule executions.</p></div><button className="button secondary" onClick={() => setSettingsOpen(true)}>Configure workers</button></div>{workers.length === 0 ? <section className="panel"><Empty title="Worker telemetry is not configured" detail="Add each worker's private Prometheus /metrics endpoint in Settings. This page intentionally does not infer worker health from API liveness."/><button className="button primary centered-button" onClick={() => setSettingsOpen(true)}>Configure metrics URLs</button></section> : <><div className="metric-grid"><MetricCard label="Workers online" value={`${onlineWorkers}/${workers.length}`} hint="Successful metrics scrapes" icon="▦"/><MetricCard label="In-flight events" value={sumGauge(workers, w => w.inFlight)} hint={coverageHint(workers, w => w.inFlight, "Sum of current worker gauges")} icon="↻"/><MetricCard label="Active partition keys" value={sumGauge(workers, w => w.activeKeys)} hint={coverageHint(workers, w => w.activeKeys, "Process-local queue gauges")} icon="⌘"/><MetricCard label="Cumulative executions" value={executionTotal.toLocaleString()} hint="Summed execution counters" icon="↗"/></div><div className="worker-cards">{workers.map(worker=><section className="panel worker-card" key={worker.url}><div className="worker-card-header"><div className="worker-symbol large">▦</div><div className="grow"><h3>{worker.name}</h3><p>{worker.url}</p></div><Status value={worker.online ? "Online" : "Unreachable"}/></div>{worker.error ? <div className="alert error">{worker.error}</div> : <><div className="worker-kpis"><div><small>In flight</small><strong>{worker.inFlight ?? "—"}</strong></div><div><small>Active keys</small><strong>{worker.activeKeys ?? "—"}</strong></div><div><small>Owned shards</small><strong>{worker.shards ?? "—"}</strong></div></div><h4>Rule execution counters</h4>{worker.executions.length ? worker.executions.map(item=><div className="throughput-row" key={item.ruleSet}><span>{item.ruleSet}</span><div className="bar-track"><i style={{width:`${Math.max(2, item.count / Math.max(...worker.executions.map(x=>x.count),1)*100)}%`}}/></div><strong>{item.count.toLocaleString()}</strong></div>) : <p className="muted">No execution series exposed by this worker yet.</p>}</> }<div className="worker-footer">Last scrape: {new Date(worker.checkedAt).toLocaleTimeString()}</div></section>)}</div></>}</section>}
      {nav === "rules" && <RuleStudio settings={settings} activeRules={activeRules} contracts={contracts} onChanged={refresh}/>}
      {nav === "logs" && <section className="page-content"><div className="page-intro page-intro-row"><div><h2>Runtime logs</h2><p>Query recent structured logs from your configured Loki-compatible log backend.</p></div><button className="button primary" onClick={() => void loadLogs()} disabled={logsBusy}>{logsBusy ? "Querying…" : "↻ Query last hour"}</button></div>{logsError && <div className="alert info"><strong>Log source</strong><span>{logsError}</span><button onClick={() => setSettingsOpen(true)}>Configure</button></div>}<section className="panel"><div className="panel-heading"><div><h3>Log stream</h3><p>Most recent 200 entries from the last hour</p></div><span className="muted">{logs.length} entries</span></div>{logs.length === 0 ? <Empty title="No log entries loaded" detail="Set the Loki base URL and query in Settings, then query the stream. The application does not currently provide a log-tail HTTP endpoint."/> : <div className="logs-list">{logs.map((log,i)=><div className="log-row" key={i}><time>{log.timestamp}</time><span className="log-level">{String(log.line).match(/\b(ERROR|WARN|INFO|DEBUG)\b/)?.[1] || "LOG"}</span><code>{log.line}</code><button className="copy-log" onClick={()=>navigator.clipboard?.writeText(log.line)}>Copy</button></div>)}</div>}</section></section>}
      <footer className="footer"><span>FlowRulz Console</span><span>Data refreshes every 10 seconds · {lastRefresh ? `Updated ${lastRefresh.toLocaleTimeString()}` : "Waiting for first refresh"}</span></footer>
    </main>
    {settingsOpen && <div className="modal-backdrop" onMouseDown={e=>{if(e.target===e.currentTarget)setSettingsOpen(false);}}><section className="settings-modal" role="dialog" aria-modal="true" aria-label="Console settings" onKeyDown={e=>{if(e.key==="Escape")setSettingsOpen(false);}}><div className="modal-heading"><div><h2>Console settings</h2><p>Connection details are stored in this browser only.</p></div><button className="icon-button" onClick={() => setSettingsOpen(false)}>×</button></div><label>API base URL<input value={settings.apiBase} onChange={e=>setSettings({...settings,apiBase:e.target.value})} placeholder="http://localhost:8080"/></label><label>Tenant ID<input value={settings.tenant} onChange={e=>setSettings({...settings,tenant:e.target.value})} placeholder="development-tenant"/></label><label>Bearer token <span className="muted">(optional)</span><input type="password" value={settings.token} onChange={e=>setSettings({...settings,token:e.target.value})} autoComplete="off"/></label><label>Worker metrics URLs <span className="muted">(one per line or comma-separated)</span><textarea value={settings.workerMetrics} onChange={e=>setSettings({...settings,workerMetrics:e.target.value})} rows={3} placeholder="http://worker-1:9090/metrics"/></label><label>Loki base URL <span className="muted">(optional)</span><input value={settings.lokiBase} onChange={e=>setSettings({...settings,lokiBase:e.target.value})} placeholder="http://localhost:3100"/></label><label>Loki query<input value={settings.lokiQuery} onChange={e=>setSettings({...settings,lokiQuery:e.target.value})} placeholder='{service_name="flowrulz-worker"}'/></label><div className="info-callout"><strong>Security note</strong><p>Keep metrics and Loki private. Browser requests need correct CORS settings. Tokens are stored in localStorage; use a short-lived, least-privilege token and do not use a shared machine.</p></div><div className="modal-actions"><button className="button secondary" onClick={() => setSettings(DEFAULT_SETTINGS)}>Reset</button><button className="button primary" onClick={() => saveSettings(settings)}>Save settings</button></div></section></div>}
  </div>;
}
