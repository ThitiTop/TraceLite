import { useEffect, useMemo, useState } from "react";
import DependencyGraph, { type GraphEdge } from "./components/DependencyGraph";

type TraceItem = {
  trace_id: string;
  root_service: string;
  duration_ms: number;
  error_count: number;
  critical_path_ms: number;
  start_ts: string;
};

type HostItem = {
  host: string;
  logs: number | string;
  errors: number | string;
  active_services: number | string;
  last_seen: string;
  error_rate: number;
};

type CompareMetric = {
  version: string;
  spans: number | string;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
  error_rate: number;
};

type OperationDiff = {
  operation: string;
  base_p95_ms: number;
  cand_p95_ms: number;
  delta_p95_ms: number;
  base_calls: number | string;
  cand_calls: number | string;
};

type RootCause = {
  service: string;
  score: number;
  latency_delta_pct: number;
  error_delta_pct: number;
  call_delta_pct: number;
  blocking_ratio: number;
  reason: string;
};

type AnomalyBadge = {
  level: string;
  title: string;
  message: string;
  deviation_score: number;
};

type WaterfallSpan = {
  span_id: string;
  parent_span_id: string;
  service: string;
  operation: string;
  start_ts: string;
  end_ts: string;
  duration_ms: number;
  self_time_ms: number;
  wait_ms: number;
  blocking_ratio: number;
  depth: number;
  is_critical: boolean;
  is_error: boolean;
  left_pct: number;
  width_pct: number;
  explanation: string;
};

type SlowSpot = {
  span_id: string;
  service: string;
  duration_ms: number;
  self_time_ms: number;
  wait_ms: number;
  blocking_ratio: number;
  score: number;
  explanation: string;
};

type ErrorChain = {
  error_span_id: string;
  path: string[];
};

type DrilldownPayload = {
  trace: TraceItem | null;
  waterfall: WaterfallSpan[];
  critical_path: string[];
  error_chains: ErrorChain[];
  slow_spots: SlowSpot[];
};

type DependencyDiffEdge = {
  caller_service: string;
  callee_service: string;
  status: string;
  base_calls: number;
  cand_calls: number;
  call_diff_pct: number;
  p95_diff_ms: number;
  error_rate_diff: number;
  is_new_edge: boolean;
};

type ErrorPanel = {
  service_breakdown: Array<{ service: string; errors: number | string; calls: number | string; error_rate: number }>;
  top_operations: Array<{
    service: string;
    operation: string;
    errors: number | string;
    calls: number | string;
    error_rate: number;
  }>;
  new_errors: Array<{ service: string; operation: string; base_errors: number | string; cand_errors: number | string }>;
};

const apiBase =
  import.meta.env.VITE_API_BASE ?? `${window.location.protocol}//${window.location.hostname}:8080`;

const num = (v: unknown): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

function App() {
  const [env, setEnv] = useState("dev");
  const [service, setService] = useState("api");
  const [baseVersion, setBaseVersion] = useState("1.0.0");
  const [candVersion, setCandVersion] = useState("1.1.0");
  const [lookbackHours, setLookbackHours] = useState(168);
  const [loading, setLoading] = useState(false);

  const [traces, setTraces] = useState<TraceItem[]>([]);
  const [selectedTraceId, setSelectedTraceId] = useState("");
  const [drilldown, setDrilldown] = useState<DrilldownPayload | null>(null);

  const [hosts, setHosts] = useState<HostItem[]>([]);
  const [edges, setEdges] = useState<GraphEdge[]>([]);
  const [metrics, setMetrics] = useState<CompareMetric[]>([]);
  const [operationDiff, setOperationDiff] = useState<OperationDiff[]>([]);
  const [rootCauses, setRootCauses] = useState<RootCause[]>([]);
  const [anomalies, setAnomalies] = useState<AnomalyBadge[]>([]);

  const [dependencyDiff, setDependencyDiff] = useState<DependencyDiffEdge[]>([]);
  const [errorPanel, setErrorPanel] = useState<ErrorPanel | null>(null);

  const params = useMemo(() => {
    const to = new Date();
    const from = new Date(to.getTime() - lookbackHours * 60 * 60 * 1000);
    return new URLSearchParams({
      from: from.toISOString(),
      to: to.toISOString(),
      env
    });
  }, [env, lookbackHours]);

  const fetchDrilldown = async (traceId: string) => {
    if (!traceId) {
      setDrilldown(null);
      return;
    }
    const res = await fetch(`${apiBase}/v1/traces/${traceId}/waterfall`);
    const payload = await res.json();
    setDrilldown(payload as DrilldownPayload);
  };

  const refresh = async () => {
    setLoading(true);
    try {
      const q = params.toString();
      const [tracesRes, hostRes, depRes, compareRes, diffRes, errorRes] = await Promise.all([
        fetch(`${apiBase}/v1/traces?${q}&limit=30&service=${encodeURIComponent(service)}`),
        fetch(`${apiBase}/v1/hosts?${q}`),
        fetch(`${apiBase}/v1/dependency?${q}`),
        fetch(
          `${apiBase}/v1/compare?${q}&service=${encodeURIComponent(service)}&base=${encodeURIComponent(baseVersion)}&cand=${encodeURIComponent(candVersion)}`
        ),
        fetch(
          `${apiBase}/v1/dependency/diff?${q}&service=${encodeURIComponent(service)}&base=${encodeURIComponent(baseVersion)}&cand=${encodeURIComponent(candVersion)}`
        ),
        fetch(
          `${apiBase}/v1/errors?${q}&service=${encodeURIComponent(service)}&base=${encodeURIComponent(baseVersion)}&cand=${encodeURIComponent(candVersion)}`
        )
      ]);

      const tracesData = await tracesRes.json();
      const hostData = await hostRes.json();
      const depData = await depRes.json();
      const compareData = await compareRes.json();
      const diffData = await diffRes.json();
      const errData = await errorRes.json();

      const traceList = (tracesData.data ?? []) as TraceItem[];
      setTraces(traceList);
      setHosts((hostData.hosts ?? []) as HostItem[]);
      setEdges((depData.edges ?? []) as GraphEdge[]);
      setMetrics((compareData.metrics ?? []) as CompareMetric[]);
      setOperationDiff((compareData.operation_diff ?? []) as OperationDiff[]);
      setRootCauses((compareData.root_causes ?? []) as RootCause[]);
      setAnomalies((compareData.anomalies ?? []) as AnomalyBadge[]);
      setDependencyDiff((diffData.edges ?? []) as DependencyDiffEdge[]);
      setErrorPanel((errData ?? null) as ErrorPanel | null);

      const preferred =
        selectedTraceId && traceList.some((t) => t.trace_id === selectedTraceId)
          ? selectedTraceId
          : traceList[0]?.trace_id ?? "";
      setSelectedTraceId(preferred);
      if (preferred) {
        await fetchDrilldown(preferred);
      } else {
        setDrilldown(null);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
  }, [params.toString(), service, baseVersion, candVersion]);

  return (
    <div className="page">
      <header className="topbar">
        <h1>trace-lite</h1>
        <div className="controls">
          <input value={env} onChange={(e) => setEnv(e.target.value)} placeholder="env" />
          <input value={service} onChange={(e) => setService(e.target.value)} placeholder="service" />
          <input value={baseVersion} onChange={(e) => setBaseVersion(e.target.value)} placeholder="base" />
          <input value={candVersion} onChange={(e) => setCandVersion(e.target.value)} placeholder="candidate" />
          <select value={lookbackHours} onChange={(e) => setLookbackHours(Number(e.target.value))}>
            <option value={1}>Last 1h</option>
            <option value={6}>Last 6h</option>
            <option value={24}>Last 24h</option>
            <option value={168}>Last 7d</option>
          </select>
          <button disabled={loading} onClick={() => void refresh()}>
            {loading ? "Loading..." : "Refresh"}
          </button>
        </div>
      </header>

      <section className="panel-grid">
        <article className="panel">
          <h2>Trace Explorer</h2>
          <table>
            <thead>
              <tr>
                <th>Trace</th>
                <th>Root</th>
                <th>Duration</th>
                <th>Critical</th>
                <th>Errors</th>
              </tr>
            </thead>
            <tbody>
              {traces.map((t) => (
                <tr
                  key={t.trace_id}
                  className={selectedTraceId === t.trace_id ? "row-active" : ""}
                  onClick={() => {
                    setSelectedTraceId(t.trace_id);
                    void fetchDrilldown(t.trace_id);
                  }}
                >
                  <td>{t.trace_id}</td>
                  <td>{t.root_service}</td>
                  <td>{num(t.duration_ms)} ms</td>
                  <td>{num(t.critical_path_ms)} ms</td>
                  <td>{num(t.error_count)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="panel large">
          <h2>Trace Drill-Down Waterfall</h2>
          {drilldown ? (
            <>
              <div className="drill-meta">
                <span>Trace: {drilldown.trace?.trace_id}</span>
                <span>Critical Path: {drilldown.critical_path?.join(" -> ") || "-"}</span>
              </div>
              <div className="waterfall-wrap">
                {drilldown.waterfall?.map((s) => (
                  <div key={s.span_id} className={`wf-row ${s.is_critical ? "wf-critical" : ""} ${s.is_error ? "wf-error" : ""}`}>
                    <div className="wf-label" style={{ paddingLeft: `${num(s.depth) * 14}px` }}>
                      <strong>{s.service}</strong> <span>{s.span_id}</span>
                      <small>{num(s.duration_ms)}ms (self {num(s.self_time_ms)} / wait {num(s.wait_ms)})</small>
                    </div>
                    <div className="wf-timeline">
                      <div className="wf-bar" style={{ left: `${num(s.left_pct)}%`, width: `${num(s.width_pct)}%` }} />
                    </div>
                    <div className="wf-explain">{s.explanation}</div>
                  </div>
                ))}
              </div>
              <div className="chip-row">
                <div className="chip-block">
                  <h3>Slow Spots</h3>
                  {(drilldown.slow_spots ?? []).slice(0, 5).map((s) => (
                    <div key={s.span_id} className="chip-item">
                      {s.service}#{s.span_id} score {num(s.score).toFixed(2)}
                    </div>
                  ))}
                </div>
                <div className="chip-block">
                  <h3>Error Chains</h3>
                  {(drilldown.error_chains ?? []).length === 0 && <div className="chip-item">No error chain</div>}
                  {(drilldown.error_chains ?? []).map((c) => (
                    <div key={c.error_span_id} className="chip-item">
                      {c.path.join(" -> ")}
                    </div>
                  ))}
                </div>
              </div>
            </>
          ) : (
            <p>No trace selected.</p>
          )}
        </article>

        <article className="panel">
          <h2>Dependency Graph</h2>
          <DependencyGraph edges={edges} />
        </article>

        <article className="panel">
          <h2>Root Cause Ranking</h2>
          <div className="badge-row">
            {anomalies.map((a, idx) => (
              <span key={`${a.title}-${idx}`} className={`badge badge-${a.level}`}>
                {a.title}: {a.message}
              </span>
            ))}
          </div>
          <table>
            <thead>
              <tr>
                <th>Service</th>
                <th>Score</th>
                <th>Latency%</th>
                <th>Error%</th>
                <th>Calls%</th>
              </tr>
            </thead>
            <tbody>
              {rootCauses.map((r) => (
                <tr key={r.service}>
                  <td>{r.service}</td>
                  <td>{num(r.score).toFixed(2)}</td>
                  <td>{num(r.latency_delta_pct).toFixed(1)}</td>
                  <td>{num(r.error_delta_pct).toFixed(1)}</td>
                  <td>{num(r.call_delta_pct).toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="panel">
          <h2>Host View</h2>
          <table>
            <thead>
              <tr>
                <th>Host</th>
                <th>Logs</th>
                <th>Error %</th>
                <th>Active Services</th>
                <th>Last Seen</th>
              </tr>
            </thead>
            <tbody>
              {hosts.map((h) => (
                <tr key={h.host}>
                  <td>{h.host}</td>
                  <td>{num(h.logs)}</td>
                  <td>{(num(h.error_rate) * 100).toFixed(2)}</td>
                  <td>{num(h.active_services)}</td>
                  <td>{h.last_seen}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="panel">
          <h2>Version Comparison</h2>
          <div className="compare-cards">
            {metrics.map((m) => (
              <div key={m.version} className="card">
                <h3>{m.version}</h3>
                <p>Spans: {num(m.spans)}</p>
                <p>P95: {num(m.p95_ms)} ms</p>
                <p>P99: {num(m.p99_ms)} ms</p>
                <p>Error: {(num(m.error_rate) * 100).toFixed(2)}%</p>
              </div>
            ))}
          </div>
          <table>
            <thead>
              <tr>
                <th>Operation</th>
                <th>Base p95</th>
                <th>Cand p95</th>
                <th>Delta</th>
              </tr>
            </thead>
            <tbody>
              {operationDiff.map((d) => (
                <tr key={d.operation}>
                  <td>{d.operation}</td>
                  <td>{num(d.base_p95_ms)}</td>
                  <td>{num(d.cand_p95_ms)}</td>
                  <td className={num(d.delta_p95_ms) > 0 ? "bad" : "good"}>{num(d.delta_p95_ms)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="panel">
          <h2>Structural Dependency Diff</h2>
          <table>
            <thead>
              <tr>
                <th>Edge</th>
                <th>Status</th>
                <th>Call Diff%</th>
                <th>P95 Diff</th>
                <th>Error Diff</th>
              </tr>
            </thead>
            <tbody>
              {dependencyDiff.slice(0, 20).map((e, idx) => (
                <tr key={`${e.caller_service}-${e.callee_service}-${idx}`}>
                  <td>
                    {e.caller_service} -&gt; {e.callee_service}
                  </td>
                  <td className={e.status === "new" ? "bad" : e.status === "removed" ? "warn" : ""}>{e.status}</td>
                  <td>{num(e.call_diff_pct).toFixed(1)}</td>
                  <td>{num(e.p95_diff_ms).toFixed(1)} ms</td>
                  <td>{(num(e.error_rate_diff) * 100).toFixed(2)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="panel">
          <h2>Error Analysis</h2>
          <h3>By Service</h3>
          <table>
            <thead>
              <tr>
                <th>Service</th>
                <th>Errors</th>
                <th>Calls</th>
                <th>Error %</th>
              </tr>
            </thead>
            <tbody>
              {(errorPanel?.service_breakdown ?? []).map((r) => (
                <tr key={r.service}>
                  <td>{r.service}</td>
                  <td>{num(r.errors)}</td>
                  <td>{num(r.calls)}</td>
                  <td>{(num(r.error_rate) * 100).toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <h3>Top Failing Operations</h3>
          <table>
            <thead>
              <tr>
                <th>Service</th>
                <th>Operation</th>
                <th>Errors</th>
              </tr>
            </thead>
            <tbody>
              {(errorPanel?.top_operations ?? []).slice(0, 8).map((r, idx) => (
                <tr key={`${r.service}-${r.operation}-${idx}`}>
                  <td>{r.service}</td>
                  <td>{r.operation}</td>
                  <td>{num(r.errors)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>
      </section>
    </div>
  );
}

export default App;
