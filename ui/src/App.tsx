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
  logs: number;
  errors: number;
  active_services: number;
  last_seen: string;
  error_rate: number;
};

type CompareMetric = {
  version: string;
  spans: number;
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
  base_calls: number;
  cand_calls: number;
};

const apiBase =
  import.meta.env.VITE_API_BASE ?? `${window.location.protocol}//${window.location.hostname}:8080`;

function App() {
  const [env, setEnv] = useState("dev");
  const [service, setService] = useState("api");
  const [baseVersion, setBaseVersion] = useState("1.0.0");
  const [candVersion, setCandVersion] = useState("1.1.0");
  const [lookbackHours, setLookbackHours] = useState(24);
  const [loading, setLoading] = useState(false);

  const [traces, setTraces] = useState<TraceItem[]>([]);
  const [hosts, setHosts] = useState<HostItem[]>([]);
  const [edges, setEdges] = useState<GraphEdge[]>([]);
  const [metrics, setMetrics] = useState<CompareMetric[]>([]);
  const [operationDiff, setOperationDiff] = useState<OperationDiff[]>([]);

  const params = useMemo(() => {
    const to = new Date();
    const from = new Date(to.getTime() - lookbackHours * 60 * 60 * 1000);
    return new URLSearchParams({
      from: from.toISOString(),
      to: to.toISOString(),
      env
    });
  }, [env, lookbackHours]);

  const refresh = async () => {
    setLoading(true);
    try {
      const [tracesRes, hostRes, depRes, compareRes] = await Promise.all([
        fetch(`${apiBase}/v1/traces?${params.toString()}&limit=30&service=${service}`),
        fetch(`${apiBase}/v1/hosts?${params.toString()}`),
        fetch(`${apiBase}/v1/dependency?${params.toString()}`),
        fetch(
          `${apiBase}/v1/compare?${params.toString()}&service=${service}&base=${baseVersion}&cand=${candVersion}`
        )
      ]);

      const tracesData = await tracesRes.json();
      const hostData = await hostRes.json();
      const depData = await depRes.json();
      const compareData = await compareRes.json();

      setTraces((tracesData.data ?? []) as TraceItem[]);
      setHosts((hostData.hosts ?? []) as HostItem[]);
      setEdges((depData.edges ?? []) as GraphEdge[]);
      setMetrics((compareData.metrics ?? []) as CompareMetric[]);
      setOperationDiff((compareData.operation_diff ?? []) as OperationDiff[]);
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
                <tr key={t.trace_id}>
                  <td>{t.trace_id}</td>
                  <td>{t.root_service}</td>
                  <td>{t.duration_ms} ms</td>
                  <td>{t.critical_path_ms} ms</td>
                  <td>{t.error_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </article>

        <article className="panel large">
          <h2>Dependency Graph</h2>
          <DependencyGraph edges={edges} />
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
                  <td>{h.logs}</td>
                  <td>{(h.error_rate * 100).toFixed(2)}</td>
                  <td>{h.active_services}</td>
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
                <p>Spans: {m.spans}</p>
                <p>P95: {m.p95_ms} ms</p>
                <p>P99: {m.p99_ms} ms</p>
                <p>Error: {(m.error_rate * 100).toFixed(2)}%</p>
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
                  <td>{d.base_p95_ms}</td>
                  <td>{d.cand_p95_ms}</td>
                  <td className={d.delta_p95_ms > 0 ? "bad" : "good"}>{d.delta_p95_ms}</td>
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
