import { useMemo } from "react";
import ReactFlow, { Background, Controls, type Edge, type Node } from "reactflow";
import "reactflow/dist/style.css";

export type GraphEdge = {
  caller_service: string;
  callee_service: string;
  calls?: number;
  p95_ms?: number;
  avg_latency_ms?: number;
  error_calls?: number;
  error_rate?: number;
  status?: string;
  call_diff_pct?: number;
  is_new_edge?: boolean;
};

type Props = {
  edges: GraphEdge[];
};

function DependencyGraph({ edges }: Props) {
  const { nodes, flowEdges } = useMemo(() => {
    const services = new Set<string>();
    edges.forEach((e) => {
      services.add(e.caller_service);
      services.add(e.callee_service);
    });

    const arr = Array.from(services);
    const nodes: Node[] = arr.map((name, idx) => ({
      id: name,
      position: { x: (idx % 4) * 240, y: Math.floor(idx / 4) * 120 },
      data: { label: name },
      style: {
        border: "1px solid #22435f",
        borderRadius: 12,
        padding: 8,
        background: "#f1f7ff",
        color: "#0c1f33",
        fontWeight: 700
      }
    }));

    const flowEdges: Edge[] = edges.map((e, idx) => ({
      id: `${e.caller_service}-${e.callee_service}-${idx}`,
      source: e.caller_service,
      target: e.callee_service,
      label: `${Math.round(e.calls ?? 0)} calls | p95 ${Math.round(e.p95_ms ?? 0)}ms | err ${Math.round((e.error_rate ?? 0) * 100)}%`,
      animated: (e.p95_ms ?? 0) > 500 || (e.call_diff_pct ?? 0) > 100,
      style: {
        stroke: e.is_new_edge || e.status === "new" ? "#cf1322" : (e.error_rate ?? 0) > 0.1 ? "#d64545" : "#205493",
        strokeDasharray: e.status === "removed" ? "4 3" : undefined,
        strokeWidth: Math.min(8, Math.max(1, (e.calls ?? 0) / 100))
      }
    }));

    return { nodes, flowEdges };
  }, [edges]);

  return (
    <div style={{ width: "100%", height: 380 }}>
      <ReactFlow nodes={nodes} edges={flowEdges} fitView>
        <Background color="#d6e5f4" />
        <Controls />
      </ReactFlow>
    </div>
  );
}

export default DependencyGraph;
