import type { GraphNode, NodeState } from "../types";

interface ScratchPanelProps {
  nodes: GraphNode[];
}

function getStateColor(state: NodeState): string {
  switch (state) {
    case "created":
      return "#e0e0e0";
    case "conditioned":
      return "#fff3cd";
    default:
      return "#e0e0e0";
  }
}

export function ScratchPanel({ nodes }: ScratchPanelProps) {
  const uncommittedNodes = nodes.filter(n => !n.committed);

  return (
    <div className="scratch-panel">
      <h3>Scratch (Uncommitted)</h3>
      {uncommittedNodes.length === 0 ? (
        <div className="empty-panel">No uncommitted tasks</div>
      ) : (
        <ul className="scratch-list">
          {uncommittedNodes.map(node => (
            <li
              key={node.id}
              className="scratch-item"
              style={{ backgroundColor: getStateColor(node.state) }}
            >
              <span className="scratch-name">{node.name || `Task ${node.id}`}</span>
              <span className="scratch-id">ID: {node.id}</span>
              <span className="scratch-state">{node.state}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
