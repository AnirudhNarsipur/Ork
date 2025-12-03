import { useMemo } from "react";
import type { WorkflowState, GraphNode, GraphEdge, NodeState } from "../types";

interface TaskGraphProps {
  state: WorkflowState;
}

const NODE_WIDTH = 120;
const NODE_HEIGHT = 50;
const LEVEL_GAP_X = 180;
const NODE_GAP_Y = 80;
const PADDING = 40;

function getNodeColor(state: NodeState): string {
  switch (state) {
    case "created":
      return "#e0e0e0";
    case "conditioned":
      return "#fff3cd";
    case "pending":
      return "#cce5ff";
    case "running":
      return "#d4edda";
    case "completed":
      return "#28a745";
    case "failed":
      return "#dc3545";
    default:
      return "#e0e0e0";
  }
}

function getTextColor(state: NodeState): string {
  if (state === "completed" || state === "failed") {
    return "#ffffff";
  }
  return "#333333";
}

interface NodePosition {
  x: number;
  y: number;
}

function calculateLayout(
  nodes: Map<number, GraphNode>,
  edges: GraphEdge[]
): Map<number, NodePosition> {
  const positions = new Map<number, NodePosition>();
  const nodeArray = Array.from(nodes.values()).filter(n => n.committed);

  if (nodeArray.length === 0) return positions;

  // Build adjacency for topological sort
  const inDegree = new Map<number, number>();
  const outEdges = new Map<number, number[]>();

  for (const node of nodeArray) {
    inDegree.set(node.id, 0);
    outEdges.set(node.id, []);
  }

  for (const edge of edges) {
    if (nodes.get(edge.from)?.committed && nodes.get(edge.to)?.committed) {
      inDegree.set(edge.to, (inDegree.get(edge.to) || 0) + 1);
      outEdges.get(edge.from)?.push(edge.to);
    }
  }

  // Assign levels using BFS-based topological approach
  const levels = new Map<number, number>();
  const queue: number[] = [];

  for (const [id, deg] of inDegree) {
    if (deg === 0) {
      queue.push(id);
      levels.set(id, 0);
    }
  }

  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentLevel = levels.get(current)!;

    for (const next of outEdges.get(current) || []) {
      const newDegree = (inDegree.get(next) || 1) - 1;
      inDegree.set(next, newDegree);

      const existingLevel = levels.get(next);
      if (existingLevel === undefined || currentLevel + 1 > existingLevel) {
        levels.set(next, currentLevel + 1);
      }

      if (newDegree === 0) {
        queue.push(next);
      }
    }
  }

  // Handle any unvisited nodes (disconnected)
  for (const node of nodeArray) {
    if (!levels.has(node.id)) {
      levels.set(node.id, 0);
    }
  }

  // Group by level
  const levelGroups = new Map<number, number[]>();
  for (const [id, level] of levels) {
    if (!levelGroups.has(level)) {
      levelGroups.set(level, []);
    }
    levelGroups.get(level)!.push(id);
  }

  // Position nodes
  for (const [level, ids] of levelGroups) {
    ids.sort((a, b) => a - b);
    for (let i = 0; i < ids.length; i++) {
      positions.set(ids[i], {
        x: PADDING + level * LEVEL_GAP_X,
        y: PADDING + i * NODE_GAP_Y,
      });
    }
  }

  return positions;
}

export function TaskGraph({ state }: TaskGraphProps) {
  const { positions, viewBox } = useMemo(() => {
    const pos = calculateLayout(state.nodes, state.edges);
    let minX = PADDING;
    let minY = PADDING;
    let maxX = PADDING + NODE_WIDTH;
    let maxY = PADDING + NODE_HEIGHT;

    for (const p of pos.values()) {
      minX = Math.min(minX, p.x);
      minY = Math.min(minY, p.y);
      maxX = Math.max(maxX, p.x + NODE_WIDTH);
      maxY = Math.max(maxY, p.y + NODE_HEIGHT);
    }

    // Add padding around the content
    const padded = {
      x: minX - PADDING,
      y: minY - PADDING,
      width: maxX - minX + PADDING * 2,
      height: maxY - minY + PADDING * 2,
    };

    return {
      positions: pos,
      viewBox: `${padded.x} ${padded.y} ${padded.width} ${padded.height}`
    };
  }, [state.nodes, state.edges]);

  const committedNodes = Array.from(state.nodes.values()).filter(n => n.committed);
  const committedEdges = state.edges.filter(
    e => state.nodes.get(e.from)?.committed && state.nodes.get(e.to)?.committed
  );

  return (
    <div className="task-graph">
      <h3>Task Graph</h3>
      {committedNodes.length === 0 ? (
        <div className="empty-graph">No committed tasks yet</div>
      ) : (
        <svg viewBox={viewBox} preserveAspectRatio="xMidYMid meet">
          <defs>
            <marker
              id="arrowhead"
              markerWidth="10"
              markerHeight="7"
              refX="9"
              refY="3.5"
              orient="auto"
            >
              <polygon points="0 0, 10 3.5, 0 7" fill="#666" />
            </marker>
          </defs>

          {/* Edges */}
          {committedEdges.map((edge, idx) => {
            const fromPos = positions.get(edge.from);
            const toPos = positions.get(edge.to);
            if (!fromPos || !toPos) return null;

            const x1 = fromPos.x + NODE_WIDTH;
            const y1 = fromPos.y + NODE_HEIGHT / 2;
            const x2 = toPos.x;
            const y2 = toPos.y + NODE_HEIGHT / 2;

            const isConditional = edge.condition !== undefined;

            // Determine edge color based on condition resolution
            let edgeColor = "#666"; // default for non-conditional
            if (isConditional) {
              const conditionNode = state.nodes.get(edge.condition!);
              if (conditionNode?.state === "completed") {
                // Condition is resolved - check if this edge's condition is satisfied
                const conditionResult = Boolean(conditionNode.result);
                const edgeSatisfied = edge.negated ? !conditionResult : conditionResult;
                edgeColor = edgeSatisfied ? "#28a745" : "#dc3545"; // green or red
              } else {
                edgeColor = "#f0ad4e"; // orange - not yet resolved
              }
            }

            return (
              <g key={`edge-${idx}`}>
                <line
                  x1={x1}
                  y1={y1}
                  x2={x2}
                  y2={y2}
                  stroke={edgeColor}
                  strokeWidth={2}
                  strokeDasharray={isConditional ? "5,5" : undefined}
                  markerEnd="url(#arrowhead)"
                />
                {isConditional && (
                  <text
                    x={(x1 + x2) / 2}
                    y={(y1 + y2) / 2 - 8}
                    fontSize={10}
                    fill={edgeColor}
                    textAnchor="middle"
                  >
                    {edge.negated ? `if not ${edge.condition}` : `if ${edge.condition}`}
                  </text>
                )}
              </g>
            );
          })}

          {/* Nodes */}
          {committedNodes.map(node => {
            const pos = positions.get(node.id);
            if (!pos) return null;

            return (
              <g key={`node-${node.id}`}>
                <rect
                  x={pos.x}
                  y={pos.y}
                  width={NODE_WIDTH}
                  height={NODE_HEIGHT}
                  rx={8}
                  fill={getNodeColor(node.state)}
                  stroke="#333"
                  strokeWidth={1}
                />
                <text
                  x={pos.x + NODE_WIDTH / 2}
                  y={pos.y + 16}
                  textAnchor="middle"
                  fontSize={12}
                  fontWeight="bold"
                  fill={getTextColor(node.state)}
                >
                  {node.name || `Task ${node.id}`}
                </text>
                <text
                  x={pos.x + NODE_WIDTH / 2}
                  y={pos.y + 30}
                  textAnchor="middle"
                  fontSize={9}
                  fill={getTextColor(node.state)}
                  opacity={0.7}
                >
                  id: {node.id}
                </text>
                <text
                  x={pos.x + NODE_WIDTH / 2}
                  y={pos.y + 43}
                  textAnchor="middle"
                  fontSize={10}
                  fill={getTextColor(node.state)}
                >
                  {node.state}
                </text>
              </g>
            );
          })}
        </svg>
      )}
    </div>
  );
}
