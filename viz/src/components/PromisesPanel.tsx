import type { Promise } from "../types";

interface PromisesPanelProps {
  promises: Promise[];
}

function formatNodes(nodes: (string | number)[] | "ALL"): string {
  if (nodes === "ALL") return "ALL";
  return nodes.join(", ");
}

function formatConstraint(promise: Promise): string {
  if (promise.constraintOp && promise.constraintN !== undefined) {
    return `${promise.constraintOp} ${promise.constraintN}`;
  }
  return "";
}

export function PromisesPanel({ promises }: PromisesPanelProps) {
  return (
    <div className="promises-panel">
      <h3>Promises</h3>
      {promises.length === 0 ? (
        <div className="empty-panel">No promises defined</div>
      ) : (
        <ul className="promise-list">
          {promises.map((promise, idx) => (
            <li key={idx} className="promise-item">
              <span className="promise-type">{promise.constraintType}</span>
              <div className="promise-detail">
                <span className="promise-from">
                  From: <strong>{formatNodes(promise.fromNodes)}</strong>
                </span>
                <span className="promise-to">
                  To: <strong>{formatNodes(promise.toNodes)}</strong>
                </span>
                {formatConstraint(promise) && (
                  <span className="promise-constraint">
                    Constraint: <strong>{formatConstraint(promise)}</strong>
                  </span>
                )}
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
