import type { Promise } from "../types";

interface PromisesPanelProps {
  promises: Promise[];
}

function formatNodes(nodes: (string | number)[] | "ALL" | null | undefined): string {
  if (nodes === "ALL") return "ALL";
  if (nodes === null || nodes === undefined) return "-";
  return nodes.join(", ");
}

function formatConstraint(promise: Promise): string {
  if (promise.constraintOp && promise.constraintN !== undefined) {
    return `${promise.constraintOp} ${promise.constraintN}`;
  }
  return "";
}

function getPromiseDescription(promise: Promise): string {
  if (promise.constraintType === "RunPromise") {
    const constraint = formatConstraint(promise);
    return `Max ${promise.constraintN} concurrent`;
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
            <li key={idx} className={`promise-item ${promise.constraintType.toLowerCase()}`}>
              <span className="promise-type">{promise.constraintType}</span>
              <div className="promise-detail">
                <span className="promise-from">
                  {promise.constraintType === "RunPromise" ? "Tasks" : "From"}:{" "}
                  <strong>{formatNodes(promise.fromNodes)}</strong>
                </span>
                {promise.constraintType !== "RunPromise" && (
                  <span className="promise-to">
                    To: <strong>{formatNodes(promise.toNodes)}</strong>
                  </span>
                )}
                {formatConstraint(promise) && (
                  <span className="promise-constraint">
                    {promise.constraintType === "RunPromise"
                      ? `Max concurrent: ${promise.constraintN}`
                      : `Constraint: ${formatConstraint(promise)}`
                    }
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
