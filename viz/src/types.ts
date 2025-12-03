// Event types matching src/ork/event.py

export interface FromEdgeModel {
  from_node: number;
  cond: number | null;
}

export interface MessageEvent {
  event_type: "message";
  action: string;
  args: unknown[];
}

export interface CreateNodeEvent {
  event_type: "create_node";
  node_id: number;
  name: string;
  deps: FromEdgeModel[];
}

export interface MarkStateTransition {
  event_type: "mark_state_transition";
  node_id: number;
  new_state: "conditioned" | "pending" | "running" | "completed" | "failed";
  result?: unknown;
  error_msg?: string;
}

export interface CreatePromiseEvent {
  event_type: "create_promise";
  constraint_type: "EdgePromise" | "NodePromise";
  from_nodes: (string | number)[] | "ALL";
  to_nodes: (string | number)[] | "ALL";
  constraint_op?: "==" | "<=" | "<" | ">=" | ">";
  constraint_n?: number;
}

export interface ConditionedTask {
  condition: Record<string, unknown>;
  task_id: string;
}

export interface CreateConditionEvent {
  event_type: "create_condition";
  case_groups: ConditionedTask[];
}

export interface ShutdownEvent {
  event_type: "shutdown";
  successful: boolean;
}

export type EventPayload =
  | MessageEvent
  | CreateNodeEvent
  | MarkStateTransition
  | CreatePromiseEvent
  | CreateConditionEvent
  | ShutdownEvent;

export interface Event {
  timestamp: number;
  event: EventPayload;
}

// Visualization state types
export type NodeState = "created" | "conditioned" | "pending" | "running" | "completed" | "failed";

export interface GraphNode {
  id: number;
  name: string;
  state: NodeState;
  result?: unknown;
  errorMsg?: string;
  committed: boolean;
}

export interface GraphEdge {
  from: number;
  to: number;
  condition?: number; // node id that acts as condition
  negated?: boolean; // true if this is a negated condition (if NOT)
}

export interface Promise {
  constraintType: "EdgePromise" | "NodePromise";
  fromNodes: (string | number)[] | "ALL";
  toNodes: (string | number)[] | "ALL";
  constraintOp?: "==" | "<=" | "<" | ">=" | ">";
  constraintN?: number;
}

export interface Condition {
  caseGroups: ConditionedTask[];
}

export interface WorkflowState {
  nodes: Map<number, GraphNode>;
  edges: GraphEdge[];
  promises: Promise[];
  conditions: Condition[];
  shutdown?: {
    successful: boolean;
  };
}
