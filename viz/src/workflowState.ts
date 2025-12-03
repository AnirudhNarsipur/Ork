import type {
  Event,
  EventPayload,
  GraphNode,
  WorkflowState,
  CreateNodeEvent,
  MarkStateTransition,
  CreatePromiseEvent,
  CreateConditionEvent,
} from "./types";

export function createEmptyState(): WorkflowState {
  return {
    nodes: new Map(),
    edges: [],
    promises: [],
    conditions: [],
  };
}

export function cloneState(state: WorkflowState): WorkflowState {
  return {
    nodes: new Map(state.nodes),
    edges: [...state.edges],
    promises: [...state.promises],
    conditions: [...state.conditions],
  };
}

function applyCreateNode(state: WorkflowState, event: CreateNodeEvent): void {
  const node: GraphNode = {
    id: event.node_id,
    name: event.name,
    state: "created",
    committed: false,
  };
  state.nodes.set(event.node_id, node);

  for (const dep of event.deps) {
    state.edges.push({
      from: dep.from_node,
      to: event.node_id,
      condition: dep.cond ?? undefined,
    });
  }
}

function applyStateTransition(state: WorkflowState, event: MarkStateTransition): void {
  const node = state.nodes.get(event.node_id);
  if (node) {
    node.state = event.new_state;
    node.result = event.result ?? undefined;
    node.errorMsg = event.error_msg ?? undefined;
    // Mark as committed on any state transition (including conditioned)
    node.committed = true;
  }
}

function applyCreatePromise(state: WorkflowState, event: CreatePromiseEvent): void {
  state.promises.push({
    constraintType: event.constraint_type,
    fromNodes: event.from_nodes,
    toNodes: event.to_nodes,
    constraintOp: event.constraint_op ?? undefined,
    constraintN: event.constraint_n ?? undefined,
  });
}

// Extract the input node ID and negation status from a condition object
function extractConditionInput(condition: Record<string, unknown>): { nodeId: number; negated: boolean } | null {
  // Direct input: {"inp": 3}
  if (typeof condition.inp === "number") {
    return { nodeId: condition.inp, negated: false };
  }
  // Negated condition: {"arg": {"inp": 3}}
  if (condition.arg && typeof condition.arg === "object") {
    const inner = extractConditionInput(condition.arg as Record<string, unknown>);
    if (inner) {
      return { nodeId: inner.nodeId, negated: !inner.negated };
    }
  }
  return null;
}

function applyCreateCondition(state: WorkflowState, event: CreateConditionEvent): void {
  state.conditions.push({
    caseGroups: event.case_groups,
  });

  // Create edges from condition input to conditioned tasks
  for (const caseGroup of event.case_groups) {
    const condInfo = extractConditionInput(caseGroup.condition);
    const taskId = parseInt(caseGroup.task_id as unknown as string, 10) || caseGroup.task_id;

    if (condInfo !== null && typeof taskId === "number") {
      // Add edge from the condition input node to the conditioned task
      // Mark it as a conditional edge
      state.edges.push({
        from: condInfo.nodeId,
        to: taskId as number,
        condition: condInfo.nodeId, // The condition source
        negated: condInfo.negated,
      });
    }
  }
}

export function applyEvent(state: WorkflowState, eventPayload: EventPayload): WorkflowState {
  const newState = cloneState(state);

  switch (eventPayload.event_type) {
    case "message":
      // Ignore message events per requirements
      return state;
    case "create_node":
      applyCreateNode(newState, eventPayload);
      break;
    case "mark_state_transition":
      applyStateTransition(newState, eventPayload);
      break;
    case "create_promise":
      applyCreatePromise(newState, eventPayload);
      break;
    case "create_condition":
      applyCreateCondition(newState, eventPayload);
      break;
  }

  return newState;
}

export function parseEvents(content: string): Event[] {
  const lines = content.trim().split("\n").filter(line => line.trim());
  const events: Event[] = [];

  for (const line of lines) {
    try {
      const event = JSON.parse(line) as Event;
      events.push(event);
    } catch {
      console.warn("Failed to parse event line:", line);
    }
  }

  return events;
}

// Filter out message events and return only visualization-relevant events
export function filterNonMessageEvents(events: Event[]): Event[] {
  return events.filter(e => e.event.event_type !== "message");
}

// Build state at a given event index (considering only non-message events)
export function buildStateAtIndex(events: Event[], index: number): WorkflowState {
  let state = createEmptyState();
  const filtered = filterNonMessageEvents(events);

  for (let i = 0; i <= index && i < filtered.length; i++) {
    state = applyEvent(state, filtered[i].event);
  }

  return state;
}
