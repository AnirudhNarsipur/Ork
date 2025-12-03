Given a workflow event log of the type generated in the `event_logs` directory, we'd like to generate a visualisation of the workflow in real-time.

## Guidelines
- Should update in realtime as new events come in
- Given that we have all the events should have "backwards" and "forwards" buttons"
- Ignore Message events
- Should have the ability to select a workflow event file to track
- Only work within the `viz` directory you should not modify anything outside of it.
## UI Suggestions - These are ideas not instructions
Display the actual task graph (task id ,status of task,dependencies between tasks, conditions on edges) in the center. Created nodes that are not comitted yet can be in a scratch box on the left, Promises should be in a box on the right

Make use of the promise/condition information to display richer information. For example if we know T1 needs to create one of X,Y,Z that info should be used to visualize [X,Y,Z] as a group