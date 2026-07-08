export interface DurationEvents {
  event_name:
    | 'query_execution_duration'
    | 'mount_file_buffer_duration'
    | 'query_queue_duration'
    | 'json_to_buffer_conversion_duration'
    | 'clone_buffer_duration';
  duration: number;
}

export interface QueueEvents {
  event_name: 'query_queue_length';
  value: number;
}

export type DBMEvent = (DurationEvents | QueueEvents) & { metadata?: object };

/**
 * Event names that belong to a single `queryWithTables` run. These are routed
 * to the per-query {@link QueryOptions.onEvent} when one is supplied.
 *
 * Every other event (`query_queue_length` — a cross-query gauge;
 * `json_to_buffer_conversion_duration` — emitted at load time outside any
 * query; `clone_buffer_duration` — runner-side buffer setup) has no single
 * owning query and is routed to the instance-level callback only.
 */
const QUERY_SCOPED_EVENT_NAMES: ReadonlySet<DBMEvent['event_name']> = new Set([
  'mount_file_buffer_duration',
  'query_execution_duration',
  'query_queue_duration',
]);

/**
 * Whether an event is scoped to a single query run (vs. cross-query/load-time).
 * Routing decisions key off this so the scope lives with the event definition,
 * not at each emit/dispatch site.
 */
export const isQueryScopedEvent = (event: DBMEvent): boolean =>
  QUERY_SCOPED_EVENT_NAMES.has(event.event_name);
