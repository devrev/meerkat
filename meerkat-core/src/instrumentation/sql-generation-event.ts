/**
 * Instrumentation for the cube-query -> SQL generation path. This lives in
 * meerkat-core (which meerkat-node/meerkat-browser already depend on) so the
 * SQL builders can emit timings without taking a dependency on meerkat-dbm's
 * DBMEvent. It is intentionally a separate event shape from DBMEvent: the two
 * cover different phases (generation vs. execution) and different packages.
 */

/**
 * Distinct phases of `cubeQueryToSQL`. `total` wraps the whole call; the rest
 * are the individually timed steps that add up to (roughly) it.
 */
export type SqlGenerationPhase =
  | 'base_sql'
  | 'ast_build'
  | 'ast_deserialize_roundtrip'
  | 'filter_params'
  | 'projections'
  | 'total';

export interface SqlGenerationEvent {
  event_name: 'sql_generation_duration';
  phase: SqlGenerationPhase;
  /** Duration in milliseconds. */
  duration: number;
  metadata?: object;
}

/**
 * Optional per-call callback for SQL-generation timings. Supplied by the caller
 * of `cubeQueryToSQL`; invoked once per phase (plus once for `total`).
 */
export type SqlGenerationOnEvent = (event: SqlGenerationEvent) => void;

/**
 * Time an async phase and, when an `onEvent` is supplied, emit its duration.
 * Returns the phase's result untouched so call sites read as a thin wrapper.
 * The event fires even if `fn` throws is NOT desired here — a failed phase has
 * no meaningful duration to report, so timing is emitted only on success.
 */
export const timePhase = async <T>(
  phase: SqlGenerationPhase,
  fn: () => Promise<T>,
  onEvent?: SqlGenerationOnEvent,
  metadata?: object
): Promise<T> => {
  if (!onEvent) {
    return fn();
  }
  const start = performance.now();
  const result = await fn();
  onEvent({
    event_name: 'sql_generation_duration',
    phase,
    duration: performance.now() - start,
    metadata,
  });
  return result;
};
