import { SqlGenerationEvent, timePhase } from './sql-generation-event';

describe('timePhase', () => {
  it('returns the wrapped result unchanged', async () => {
    const result = await timePhase('base_sql', async () => 'sql-string');
    expect(result).toBe('sql-string');
  });

  it('does not invoke onEvent when omitted', async () => {
    // Simply must not throw when no callback is supplied.
    await expect(
      timePhase('ast_build', async () => 42)
    ).resolves.toBe(42);
  });

  it('emits a sql_generation_duration event for the phase when onEvent is supplied', async () => {
    const events: SqlGenerationEvent[] = [];

    await timePhase('filter_params', async () => 'x', (e) => events.push(e));

    expect(events).toHaveLength(1);
    expect(events[0]).toEqual(
      expect.objectContaining({
        event_name: 'sql_generation_duration',
        phase: 'filter_params',
      })
    );
    expect(typeof events[0].duration).toBe('number');
    expect(events[0].duration).toBeGreaterThanOrEqual(0);
  });

  it('forwards metadata onto the event', async () => {
    const events: SqlGenerationEvent[] = [];

    await timePhase(
      'projections',
      async () => null,
      (e) => events.push(e),
      { queryId: 'q-1' }
    );

    expect(events[0].metadata).toEqual({ queryId: 'q-1' });
  });

  it('does not emit an event when the phase throws', async () => {
    const events: SqlGenerationEvent[] = [];

    await expect(
      timePhase(
        'ast_deserialize_roundtrip',
        async () => {
          throw new Error('boom');
        },
        (e) => events.push(e)
      )
    ).rejects.toThrow('boom');

    expect(events).toHaveLength(0);
  });
});
