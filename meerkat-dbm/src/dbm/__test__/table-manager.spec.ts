import { TableLockManager } from '../table-lock-manager';

describe('Table Lock Manager', () => {
  let tableLockManager: TableLockManager;

  beforeEach(() => {
    tableLockManager = new TableLockManager();
  });

  it('should lock the table and release it', async () => {
    const tableName = 'exampleTable';

    // Request the lock for the table and then release it
    await tableLockManager.lockTables([tableName]);

    expect(tableLockManager.isTableLocked(tableName)).toBe(true);

    await tableLockManager.unlockTables([tableName]);

    expect(tableLockManager.isTableLocked(tableName)).toBe(false);

    // Again request the lock for the table
    await tableLockManager.lockTables([tableName]);

    await tableLockManager.unlockTables([tableName]);
  });

  it('two consumers requesting lock for the same table', async () => {
    const tableName = 'exampleTable';

    // Set up promises for the two consumers
    const consumer1Promise = tableLockManager.lockTables([tableName]);
    const consumer2Promise = tableLockManager.lockTables([tableName]);

    // Wait for the first consumer to get the lock
    await expect(consumer1Promise).resolves.toBeUndefined();

    expect(tableLockManager.isTableLocked(tableName)).toBe(true);

    const timeout1 = new Promise((resolve) => {
      setTimeout(resolve, 1000, 'TIMEOUT');
    });

    // Promise.race will wait for either the promises be resolved
    // consumer2 will not be able to get the lock as it is already locked by consumer1
    await expect(Promise.race([consumer2Promise, timeout1])).resolves.toBe(
      'TIMEOUT'
    );

    // Release the lock for the first consumer
    await tableLockManager.unlockTables([tableName]);

    // Check if the table is still locked as the consumer2 will get the lock
    expect(tableLockManager.isTableLocked(tableName)).toBe(true);

    const timeout2 = new Promise((resolve) => {
      setTimeout(resolve, 1000, 'TIMEOUT');
    });

    // This time the consumer2 will get the lock
    await expect(
      Promise.race([consumer2Promise, timeout2])
    ).resolves.toBeUndefined();

    // Release the lock
    await tableLockManager.unlockTables([tableName]);

    expect(tableLockManager.isTableLocked(tableName)).toBe(false);
  });
});
