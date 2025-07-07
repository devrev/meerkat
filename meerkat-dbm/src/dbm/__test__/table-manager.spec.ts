import { TableLockManager } from '../table-lock-manager';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('TableLockManager', () => {
  let tableLockManager: TableLockManager;
  const tableName = 'testTable';

  beforeEach(() => {
    tableLockManager = new TableLockManager();
  });

  describe('write locks', () => {
    it('should acquire and release a write lock', async () => {
      await tableLockManager.lockTables([tableName], 'write');
      expect(tableLockManager.isTableLocked(tableName)).toBe(true);

      tableLockManager.unlockTables([tableName], 'write');
      expect(tableLockManager.isTableLocked(tableName)).toBe(false);
    });

    it('a second writer should wait for the first writer to release the lock', async () => {
      let writer1Finished = false;
      const writer1 = async () => {
        await tableLockManager.lockTables([tableName], 'write');

        await delay(50); // Simulate work

        expect(tableLockManager.isTableLocked(tableName)).toBe(true);
        tableLockManager.unlockTables([tableName], 'write');
        writer1Finished = true;
      };

      let writer2Finished = false;
      const writer2 = async () => {
        await tableLockManager.lockTables([tableName], 'write');

        // When writer2 gets the lock, writer1 should be finished.
        expect(writer1Finished).toBe(true);

        expect(tableLockManager.isTableLocked(tableName)).toBe(true);
        tableLockManager.unlockTables([tableName], 'write');
        writer2Finished = true;
      };

      await Promise.all([writer1(), writer2()]);

      expect(writer1Finished).toBe(true);
      expect(writer2Finished).toBe(true);

      expect(tableLockManager.isTableLocked(tableName)).toBe(false);
    });
  });

  describe('read locks', () => {
    it('should allow multiple readers to acquire a lock simultaneously', async () => {
      const readerPromises = [
        tableLockManager.lockTables([tableName], 'read'),
        tableLockManager.lockTables([tableName], 'read'),
        tableLockManager.lockTables([tableName], 'read'),
      ];

      await Promise.all(readerPromises);
      expect(tableLockManager.isTableLocked(tableName)).toBe(true);

      tableLockManager.unlockTables([tableName], 'read');
      tableLockManager.unlockTables([tableName], 'read');
      tableLockManager.unlockTables([tableName], 'read');
      expect(tableLockManager.isTableLocked(tableName)).toBe(false);
    });
  });

  describe('mixed read/write locks', () => {
    it('should not allow a writer if readers have the lock', async () => {
      await tableLockManager.lockTables([tableName], 'read');

      let writerAcquiredLock = false;
      const writerPromise = tableLockManager
        .lockTables([tableName], 'write')
        .then(() => {
          writerAcquiredLock = true;
        });

      await delay(10); // Give writer time to wait
      expect(writerAcquiredLock).toBe(false);
      expect(tableLockManager.isTableLocked(tableName)).toBe(true);

      // Reader releases the lock
      tableLockManager.unlockTables([tableName], 'read');

      await writerPromise;
      expect(writerAcquiredLock).toBe(true);
      expect(tableLockManager.isTableLocked(tableName)).toBe(true);

      tableLockManager.unlockTables([tableName], 'write');
      expect(tableLockManager.isTableLocked(tableName)).toBe(false);
    });

    it('should not allow a reader if a writer has the lock', async () => {
      await tableLockManager.lockTables([tableName], 'write');

      let readerAcquiredLock = false;
      const readerPromise = tableLockManager
        .lockTables([tableName], 'read')
        .then(() => {
          readerAcquiredLock = true;
        });

      await delay(10);
      expect(readerAcquiredLock).toBe(false);
      expect(tableLockManager.isTableLocked(tableName)).toBe(true);

      // Writer releases lock
      tableLockManager.unlockTables([tableName], 'write');

      await readerPromise;
      expect(readerAcquiredLock).toBe(true);

      tableLockManager.unlockTables([tableName], 'read');
      expect(tableLockManager.isTableLocked(tableName)).toBe(false);
    });

    it('should prioritize waiting readers over a new writer', async () => {
      // Writer1 acquires the lock
      await tableLockManager.lockTables([tableName], 'write');

      // Readers start waiting
      const readerPromises = [
        tableLockManager.lockTables([tableName], 'read'),
        tableLockManager.lockTables([tableName], 'read'),
      ];

      // Writer2 starts waiting
      const writer2Promise = tableLockManager.lockTables([tableName], 'write');

      // Release writer1's lock
      tableLockManager.unlockTables([tableName], 'write');

      // The waiting readers should get the lock next
      await Promise.all(readerPromises);
      expect(tableLockManager.isTableLocked(tableName)).toBe(true);

      let writer2AcquiredLock = false;
      writer2Promise.then(() => {
        writer2AcquiredLock = true;
      });

      await delay(10);
      // Writer2 should still be waiting
      expect(writer2AcquiredLock).toBe(false);

      // Readers release locks
      tableLockManager.unlockTables([tableName], 'read');
      tableLockManager.unlockTables([tableName], 'read');

      // Now writer2 should get the lock
      await writer2Promise;
      expect(writer2AcquiredLock).toBe(true);
      expect(tableLockManager.isTableLocked(tableName)).toBe(true);

      tableLockManager.unlockTables([tableName], 'write');
      expect(tableLockManager.isTableLocked(tableName)).toBe(false);
    });
  });
});
