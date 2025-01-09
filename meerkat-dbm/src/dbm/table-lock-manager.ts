import { TableLock } from './types';

export class TableLockManager {
  private tableLockRegistry: Record<string, TableLock> = {};

  async lockTables(tableNames: string[]): Promise<void> {
    const promises = [];

    for (const tableName of tableNames) {
      const tableLock = this.tableLockRegistry[tableName];

      // If the table lock doesn't exist, create a new lock
      if (!tableLock) {
        this.tableLockRegistry[tableName] = {
          isLocked: true,
          promiseQueue: [],
        };
        continue;
      }

      // If the table is already locked, add the promise to the queue
      if (tableLock.isLocked) {
        const promise = new Promise<void>((resolve, reject) => {
          tableLock.promiseQueue.push({ reject, resolve });
        });
        promises.push(promise);
      }

      // Set the table as locked
      tableLock.isLocked = true;
    }

    // Wait for all promises to resolve (locks to be acquired)
    await Promise.all(promises);
  }

  async unlockTables(tableNames: string[]): Promise<void> {
    for (const tableName of tableNames) {
      const tableLock = this.tableLockRegistry[tableName];

      // If the table lock doesn't exist, create a new lock
      if (!tableLock) {
        this.tableLockRegistry[tableName] = {
          isLocked: false,
          promiseQueue: [],
        };
      }

      const nextPromiseInQueue = tableLock?.promiseQueue?.shift();

      // If there is a promise in the queue, resolve it and keep the table as locked
      if (nextPromiseInQueue) {
        tableLock.isLocked = true;
        nextPromiseInQueue.resolve();
      } else {
        // If there are no promises in the queue, set the table as unlocked
        tableLock.isLocked = false;
      }
    }
  }

  public isTableLocked(tableName: string): boolean {
    return this.tableLockRegistry[tableName]?.isLocked ?? false;
  }
}
