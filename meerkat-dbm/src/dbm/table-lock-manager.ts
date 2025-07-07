import { TableLock } from './types';

/*
 * A table lock manager that allows multiple readersCount or a single writer to access a table at a time.
 */
export class TableLockManager {
  private tableLockRegistry: Record<string, TableLock> = {};

  private getOrCreateLock(tableName: string): TableLock {
    if (!this.tableLockRegistry[tableName]) {
      this.tableLockRegistry[tableName] = {
        readersCount: 0,
        readersQueue: [],
        writersQueue: [],
        writer: false,
      };
    }
    return this.tableLockRegistry[tableName];
  }

  async lockTables(
    tableNames: string[],
    mode: 'read' | 'write' = 'write'
  ): Promise<void> {
    const promises: Promise<void>[] = [];

    for (const tableName of tableNames) {
      const tableLock = this.getOrCreateLock(tableName);

      if (mode === 'read') {
        // Can read if no writer
        if (!tableLock.writer) {
          tableLock.readersCount++;
          continue;
        }

        // Wait for read access
        const promise = new Promise<void>((resolve) => {
          tableLock.readersQueue.push(resolve);
        });
        promises.push(promise);
      } else {
        // Can write if no readersCount and no writer
        if (tableLock.readersCount === 0 && !tableLock.writer) {
          tableLock.writer = true;
          continue;
        }

        // Wait for write access
        const promise = new Promise<void>((resolve) => {
          tableLock.writersQueue.push(resolve);
        });

        promises.push(promise);
      }
    }

    await Promise.all(promises);
  }

  unlockTables(tableNames: string[], mode: 'read' | 'write' = 'write'): void {
    for (const tableName of tableNames) {
      const tableLock = this.tableLockRegistry[tableName];
      if (!tableLock) continue;

      if (mode === 'read') {
        tableLock.readersCount--;

        // If no more readersCount, let a writer go
        if (tableLock.readersCount === 0 && tableLock.writersQueue.length > 0) {
          const nextWriter = tableLock.writersQueue.shift();

          if (nextWriter) {
            // Set the writer flag to true and call the writer to release the lock
            tableLock.writer = true;
            nextWriter();
          }
        }
      } else {
        tableLock.writer = false;

        // readersCount first, then writers
        if (tableLock.readersQueue.length > 0) {
          const readersCount = tableLock.readersQueue.splice(0);
          tableLock.readersCount += readersCount.length;

          // Call the readersCount to release the lock
          readersCount.forEach((reader) => reader());
        } else if (tableLock.writersQueue.length > 0) {
          const nextWriter = tableLock.writersQueue.shift();

          if (nextWriter) {
            // Set the writer flag to true and call the writer to release the lock
            tableLock.writer = true;
            nextWriter();
          }
        }
      }
    }
  }

  isTableLocked(tableName: string): boolean {
    const tableLock = this.tableLockRegistry[tableName];
    return tableLock ? tableLock.readersCount > 0 || tableLock.writer : false;
  }
}
