import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import 'fake-indexeddb/auto';
import { InstanceManagerType } from '../../dbm/instance-manager';
import { FILE_TYPES } from '../file-manager-type';
import { DuckDBFilesDatabase } from './duckdb-files-database';
import { IndexedDBFileManager } from './indexed-db-file-manager';

const mockDB = {
  registerFileBuffer: async (fileName: string, buffer: Uint8Array) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve([fileName]);
      }, 200);
    });
  },
  unregisterFileBuffer: async (fileName: string) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve([fileName]);
      }, 200);
    });
  },
};

describe('IndexedDBFileManager', () => {
  let fileManager: IndexedDBFileManager;
  let db: AsyncDuckDB;
  let indexedDB: DuckDBFilesDatabase;
  let instanceManager: InstanceManagerType;

  const fileBuffer = {
    tableName: 'taxi1',
    fileName: 'taxi1.parquet',
    buffer: new Uint8Array([1, 2, 3]),
    fileType: FILE_TYPES.PARQUET,
  };

  const fileBuffers = [
    {
      tableName: 'taxi1',
      fileName: 'taxi2.parquet',
      buffer: new Uint8Array([1, 2, 3, 4]),
      fileType: FILE_TYPES.PARQUET,
    },
    {
      tableName: 'taxi2',
      fileName: 'taxi3.parquet',
      buffer: new Uint8Array([1, 2, 3, 4, 5]),
      fileType: FILE_TYPES.PARQUET,
    },
  ];

  beforeAll(() => {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    db = mockDB;
    instanceManager = {
      getDB: async () => {
        return db;
      },
      terminateDB: async () => {
        return;
      },
    };
  });

  beforeEach(async () => {
    indexedDB = new DuckDBFilesDatabase();
    fileManager = new IndexedDBFileManager({
      fetchTableFileBuffers: async () => {
        return [];
      },
      instanceManager,
    });

    await fileManager.initializeDB();
  });

  it('should register file buffers', async () => {
    // Register single file buffer
    await fileManager.registerFileBuffer(fileBuffer);

    // Fetch the stored data in the indexedDB
    const tableData1 = await indexedDB.tablesKey.toArray();
    const fileBufferData1 = await indexedDB.files.toArray();

    /**
     * There should be one table with the table name 'taxi1' and one file buffer with the file name 'taxi1.parquet'
     */
    expect(tableData1.length).toBe(1);
    expect(tableData1[0]).toEqual({
      tableName: 'taxi1',
      files: [{ fileName: fileBuffer.fileName, fileType: FILE_TYPES.PARQUET }],
    });

    /**
     * There should be one file buffer
     */
    expect(fileBufferData1.length).toBe(1);

    // Bulk register file buffers
    await fileManager.bulkRegisterFileBuffer(fileBuffers);

    const tableData2 = await indexedDB.tablesKey.toArray();
    const fileBufferData2 = await indexedDB.files.toArray();

    /**
     * There should be two tablesKey
     * The first taxi1 table should have two file buffers
     */
    expect(tableData2.length).toBe(2);
    expect(tableData2[0].files.map((file) => file.fileName)).toEqual([
      'taxi1.parquet',
      'taxi2.parquet',
    ]);

    /**
     * There should be three file buffers
     */
    expect(fileBufferData2.map((file) => file.fileName)).toEqual([
      'taxi1.parquet',
      'taxi2.parquet',
      'taxi3.parquet',
    ]);
  });

  it('should flush the database when initialized', async () => {
    // Fetch the stored data in the indexedDB
    const tableData = await indexedDB.tablesKey.toArray();
    const fileBufferData = await indexedDB.files.toArray();

    /**
     * There should be no tablesKey and no file buffers
     */
    expect(tableData.length).toBe(0);
    expect(fileBufferData.length).toBe(0);
  });

  it('should override the file buffer when registering the same file again', async () => {
    // Register single file buffer
    await fileManager.registerFileBuffer(fileBuffer);

    const tableData = await indexedDB.tablesKey.toArray();
    const fileBufferData1 = await indexedDB.files.toArray();

    /**
     * There should be one file buffer, the buffer should be initial file buffer
     */
    expect(fileBufferData1.length).toBe(1);
    expect(fileBufferData1[0].buffer).toEqual(fileBuffer.buffer);

    // Register the same file with a different buffer
    await fileManager.registerFileBuffer({
      ...fileBuffer,
      buffer: new Uint8Array([1]),
    });

    const fileBufferData2 = await indexedDB.files.toArray();

    /**
     * There should be one table with one file buffer
     */
    expect(tableData.length).toBe(1);
    expect(tableData[0].files.length).toBe(1);

    /**
     * There should be one file buffer, the buffer should be updated to the new buffer
     */
    expect(fileBufferData2.length).toBe(1);
    expect(fileBufferData2[0].buffer).toEqual(new Uint8Array([1]));
  });
});
