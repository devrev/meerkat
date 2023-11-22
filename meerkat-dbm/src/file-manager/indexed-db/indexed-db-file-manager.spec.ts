import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import 'fake-indexeddb/auto';
import { DuckDBDatabase } from './duckdb-database';
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
  let indexedDB: DuckDBDatabase;

  const fileBuffer = {
    tableName: 'taxi1',
    fileName: 'taxi1.parquet',
    buffer: new Uint8Array([1, 2, 3]),
  };

  const fileBuffers = [
    {
      tableName: 'taxi1',
      fileName: 'taxi2.parquet',
      buffer: new Uint8Array([1, 2, 3, 4]),
    },
    {
      tableName: 'taxi2',
      fileName: 'taxi3.parquet',
      buffer: new Uint8Array([1, 2, 3, 4, 5]),
    },
  ];

  beforeAll(() => {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    db = mockDB;
  });

  beforeEach(() => {
    indexedDB = new DuckDBDatabase();
    fileManager = new IndexedDBFileManager({
      fetchTableFileBuffers: async () => {
        return [];
      },
      db,
    });
  });

  it('should register file buffers', async () => {
    // Register single file buffer
    await fileManager.registerFileBuffer(fileBuffer);

    // Fetch the stored data in the indexedDB
    const datasetData1 = await indexedDB.datasets.toArray();
    const fileBufferData1 = await indexedDB.files.toArray();

    /**
     * There should be one dataset with the table name 'taxi1' and one file buffer with the file name 'taxi1.parquet'
     */
    expect(datasetData1.length).toBe(1);
    expect(datasetData1[0]).toEqual({
      tableName: 'taxi1',
      files: [{ fileName: fileBuffer.fileName }],
    });

    /**
     * There should be one file buffer
     */
    expect(fileBufferData1.length).toBe(1);

    // Bulk register file buffers
    await fileManager.bulkRegisterFileBuffer(fileBuffers);

    const datasetData2 = await indexedDB.datasets.toArray();
    const fileBufferData2 = await indexedDB.files.toArray();

    /**
     * There should be two datasets
     * The first taxi1 dataset should have two file buffers
     */
    expect(datasetData2.length).toBe(2);
    expect(datasetData2[0].files.map((file) => file.fileName)).toEqual([
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
    const datasetData = await indexedDB.datasets.toArray();
    const fileBufferData = await indexedDB.files.toArray();

    /**
     * There should be no datasets and no file buffers
     */
    expect(datasetData.length).toBe(0);
    expect(fileBufferData.length).toBe(0);
  });

  it('should override the file buffer when registering the same file again', async () => {
    // Register single file buffer
    await fileManager.registerFileBuffer(fileBuffer);

    const datasetData = await indexedDB.datasets.toArray();
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
     * There should be one dataset with one file buffer
     */
    expect(datasetData.length).toBe(1);
    expect(datasetData[0].files.length).toBe(1);

    /**
     * There should be one file buffer, the buffer should be updated to the new buffer
     */
    expect(fileBufferData2.length).toBe(1);
    expect(fileBufferData2[0].buffer).toEqual(new Uint8Array([1]));
  });
});
