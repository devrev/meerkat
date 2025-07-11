import { AsyncDuckDB } from '@duckdb/duckdb-wasm';
import 'fake-indexeddb/auto';
import { InstanceManagerType } from '../../dbm/instance-manager';
import { FILE_TYPES } from '../../types';
import { IndexedDBFileManager } from '../indexed-db/indexed-db-file-manager';
import { MeerkatDatabase } from '../indexed-db/meerkat-database';
import { JSON_FILE, JSON_FILES, mockDB } from './mock';
import log = require('loglevel');

describe('IndexedDBFileManager', () => {
  let fileManager: IndexedDBFileManager;
  let db: AsyncDuckDB;
  let indexedDB: MeerkatDatabase;
  let instanceManager: InstanceManagerType;

  const fileBuffer = {
    tableName: 'taxi1',
    fileName: 'taxi1.parquet',
    buffer: new Uint8Array([1, 2, 3]),
    fileType: FILE_TYPES.PARQUET,
    partitions: ['customer_oid=1'],
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
    indexedDB = new MeerkatDatabase();
    fileManager = new IndexedDBFileManager({
      fetchTableFileBuffers: async () => {
        return [];
      },
      instanceManager,
      logger: log,
      onEvent: (event) => {
        console.log(event);
      },
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
      files: [
        {
          fileName: fileBuffer.fileName,
          fileType: FILE_TYPES.PARQUET,
          partitions: fileBuffer.partitions,
        },
      ],
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

  it('should persist the data when the IndexedDB is reinitialized', async () => {
    const tableData = await indexedDB.tablesKey.toArray();
    const fileData = await indexedDB.files.toArray();

    expect(tableData.length).toBe(2);
    expect(fileData.length).toBe(3);
  });

  it('should override the file buffer when registering the same file again', async () => {
    const fileBufferData1 = await indexedDB.files.toArray();

    /**
     * The buffer value should be initial file buffer
     */
    expect(fileBufferData1[0].buffer).toEqual(fileBuffer.buffer);

    // Register the same file with a different buffer
    await fileManager.registerFileBuffer({
      ...fileBuffer,
      buffer: new Uint8Array([1]),
    });

    const fileBufferData2 = await indexedDB.files.toArray();

    /**
     * The buffer value should be updated to the new buffer
     */
    expect(fileBufferData2[0].buffer).toEqual(new Uint8Array([1]));
  });

  it('should return the table data', async () => {
    const fileData = await fileManager.getTableData({
      name: 'taxi1',
      partitions: fileBuffer.partitions,
    });

    expect(fileData).toEqual({
      files: [
        {
          fileName: 'taxi1.parquet',
          fileType: 'parquet',
          partitions: fileBuffer.partitions,
        },
      ],
      tableName: 'taxi1',
    });
  });

  it('should drop the file buffers for a table', async () => {
    // Drop a file when there are multiple files
    await fileManager.dropFilesByTableName('taxi1', ['taxi1.parquet']);

    const tableData1 = await indexedDB.tablesKey.toArray();
    const fileBufferData1 = await indexedDB.files.toArray();

    // Verify that the file is dropped
    expect(tableData1[0]).toEqual({
      tableName: 'taxi1',
      files: [
        {
          fileName: 'taxi2.parquet',
          fileType: 'parquet',
        },
      ],
    });

    expect(
      fileBufferData1.some((file) => file.fileName !== 'taxi1.parquet')
    ).toBe(true);

    // Drop a file when there is only one file
    await fileManager.dropFilesByTableName('taxi1', ['taxi2.parquet']);

    const tableData2 = await indexedDB.tablesKey.toArray();
    const fileBufferData2 = await indexedDB.files.toArray();

    // Verify that the file is dropped
    expect(tableData2[0]).toEqual({
      tableName: 'taxi1',
      files: [],
    });

    expect(
      fileBufferData2.some((file) => file.fileName !== 'taxi2.parquet')
    ).toBe(true);
  });

  it('should set the metadata for a table', async () => {
    await fileManager.setTableMetadata('taxi1', { test: 'test' });

    const tableData = await indexedDB.tablesKey.toArray();

    expect(tableData[0].metadata).toEqual({ test: 'test' });
  });

  it('should register JSON data', async () => {
    await fileManager.registerJSON(JSON_FILE);

    const tableData = await indexedDB.tablesKey.toArray();
    const fileBufferData = await indexedDB.files.toArray();

    expect(
      tableData.some((table) => table.tableName === JSON_FILE.tableName)
    ).toBe(true);

    expect(
      fileBufferData.some((file) => file.fileName === JSON_FILE.fileName)
    ).toBe(true);
  });

  it('should register multiple JSON data', async () => {
    await fileManager.bulkRegisterJSON(JSON_FILES);

    const tableData = await indexedDB.tablesKey.toArray();
    const fileBufferData = await indexedDB.files.toArray();

    expect(
      tableData.some((table) => table.tableName === JSON_FILES[0].tableName)
    ).toBe(true);

    expect(fileBufferData.map((file) => file.fileName)).toEqual(
      expect.arrayContaining(JSON_FILES.map((file) => file.fileName))
    );
  });

  it('should get the files for tables', async () => {
    const metadata = {
      test: 'test',
    };

    await fileManager.registerFileBuffer(fileBuffer);

    await fileManager.setTableMetadata('taxi1', metadata);

    const files = await fileManager.getFilesNameForTables([{ name: 'taxi1' }]);

    expect(files).toEqual([
      {
        tableName: 'taxi1',
        files: [
          {
            fileName: 'taxi1.parquet',
            fileType: 'parquet',
            partitions: fileBuffer.partitions,
          },
        ],
        metadata,
      },
    ]);
  });
});
