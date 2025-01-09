import 'fake-indexeddb/auto';
import { NativeBridge } from '../../dbm/dbm-native/native-bridge';
import { InstanceManagerType } from '../../dbm/instance-manager';
import { FILE_TYPES } from '../../types';
import { MeerkatDatabase } from '../indexed-db/meerkat-database';
import { NativeFileManager } from '../native/native-file-manager';
import log = require('loglevel');

describe('NativeFileManager', () => {
  let fileManager: NativeFileManager;
  let indexedDB: MeerkatDatabase;
  let instanceManager: InstanceManagerType;
  let nativeBridge: NativeBridge;

  const fileUrl = {
    tableName: 'taxi1',
    fileName: 'taxi1.parquet',
    url: 'https://example.com/taxi1.parquet',
    fileType: FILE_TYPES.PARQUET,
  };

  const fileUrls = [
    {
      tableName: 'taxi1',
      fileName: 'taxi2.parquet',
      url: 'https://example.com/taxi2.parquet',
      fileType: FILE_TYPES.PARQUET,
    },
    {
      tableName: 'taxi2',
      fileName: 'taxi3.parquet',
      url: 'https://example.com/taxi3.parquet',
      fileType: FILE_TYPES.PARQUET,
    },
  ];

  beforeAll(() => {
    instanceManager = {
      getDB: jest.fn(),
      terminateDB: jest.fn(),
    };

    nativeBridge = {
      registerFiles: jest.fn(),
      dropFilesByTableName: jest.fn(),
    } as unknown as NativeBridge;
  });

  beforeEach(async () => {
    indexedDB = new MeerkatDatabase();
    fileManager = new NativeFileManager({
      fetchTableFileBuffers: async () => [],
      instanceManager,
      logger: log,
      onEvent: (event) => {
        console.log(event);
      },
      nativeBridge,
    });
  });

  it('should register single file url', async () => {
    // Register single file url
    await fileManager.registerFileUrl(fileUrl);

    // Verify nativeBridge was called correctly
    expect(nativeBridge.registerFiles).toHaveBeenCalledWith([
      { ...fileUrl, type: 'url' },
    ]);

    // Fetch the stored data in the indexedDB
    const tableData1 = await indexedDB.tablesKey.toArray();

    expect(tableData1.length).toBe(1);
    expect(tableData1[0]).toEqual({
      tableName: 'taxi1',
      files: [
        {
          fileName: fileUrl.fileName,
          fileType: FILE_TYPES.PARQUET,
          url: fileUrl.url,
        },
      ],
    });
  });

  it('should bulk register file urls', async () => {
    // Bulk register file urls
    await fileManager.bulkRegisterFileUrl([fileUrl, ...fileUrls]);

    // Verify nativeBridge was called correctly
    expect(nativeBridge.registerFiles).toHaveBeenCalledWith(
      [fileUrl, ...fileUrls].map((file) => ({ ...file, type: 'url' }))
    );

    const tableData2 = await indexedDB.tablesKey.toArray();

    expect(tableData2.length).toBe(2);
    expect(tableData2[0].files.map((file) => file.fileName)).toEqual([
      'taxi1.parquet',
      'taxi2.parquet',
    ]);
  });

  it('should register single json file', async () => {
    const jsonFile = {
      tableName: 'taxi-json',
      fileName: 'taxi-json.parquet',
      json: {
        test: 'test',
      },
    };

    await fileManager.registerJSON(jsonFile);

    // Verify nativeBridge was called correctly
    expect(nativeBridge.registerFiles).toHaveBeenCalledWith([
      { ...jsonFile, type: 'json' },
    ]);

    const tableData = await indexedDB.tablesKey.toArray();

    expect(
      tableData.some((table) => table.tableName === jsonFile.tableName)
    ).toBe(true);
  });

  it('should bulk register json files', async () => {
    const jsonFiles = [
      {
        tableName: 'taxi-json-bulk',
        fileName: 'taxi-json1.parquet',
        json: {
          test: 'test1',
        },
      },
      {
        tableName: 'taxi-json-bulk',
        fileName: 'taxi-json2.parquet',
        json: {
          test: 'test2',
        },
      },
    ];

    await fileManager.bulkRegisterJSON(jsonFiles);

    // Verify nativeBridge was called correctly
    expect(nativeBridge.registerFiles).toHaveBeenCalledWith(
      jsonFiles.map((file) => ({ ...file, type: 'json' }))
    );

    const tableData = await indexedDB.tablesKey.toArray();

    expect(
      tableData.some((table) => table.tableName === jsonFiles[0].tableName)
    ).toBe(true);
  });

  it('should drop files by table name', async () => {
    await fileManager.registerFileUrl(fileUrl);
    await fileManager.bulkRegisterFileUrl(fileUrls);

    await fileManager.dropFilesByTableName('taxi1', ['taxi1.parquet']);

    // Verify nativeBridge was called correctly
    expect(nativeBridge.dropFilesByTableName).toHaveBeenCalledWith({
      tableName: 'taxi1',
      fileNames: ['taxi1.parquet'],
    });

    const tableData = await indexedDB.tablesKey.toArray();
    const taxi1Table = tableData.find((table) => table.tableName === 'taxi1');

    expect(taxi1Table?.files).toEqual([
      {
        fileName: 'taxi2.parquet',
        fileType: FILE_TYPES.PARQUET,
        url: 'https://example.com/taxi2.parquet',
      },
    ]);
  });
});
