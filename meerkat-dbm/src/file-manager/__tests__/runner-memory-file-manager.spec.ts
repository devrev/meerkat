import log from 'loglevel';
import { InstanceManager } from '../../dbm/__test__/utils';
import { BROWSER_RUNNER_TYPE } from '../../window-communication/runner-types';
import { CommunicationInterface } from '../../window-communication/window-communication';
import { RunnerMemoryDBFileManager } from '../memory/runner-memory-file-manager';

describe('RunnerMemoryDBFileManager', () => {
  let fileManager: RunnerMemoryDBFileManager;
  let instanceManager: InstanceManager;
  let communication: jest.Mocked<CommunicationInterface<any>>;

  const fileBuffers = [
    {
      fileName: 'test1.parquet',
      buffer: new Uint8Array([1, 2, 3]),
      tableName: 'table1',
    },
    {
      fileName: 'test2.parquet',
      buffer: new Uint8Array([4, 5, 6]),
      tableName: 'table2`',
    },
  ];

  beforeEach(() => {
    instanceManager = new InstanceManager();

    communication = {
      sendRequest: jest.fn(),
    } as any;

    fileManager = new RunnerMemoryDBFileManager({
      instanceManager: instanceManager,
      logger: log,
      onEvent: jest.fn(),
      communication: communication,
      fetchTableFileBuffers: jest.fn(),
    });
  });

  it('should register file buffers', async () => {
    const db = await instanceManager.getDB();
    const registerFileBufferSpy = jest.spyOn(db, 'registerFileBuffer');

    await fileManager.bulkRegisterFileBuffer(fileBuffers);

    // Check if the registerFileBuffer method is called with the correct arguments
    expect(registerFileBufferSpy).toHaveBeenCalledTimes(2);
    expect(registerFileBufferSpy).toHaveBeenCalledWith(
      fileBuffers[0].fileName,
      fileBuffers[0].buffer
    );
    expect(registerFileBufferSpy).toHaveBeenCalledWith(
      fileBuffers[1].fileName,
      fileBuffers[1].buffer
    );
  });

  it('should mount file buffers for given tables', async () => {
    const tables = [{ name: 'table1', fileName: 'table1.parquet' }];

    communication.sendRequest.mockResolvedValue({
      timestamp: Date.now(),
      uuid: 'uuid',
      message: [fileBuffers[0]],
      target_app: 'runner',
    });

    const bulkRegisterFileBufferSpy = jest.spyOn(
      fileManager,
      'bulkRegisterFileBuffer'
    );

    // Mount the table1
    await fileManager.mountFileBufferByTables(tables);

    // Check if the sendRequest method is called with table1
    expect(communication.sendRequest).toHaveBeenCalledWith({
      type: BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS,
      payload: { tables },
    });
    expect(bulkRegisterFileBufferSpy).toHaveBeenCalledWith([fileBuffers[0]]);
  });

  it('should not mount already mounted tables', async () => {
    const tables = [{ name: 'table1', fileName: 'table1.parquet' }];

    communication.sendRequest.mockResolvedValue({
      timestamp: Date.now(),
      uuid: 'uuid',
      message: [fileBuffers[0]],
      target_app: 'runner',
    });

    // Mount the table1
    await fileManager.mountFileBufferByTables(tables);

    // Check if the sendRequest method is called with table1
    expect(communication.sendRequest).toHaveBeenCalledWith({
      type: BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS,
      payload: { tables: [tables[0]] },
    });
    expect(communication.sendRequest).toHaveBeenCalledTimes(1);

    // try mount the same table again
    await fileManager.mountFileBufferByTables(tables);

    // Check if the sendRequest method is not called again
    expect(communication.sendRequest).toHaveBeenCalledTimes(1);
  });

  it('should mount the tables that are not already mounted', async () => {
    const tables = [
      { name: 'table1', fileName: 'table1.parquet' },
      { name: 'table2', fileName: 'table2.parquet' },
    ];

    communication.sendRequest.mockResolvedValue({
      timestamp: Date.now(),
      uuid: 'uuid',
      message: fileBuffers,
      target_app: 'runner',
    });

    // Mount the table1
    await fileManager.mountFileBufferByTables([tables[0]]);

    // Check if the sendRequest method is called with table1
    expect(communication.sendRequest).toHaveBeenCalledWith({
      type: BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS,
      payload: { tables: [tables[0]] },
    });
    expect(communication.sendRequest).toHaveBeenCalledTimes(1);

    // Mount the both table1 and table2
    await fileManager.mountFileBufferByTables(tables);

    // Check if the sendRequest method is called with only table2
    expect(communication.sendRequest).lastCalledWith({
      type: BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS,
      payload: { tables: [tables[1]] },
    });
    expect(communication.sendRequest).toHaveBeenCalledTimes(2);
  });
});
