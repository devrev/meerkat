import log from 'loglevel';
import { InstanceManager } from '../../dbm/__test__/mock.spec';
import { FileBufferStore } from '../file-manager-type';
import { ParallelMemoryFileManager } from '../memory/parallel-memory-file-manager';

describe('ParallelMemoryFileManager', () => {
  let instanceManager: InstanceManager;
  let fileManager: ParallelMemoryFileManager;

  const fileBuffer: FileBufferStore = {
    tableName: 'table1',
    fileName: 'file1.parquet',
    buffer: new Uint8Array([1, 2, 3]),
  };

  beforeEach(() => {
    instanceManager = new InstanceManager();

    fileManager = new ParallelMemoryFileManager({
      instanceManager: instanceManager,
      logger: log,
      onEvent: jest.fn(),
      fetchTableFileBuffers: jest.fn(),
    });
  });

  it('should register file buffer correctly', async () => {
    // Register the file buffer
    await fileManager.registerFileBuffer(fileBuffer);

    // Check if the file buffer is registered correctly
    const tableBuffers = fileManager['tableFileBuffersMap'].get('table1');
    expect(tableBuffers).toContain(fileBuffer);
  });

  it('should register duplicate file buffer correctly', async () => {
    // Register the file buffer twice
    await fileManager.registerFileBuffer(fileBuffer);
    await fileManager.registerFileBuffer(fileBuffer);

    // Check if the file buffer is registered correctly
    const tableBuffers = fileManager['tableFileBuffersMap'].get('table1');
    expect(tableBuffers).toContain(fileBuffer);
  });

  it('should register multiple file buffers', async () => {
    const fileBuffers: FileBufferStore[] = [
      {
        tableName: 'table1',
        fileName: 'file1.parquet',
        buffer: new Uint8Array([1, 2, 3]),
      },
      {
        tableName: 'table2',
        fileName: 'file2.parquet',
        buffer: new Uint8Array([4, 5, 6]),
      },
    ];

    // Register multiple file buffers
    await fileManager.bulkRegisterFileBuffer(fileBuffers);

    //  Check if the file buffers are registered correctly
    const table1Buffers = fileManager['tableFileBuffersMap'].get('table1');
    const table2Buffers = fileManager['tableFileBuffersMap'].get('table2');

    expect(table1Buffers).toContain(fileBuffers[0]);
    expect(table2Buffers).toContain(fileBuffers[1]);
  });

  it('should retrieve the buffer data for the tables', async () => {
    // Register the file buffer
    await fileManager.registerFileBuffer(fileBuffer);

    // Fetch the buffer data
    const tables = [{ name: 'table1', fileName: 'file1.parquet' }];
    const bufferData = await fileManager.getTableBufferData(tables);

    // Check if the buffer data is retrieved correctly
    expect(bufferData).toEqual([fileBuffer]);
  });

  it('should clear all file buffers and the map on shutdown', async () => {
    // Register the file buffer
    await fileManager.registerFileBuffer(fileBuffer);

    // shutdown the file manager
    await fileManager.onDBShutdownHandler();

    const tableBuffers = fileManager['tableFileBuffersMap'].get('table1');

    // Check if the file buffer is cleared
    expect(tableBuffers).toEqual(undefined);
    expect(fileManager['tableFileBuffersMap'].size).toBe(0);
  });
});
