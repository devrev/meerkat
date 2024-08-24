import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table, TableWiseFiles } from '../../types';
import { getBufferFromJSON } from '../../utils';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';

export class ParallelMemoryFileManager implements FileManagerType {
  private instanceManager: InstanceManagerType;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  private tableFileBuffersMap: Map<string, FileBufferStore[]> = new Map();

  constructor({
    instanceManager,
    logger,
    onEvent,
  }: FileManagerConstructorOptions) {
    this.instanceManager = instanceManager;
    this.logger = logger;
    this.onEvent = onEvent;
  }

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
    const promiseArr = fileBuffers.map((fileBuffer) =>
      this.registerFileBuffer(fileBuffer)
    );
    await Promise.all(promiseArr);
  }

  async registerFileBuffer(fileBuffer: FileBufferStore): Promise<void> {
    const existingFiles =
      this.tableFileBuffersMap.get(fileBuffer.tableName) || [];

    existingFiles.push(fileBuffer);

    this.tableFileBuffersMap.set(fileBuffer.tableName, existingFiles);
  }

  async bulkRegisterJSON(jsonData: FileJsonStore[]): Promise<void> {
    const promiseArr = jsonData.map((fileBuffer) =>
      this.registerJSON(fileBuffer)
    );

    await Promise.all(promiseArr);
  }

  async registerJSON(jsonData: FileJsonStore): Promise<void> {
    const { json, tableName, ...fileData } = jsonData;

    /**
     * Convert JSON to buffer
     */
    const bufferData = await getBufferFromJSON({
      instanceManager: this.instanceManager,
      json,
      tableName,
      logger: this.logger,
      onEvent: this.onEvent,
      metadata: jsonData.metadata,
    });

    /**
     * Register buffer in DB
     */
    await this.registerFileBuffer({
      buffer: bufferData,
      tableName,
      ...fileData,
    });
  }

  async getTableBufferData(tables: TableConfig[]) {
    console.info('tableFileBuffersMap', this.tableFileBuffersMap);

    // Return all the buffers
    return tables.flatMap((table) => {
      const tableFileBuffers = this.tableFileBuffersMap.get(table.name) ?? [];

      return tableFileBuffers?.map((buffer) => {
        // Create a SharedArrayBuffer with the same length as the original buffer
        // const sharedBuffer = new SharedArrayBuffer(10);

        // Create a new Uint8Array view of the SharedArrayBuffer
        // const sharedArray = new Int32Array(buffer.buffer);

        // Copy the contents of the original buffer to the shared array
        // sharedArray.set(new Uint8Array(1));

        return {
          ...buffer,
          buffer: buffer.buffer, // Expose as Uint8Array
        };
      });
    });
  }

  getFileBuffer(name: string): Promise<Uint8Array> {
    throw new Error('Method not implemented.');
  }

  async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    // no-op
  }

  async getFilesNameForTables(
    tables: TableConfig[]
  ): Promise<TableWiseFiles[]> {
    return [];
  }

  async getTableData(table: TableConfig): Promise<Table | undefined> {
    // not needed for memory file manager
    return;
  }

  async setTableMetadata(table: string, metadata: object): Promise<void> {
    // not needed for memory file manager
  }

  async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    // not needed for memory file manager
  }

  async onDBShutdownHandler() {
    // not needed for memory file manager
  }
}
