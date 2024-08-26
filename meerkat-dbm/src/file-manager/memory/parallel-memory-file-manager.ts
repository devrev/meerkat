import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table, TableWiseFiles } from '../../types';
import { getBufferFromJSON } from '../../utils';
import {
  BaseFileStore,
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';

export interface ParallelMemoryFileManagerType {
  /**
   *
   * @description
   * Retrieves the buffer data for the tables.
   * @param tables - An array of tables.
   * @returns An array of FileBufferStore objects.
   */
  getTableBufferData?: (tables: TableConfig[]) => Promise<
    (BaseFileStore & {
      buffer: SharedArrayBuffer;
    })[]
  >;
}

export class ParallelMemoryFileManager
  implements ParallelMemoryFileManagerType, FileManagerType<SharedArrayBuffer>
{
  private instanceManager: InstanceManagerType;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  private tableFileBuffersMap: Map<
    string,
    FileBufferStore<SharedArrayBuffer>[]
  > = new Map();

  constructor({
    instanceManager,
    logger,
    onEvent,
  }: FileManagerConstructorOptions) {
    this.instanceManager = instanceManager;
    this.logger = logger;
    this.onEvent = onEvent;
  }

  private _emitEvent(event: DBMEvent) {
    if (this.onEvent) {
      this.onEvent(event);
    }
  }

  async bulkRegisterFileBuffer(
    fileBuffers: FileBufferStore<SharedArrayBuffer>[]
  ): Promise<void> {
    const promiseArr = fileBuffers.map((fileBuffer) =>
      this.registerFileBuffer(fileBuffer)
    );
    await Promise.all(promiseArr);
  }

  async registerFileBuffer(
    fileBuffer: FileBufferStore<SharedArrayBuffer>
  ): Promise<void> {
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
      /**
       * TODO: Add types here
       */
      buffer: bufferData as any,
      tableName,
      ...fileData,
    });
  }

  async getTableBufferData(tables: TableConfig[]) {
    const response = tables.flatMap((table) => {
      const tableFileBuffers = this.tableFileBuffersMap.get(table.name) ?? [];

      return tableFileBuffers;
    });
    return response;
  }

  getFileBuffer(name: string): Promise<SharedArrayBuffer> {
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

  private clearAllFileBuffers(): void {
    for (const [tableName, fileBufferStores] of this.tableFileBuffersMap) {
      const clearedStores = fileBufferStores.map((store) => ({
        ...store,
        buffer: new SharedArrayBuffer(0), // Replace with an empty buffer
      }));
      this.tableFileBuffersMap.set(tableName, clearedStores);
    }
  }

  async onDBShutdownHandler() {
    this.clearAllFileBuffers();
    this.tableFileBuffersMap.clear();
  }
}
