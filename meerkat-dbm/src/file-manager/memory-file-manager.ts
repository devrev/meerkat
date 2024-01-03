import { InstanceManagerType } from '../dbm/instance-manager';
import { DBMEvent, DBMLogger } from '../logger';
import { Table, TableWiseFiles } from '../types';
import { getBufferFromJSON } from '../utils';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from './file-manager-type';

export class MemoryDBFileManager implements FileManagerType {
  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  instanceManager: InstanceManagerType;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    logger,
    onEvent,
  }: FileManagerConstructorOptions) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.instanceManager = instanceManager;
    this.logger = logger;
    this.onEvent = onEvent;
  }

  async bulkRegisterFileBuffer(props: FileBufferStore[]): Promise<void> {
    const promiseArr = props.map((fileBuffer) =>
      this.registerFileBuffer(fileBuffer)
    );
    console.info('bulkRegisterFileBuffer', promiseArr);
    const output = await Promise.all(promiseArr);
    console.info('bulkRegisterFileBuffer done', output);
    console.info('bulkRegisterFileBuffer done', promiseArr);
  }

  async registerFileBuffer(props: FileBufferStore): Promise<void> {
    console.info('registerFileBuffer', props);
    const db = await this.instanceManager.getDB();
    return db.registerFileBuffer(props.fileName, props.buffer);
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

  getFileBuffer(name: string): Promise<Uint8Array> {
    throw new Error('Method not implemented.');
  }

  async mountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    // not needed for memory file manager
  }

  async unmountFileBufferByTableNames(tableNames: string[]): Promise<void> {
    // not needed for memory file manager
  }

  async getFilesNameForTables(tableNames: string[]): Promise<TableWiseFiles[]> {
    // not needed for memory file manager
    return [];
  }

  async getTableData(tableName: string): Promise<Table | undefined> {
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
