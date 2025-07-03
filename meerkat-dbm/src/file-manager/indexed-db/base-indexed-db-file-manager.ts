import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table } from '../../types';
import { getBufferFromJSON, isDefined } from '../../utils';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { getFilesByPartition } from '../partition';
import { MeerkatDatabase } from './meerkat-database';

export abstract class BaseIndexedDBFileManager implements FileManagerType {
  protected indexedDB: MeerkatDatabase; // IndexedDB instance
  protected instanceManager: InstanceManagerType;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  constructor({
    instanceManager,
    logger,
    onEvent,
  }: FileManagerConstructorOptions) {
    this.indexedDB = new MeerkatDatabase();
    this.instanceManager = instanceManager;

    this.logger = logger;
    this.onEvent = onEvent;
  }

  async bulkRegisterJSON(jsonData: FileJsonStore[]): Promise<void> {
    const fileBuffers = await Promise.all(
      jsonData.map(async (jsonFile) => {
        const { json, tableName, ...fileData } = jsonFile;

        const bufferData = await getBufferFromJSON({
          instanceManager: this.instanceManager,
          json: json,
          tableName,
          logger: this.logger,
          onEvent: this.onEvent,
          metadata: jsonFile.metadata,
        });

        return { buffer: bufferData, tableName, ...fileData };
      })
    );

    await this.bulkRegisterFileBuffer(fileBuffers);
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
     * Register the buffer in the file manager
     */
    await this.registerFileBuffer({
      buffer: bufferData,
      tableName,
      ...fileData,
    });
  }

  async getFilesNameForTables(tables: TableConfig[]): Promise<Table[]> {
    const results = await Promise.all(
      tables.map(async (table) => {
        const tableData = await this.getTableData(table);

        return tableData;
      })
    );

    return results.filter(isDefined);
  }

  async getTableData(table: TableConfig): Promise<Table | undefined> {
    const tableData = await this.indexedDB.tablesKey.get(table.name);

    if (!tableData) return undefined;

    return {
      ...tableData,
      files: getFilesByPartition(tableData?.files ?? [], table.partitions),
    };
  }

  async setTableMetadata(tableName: string, metadata: object): Promise<void> {
    await this.indexedDB.tablesKey.update(tableName, {
      metadata,
    });
  }

  abstract dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void>;

  abstract bulkRegisterFileBuffer(
    fileBuffers: FileBufferStore[]
  ): Promise<void>;

  abstract registerFileBuffer(fileBuffer: FileBufferStore): Promise<void>;

  abstract mountFileBufferByTables(tables: TableConfig[]): Promise<void>;

  abstract onDBShutdownHandler(): Promise<void>;
}
