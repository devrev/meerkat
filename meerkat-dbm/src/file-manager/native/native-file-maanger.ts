import { NativeBridge } from '../../dbm/dbm-native/bridge';
import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table, TableWiseFiles } from '../../types';
import {
  getBufferFromJSON,
  isDefined,
  mergeFileBufferStoreIntoTable,
} from '../../utils';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { getFilesByPartition } from '../partition';

export class NativeBridgeFileManager implements FileManagerType {
  private nativeManager: NativeBridge;
  private instanceManager: InstanceManagerType;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    logger,
    onEvent,
    nativeManager,
  }: FileManagerConstructorOptions & { nativeManager: NativeBridge }) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.nativeManager = nativeManager;
    this.instanceManager = instanceManager;
    this.logger = logger;
    this.onEvent = onEvent;
  }

  async bulkRegisterFileBuffer(fileBuffers: FileBufferStore[]): Promise<void> {
    const tableNames = Array.from(
      new Set(fileBuffers.map((fileBuffer) => fileBuffer.tableName))
    );

    const currentTableData = await this.nativeManager.getTableData({
      tables: tableNames,
    });

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      fileBuffers,
      currentTableData
    );

    const newFilesData = fileBuffers.map((fileBuffer) => {
      return {
        buffer: fileBuffer.buffer,
        fileName: fileBuffer.fileName,
        tableName: fileBuffer.tableName,
      };
    });

    // Update the tables and files table in IndexedDB
    await this.nativeManager.registerFiles({
      files: newFilesData,
    });
  }

  async registerFileBuffer(fileBuffer: FileBufferStore): Promise<void> {
    const { buffer, fileName, tableName } = fileBuffer;

    const currentTableData = await this.nativeManager.getTableData({
      tables: [tableName],
    });

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      [fileBuffer],
      currentTableData
    );

    // Update the tables and files table in IndexedDB
    await this.indexedDB
      .transaction(
        'rw',
        this.indexedDB.tablesKey,
        this.indexedDB.files,
        async () => {
          await this.indexedDB.tablesKey.put({
            tableName: fileBuffer.tableName,
            files: updatedTableMap.get(tableName)?.files ?? [],
          });

          await this.indexedDB.files.put({ fileName, buffer });
        }
      )
      .catch((error) => {
        console.error(error);
      });
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

  async getFilesNameForTables(
    tables: TableConfig[]
  ): Promise<TableWiseFiles[]> {
    const tableNames = tables.map((table) => table.name);

    const tableData = (
      await this.nativeManager.getTableData({
        table: tableNames,
      })
    )

      .filter(isDefined)
      .reduce((tableObj, table) => {
        tableObj[table.tableName] = table;
        return tableObj;
      }, {} as { [key: string]: Table });

    return tables.map((table) => ({
      tableName: table.name,
      files: getFilesByPartition(
        tableData[table.name]?.files ?? [],
        table.partitions
      ),
    }));
  }

  async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    // no-op
  }

  async getTableData(table: TableConfig): Promise<Table | undefined> {
    const tableData = await this.nativeManager.getTableData({
      table: table.name,
    });

    if (!tableData) return undefined;

    return {
      ...tableData,
      files: getFilesByPartition(tableData?.files ?? [], table.partitions),
    };
  }

  async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    // Remove the files from the File System
    await this.nativeManager.dropFilesByTableName({
      table: tableName,
      fileNames,
    });
  }
}
