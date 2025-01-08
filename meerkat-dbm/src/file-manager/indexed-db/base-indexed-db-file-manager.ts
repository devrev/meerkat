import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { Table, TableWiseFiles } from '../../types';
import { isDefined } from '../../utils';
import {
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
import { getFilesByPartition } from '../partition';
import { MeerkatDatabase } from './meerkat-database';

export abstract class BaseIndexedDBFileManager<T>
  implements FileManagerType<T>
{
  protected indexedDB: MeerkatDatabase; // IndexedDB instance
  protected instanceManager: InstanceManagerType;

  constructor({ instanceManager }: FileManagerConstructorOptions) {
    this.indexedDB = new MeerkatDatabase();
    this.instanceManager = instanceManager;
  }

  async getFilesNameForTables(
    tables: TableConfig[]
  ): Promise<TableWiseFiles[]> {
    const tableNames = tables.map((table) => table.name);

    const tableData = (await this.indexedDB.tablesKey.bulkGet(tableNames))
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

  abstract bulkRegisterFileBuffer(fileBuffers: T[]): Promise<void>;

  abstract registerFileBuffer(fileBuffer: T): Promise<void>;

  abstract bulkRegisterJSON(jsonData: FileJsonStore[]): Promise<void>;

  abstract registerJSON(jsonData: FileJsonStore): Promise<void>;

  abstract mountFileBufferByTables(tables: TableConfig[]): Promise<void>;

  abstract onDBShutdownHandler(): Promise<void>;
}
