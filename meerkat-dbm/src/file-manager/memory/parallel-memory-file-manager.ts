import { Table, TableWiseFiles } from 'meerkat-dbm/src/types';
import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { getBufferFromJSON } from '../../utils';
import { BrowserRunnerMessage } from '../../window-communication/runner-types';
import { CommunicationInterface } from '../../window-communication/window-communication';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';

export class ParallelMemoryFileManager implements FileManagerType {
  private instanceManager: InstanceManagerType;
  private communication: CommunicationInterface<BrowserRunnerMessage>;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  private tableFileBuffersMap: Map<string, FileBufferStore[]> = new Map();

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    logger,
    onEvent,
    communication,
  }: FileManagerConstructorOptions & {
    communication: CommunicationInterface<BrowserRunnerMessage>;
  }) {
    this.instanceManager = instanceManager;
    this.logger = logger;
    this.onEvent = onEvent;
    this.communication = communication;
  }

  async bulkRegisterFileBuffer(props: FileBufferStore[]): Promise<void> {
    const promiseArr = props.map((fileBuffer) =>
      this.registerFileBuffer(fileBuffer)
    );
    await Promise.all(promiseArr);
  }

  async registerFileBuffer(props: FileBufferStore): Promise<void> {
    const existingFiles = this.tableFileBuffersMap.get(props.tableName) || [];

    existingFiles.push(props);

    this.tableFileBuffersMap.set(props.fileName, existingFiles);
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
    const tableBuffers = tables.flatMap((table) => {
      return this.tableFileBuffersMap.get(table.name) || [];
    });

    return { tableBuffers };
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
