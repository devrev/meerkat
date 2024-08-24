import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table, TableWiseFiles } from '../../types';
import { getBufferFromJSON } from '../../utils';
import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerMessage,
} from '../../window-communication/runner-types';
import { CommunicationInterface } from '../../window-communication/window-communication';
import {
  BaseFileStore,
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
export class RunnerMemoryDBFileManager implements FileManagerType {
  private instanceManager: InstanceManagerType;
  private communication: CommunicationInterface<BrowserRunnerMessage>;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;
  private mountedTables: Set<string> = new Set();

  constructor({
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
    console.info('before registerFileBuffer', props);

    const instanceManager = await this.instanceManager.getDB();
    // get ?uuid= from url
    const url = new URL(window.location.href);
    const uuid = url.searchParams.get('uuid');

    instanceManager.registerFileBuffer(props.fileName, props.buffer);
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

  getFileBuffer(name: string): Promise<Uint8Array> {
    throw new Error('Method not implemented.');
  }

  async mountFileBufferByTables(tables: TableConfig[]): Promise<void> {
    const tablesToBeRegistered = tables.filter(
      (table) => !this.mountedTables.has(table.name)
    );

    // Return there are no tables to register
    if (tablesToBeRegistered.length === 0) return;

    // Fetch file buffers for the tables to be registered
    const fileBuffersResponse = await this.communication.sendRequest<
      (BaseFileStore & {
        buffer: SharedArrayBuffer;
      })[]
    >({
      type: BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS,
      payload: {
        tables: tables,
      },
    });

    const tableBuffers = fileBuffersResponse.message;

    //Create a deep copy of the buffers
    const tableBuffersCopy = tableBuffers.map((buffer) => {
      const newBuffer = new Uint8Array(buffer.buffer);
      return {
        ...buffer,
        buffer: newBuffer, // Create a new Uint8Array with a copy of the buffer
      };
    });

    // Register the file buffers
    await this.bulkRegisterFileBuffer(tableBuffersCopy);

    tablesToBeRegistered.forEach((table) => this.mountedTables.add(table.name));
  }

  async getFilesNameForTables(
    tables: TableConfig[]
  ): Promise<TableWiseFiles[]> {
    // not needed for memory file manager
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
