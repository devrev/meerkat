import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table, TableWiseFiles } from '../../types';
import {
  convertSharedArrayBufferToUint8Array,
  getBufferFromJSON,
} from '../../utils';
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
    const instanceManager = await this.instanceManager.getDB();

    await instanceManager.registerFileBuffer(props.fileName, props.buffer);
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
    const tablesToBeMounted = tables.filter(
      (table) => !this.mountedTables.has(table.name)
    );

    // Return there are no tables to register
    if (tablesToBeMounted.length === 0) return;

    const start = performance.now();

    /**
     * Get the file buffers for the tables from the main app
     */
    const fileBuffersResponse = await this.communication.sendRequest<
      (BaseFileStore & {
        buffer: SharedArrayBuffer;
      })[]
    >({
      type: BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS,
      payload: {
        tables: tablesToBeMounted,
      },
    });

    const tableSharedBuffers = fileBuffersResponse.message;

    //Copy the buffer to its own memory
    const tableBuffers = tableSharedBuffers.map((tableBuffer) => {
      // Create a new Uint8Array with the same length
      const newBuffer = convertSharedArrayBufferToUint8Array(
        tableBuffer.buffer
      );

      return {
        ...tableBuffer,
        buffer: newBuffer,
      };
    });

    const end = performance.now();
    this.onEvent?.({
      event_name: 'clone_buffer_duration',
      duration: end - start,
    });

    // Register the file buffers
    await this.bulkRegisterFileBuffer(tableBuffers);

    // Add the tables to the mounted tables
    tablesToBeMounted.forEach((table) => this.mountedTables.add(table.name));
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
