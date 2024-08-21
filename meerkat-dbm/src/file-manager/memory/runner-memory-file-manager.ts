import { CommunicationInterface } from 'meerkat-dbm/src/window-communication/window-communication';
import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { Table, TableWiseFiles } from '../../types';
import { getBufferFromJSON } from '../../utils';
import {
  BROWSER_RUNNER_TYPE,
  BrowserRunnerMessage,
} from '../../window-communication/runner-types';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';

export class RunnerMemoryDBFileManager implements FileManagerType {
  fetchTableFileBuffers: (tableName: string) => Promise<FileBufferStore[]>;
  instanceManager: InstanceManagerType;
  communication: CommunicationInterface<BrowserRunnerMessage>;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    logger,
    onEvent,
    communication,
  }: FileManagerConstructorOptions & {
    communication: CommunicationInterface<BrowserRunnerMessage>;
  }) {
    this.fetchTableFileBuffers = fetchTableFileBuffers;
    this.instanceManager = instanceManager;
    this.logger = logger;
    this.onEvent = onEvent;
    this.communication = communication;
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
    const fileBuffer = await this.communication.sendRequest<{
      tableBuffers: FileBufferStore[];
    }>({
      type: BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS,
      payload: {
        tables: tables,
      },
    });

    await this.bulkRegisterFileBuffer(fileBuffer.message.tableBuffers);
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
