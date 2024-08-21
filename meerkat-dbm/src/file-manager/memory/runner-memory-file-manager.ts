import { InstanceManagerType } from '../../dbm/instance-manager';
import { TableConfig } from '../../dbm/types';
import { DBMEvent, DBMLogger } from '../../logger';
import { File, Table, TableWiseFiles } from '../../types';
import { getBufferFromJSON } from '../../utils';
import { BrowserRunnerMessage } from '../../window-communication/runner-types';
import { CommunicationInterface } from '../../window-communication/window-communication';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
} from '../file-manager-type';
export class RunnerMemoryDBFileManager implements FileManagerType {
  private fetchTableFileBuffers: (
    tableName: string
  ) => Promise<FileBufferStore[]>;
  private instanceManager: InstanceManagerType;
  private communication: CommunicationInterface<BrowserRunnerMessage>;

  private logger?: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;

  private tableFileBuffersMap: Map<string, File[]> = new Map();

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
    await Promise.all(promiseArr);
  }

  async registerFileBuffer(props: FileBufferStore): Promise<void> {
    console.info('registerFileBuffer', props);

    const files = this.tableFileBuffersMap.get(props.fileName) || [];

    files.push(props);

    this.tableFileBuffersMap.set(props.fileName, files);
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
    // const fileBuffer = await this.communication.sendRequest<{
    //   tableBuffers: FileBufferStore[];
    // }>({
    //   type: BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS,
    //   payload: {
    //     tables: tables,
    //   },
    // });

    // await this.bulkRegisterFileBuffer(fileBuffer.message.tableBuffers);
    const instanceManager = await this.instanceManager.getDB();

    const promises = tables.map(async (table) => {
      // Retrieve file names for the specified table
      const _filesList = this.tableFileBuffersMap.get(table.name) ?? [];

      await Promise.all(
        _filesList.map(async (file) => {
          console.log(file.buffer);
          await instanceManager.registerFileBuffer(file.fileName, file.buffer);
        })
      );
    });

    await Promise.all(promises);
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
