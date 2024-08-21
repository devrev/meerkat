import { FileManagerType } from '../../file-manager/file-manager-type';
import { DBMEvent, DBMLogger } from '../../logger';
import { DBMConstructorOptions, QueryOptions, TableConfig } from '../types';
import { InstanceManagerType } from './../instance-manager';

export class DBMParallel {
  private fileManager: FileManagerType;
  private instanceManager: InstanceManagerType;
  private logger: DBMLogger;
  private onEvent?: (event: DBMEvent) => void;
  private options: DBMConstructorOptions['options'];
  private onDuckDBShutdown?: () => void;

  constructor({
    fileManager,
    logger,
    onEvent,
    options,
    instanceManager,
    onDuckDBShutdown,
  }: DBMConstructorOptions) {
    this.fileManager = fileManager;
    this.logger = logger;
    this.onEvent = onEvent;
    this.options = options;
    this.instanceManager = instanceManager;
    this.onDuckDBShutdown = onDuckDBShutdown;
  }

  public async queryWithTables({
    query,
    tables,
    options,
  }: {
    query: string;
    tables: TableConfig[];
    options?: QueryOptions;
  }) {
    const db = await this.instanceManager.getDB();
    const connection = await db.connect();
    return connection.query(query);
  }

  public async query(query: string) {
    //no-ops
  }
}
