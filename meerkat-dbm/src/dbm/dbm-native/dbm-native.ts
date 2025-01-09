import { DBM } from '../dbm';
import { DBMConstructorOptions } from '../types';
import { NativeBridge } from './native-bridge';

export class DBMNative extends DBM {
  private nativeManager: NativeBridge;

  constructor({
    fileManager,
    logger,
    onEvent,
    options,
    instanceManager,
    onDuckDBShutdown,
    nativeManager,
  }: DBMConstructorOptions & { nativeManager: NativeBridge }) {
    super({
      fileManager,
      logger,
      onEvent,
      options,
      instanceManager,
      onDuckDBShutdown,
    });

    this.nativeManager = nativeManager;
  }

  override async query(query: string): Promise<any> {
    return this.nativeManager.query(query);
  }
}
