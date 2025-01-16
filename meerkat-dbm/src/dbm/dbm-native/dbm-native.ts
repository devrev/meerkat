import { DBM } from '../dbm';
import { DBMConstructorOptions } from '../types';
import { NativeBridge } from './native-bridge';

export class DBMNative extends DBM {
  private nativeBridge: NativeBridge;

  constructor({
    fileManager,
    logger,
    onEvent,
    options,
    instanceManager,
    onDuckDBShutdown,
    nativeBridge,
  }: DBMConstructorOptions & { nativeBridge: NativeBridge }) {
    super({
      fileManager,
      logger,
      onEvent,
      options,
      instanceManager,
      onDuckDBShutdown,
    });

    this.nativeBridge = nativeBridge;
  }

  override async query(query: string): Promise<any> {
    return this.nativeBridge.query(query);
  }
}
