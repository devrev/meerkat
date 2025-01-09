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

  async query(query: string): Promise<Record<string, unknown>> {
    /**
     * Execute the query
     */
    const result = await this.nativeManager.query(query);

    console.log('result', result);

    return result;
  }
}
