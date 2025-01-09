import { NativeBridge } from '../../dbm/dbm-native/native-bridge';
import { mergeFileStoreIntoTable } from '../../utils';
import {
  FileBufferStore,
  FileJsonStore,
  FileManagerConstructorOptions,
  FileManagerType,
  FileStore,
  FileUrlStore,
} from '../file-manager-type';
import { BaseIndexedDBFileManager } from '../indexed-db/base-indexed-db-file-manager';

export class NativeFileManager
  extends BaseIndexedDBFileManager
  implements FileManagerType
{
  private readonly nativeManager: NativeBridge;

  constructor({
    fetchTableFileBuffers,
    instanceManager,
    logger,
    onEvent,
    nativeManager,
  }: FileManagerConstructorOptions & { nativeManager: NativeBridge }) {
    super({
      fetchTableFileBuffers,
      instanceManager,
      logger,
      onEvent,
    });

    this.nativeManager = nativeManager;
  }

  private async updateIndexedDBWithTableData(
    files: FileStore[]
  ): Promise<void> {
    try {
      const tableNames = [...new Set(files.map((file) => file.tableName))];
      const currentTables = await this.indexedDB.tablesKey.toArray();
      const updatedTableMap = mergeFileStoreIntoTable(files, currentTables);

      const updates = tableNames.map((tableName) => ({
        tableName,
        files: updatedTableMap.get(tableName)?.files ?? [],
      }));

      await this.indexedDB.tablesKey.bulkPut(updates);
    } catch (error) {
      this.logger.error('Failed to update IndexedDB:', error);
      throw new Error('IndexedDB update failed');
    }
  }

  private async registerFiles<T extends FileStore>(
    files: T[],
    type: 'url' | 'buffer' | 'json'
  ): Promise<void> {
    await this.nativeManager.registerFiles(
      files.map((file) => ({ ...file, type }))
    );
    await this.updateIndexedDBWithTableData(files);
  }

  async bulkRegisterFileUrl(files: FileUrlStore[]): Promise<void> {
    await this.registerFiles(files, 'url');
  }

  async bulkRegisterJSON(files: FileJsonStore[]): Promise<void> {
    await this.registerFiles(files, 'json');
  }

  async registerFileUrl(file: FileUrlStore): Promise<void> {
    await this.registerFiles([file], 'url');
  }

  override async registerFileBuffer(file: FileBufferStore): Promise<void> {
    await this.registerFiles([file], 'buffer');
  }

  async bulkRegisterFileBuffer(): Promise<void> {
    // no-op
  }
  async registerJSON(): Promise<void> {
    // no-op
  }
  override async mountFileBufferByTables(): Promise<void> {
    // no-op
  }

  override async dropFilesByTableName(
    tableName: string,
    fileNames: string[]
  ): Promise<void> {
    try {
      const tableData = await this.indexedDB.tablesKey.get(tableName);

      if (tableData) {
        const updatedFiles = tableData.files.filter(
          (file) => !fileNames.includes(file.fileName)
        );

        await this.indexedDB.tablesKey.put({
          tableName,
          files: updatedFiles,
        });
      }

      await this.nativeManager.dropFilesByTableName({ tableName, fileNames });
    } catch (error) {
      this.logger.error('Failed to drop files:', error);
      throw new Error('Failed to drop files');
    }
  }

  override async onDBShutdownHandler(): Promise<void> {
    // no-op
  }
}
