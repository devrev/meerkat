import { Table } from '../file-manager/file-manager-type';
import { mergeFileBufferStoreIntoTable } from './merge-file-buffer-store-into-table';

describe('mergeFileBufferStoreIntoTable', () => {
  const fileBufferStore = {
    tableName: 'taxi1',
    fileName: 'taxi1.parquet',
    buffer: new Uint8Array([]),
  };

  const fileBufferStores = [
    {
      tableName: 'taxi1',
      fileName: 'taxi2.parquet',
      buffer: new Uint8Array([]),
    },
    {
      tableName: 'taxi2',
      fileName: 'taxi1.parquet',
      buffer: new Uint8Array([]),
    },
  ];

  const metadata = { size: 100 };

  it('should add files in new tables', () => {
    const currentTableState: Table[] = [];

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      fileBufferStores,
      currentTableState
    );

    // Verify that the taxi1 table has the new file added
    expect(updatedTableMap.get('taxi1')).toEqual({
      tableName: 'taxi1',
      files: [{ fileName: 'taxi2.parquet' }],
    });

    // Verify that the taxi2 table has the new file added
    expect(updatedTableMap.get('taxi2')).toEqual({
      tableName: 'taxi2',
      files: [{ fileName: 'taxi1.parquet' }],
    });
  });

  it('should append a file if the table already exists', () => {
    const currentTableState = [
      {
        tableName: fileBufferStore.tableName,
        files: [{ fileName: fileBufferStore.fileName }],
        metadata,
      },
    ];

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      [fileBufferStores[0]],
      currentTableState
    );

    // Verify that the taxi1 table has two file
    expect(updatedTableMap.get('taxi1')).toEqual({
      tableName: 'taxi1',
      files: [{ fileName: 'taxi1.parquet' }, { fileName: 'taxi2.parquet' }],
      metadata,
    });
  });

  it('should not add file if the file already exists in a table', () => {
    const currentTableState = [
      {
        tableName: fileBufferStore.tableName,
        files: [{ fileName: fileBufferStore.fileName }],
        metadata,
      },
    ];

    const updatedTableMap = mergeFileBufferStoreIntoTable(
      [fileBufferStore],
      currentTableState
    );

    // Verify that the taxi1 table still has one file (taxi1.parquet)
    expect(updatedTableMap.get('taxi1')).toEqual({
      tableName: 'taxi1',
      files: [{ fileName: 'taxi1.parquet' }],
      metadata,
    });
  });
});
