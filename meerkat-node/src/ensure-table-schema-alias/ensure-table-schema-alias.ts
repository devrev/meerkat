import {
  TableSchema,
  ensureColumnAliasBatch,
  ensureTableSchemaAliasSql,
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

export const ensureTableSchemasAlias = async (
  tableSchemas: TableSchema[]
): Promise<TableSchema[]> => {
  return ensureTableSchemaAliasSql({
    tableSchemas,
    ensureExpressionAlias: async ({ items }) => {
      const aliasedItems = await ensureColumnAliasBatch({
        items: items.map((item) => ({
          sql: item.sql,
          tableName: item.context.tableName,
          knownTableNames: item.context.knownTableNames,
        })),
        executeQuery: (query) =>
          duckdbExec<Record<string, string>[]>(query),
      });

      return aliasedItems.map((item) => item.sql);
    },
  });
};

export const ensureTableSchemaAlias = () => {
  return async (tableSchemas: TableSchema[]): Promise<TableSchema[]> => {
    return ensureTableSchemasAlias(tableSchemas);
  };
};
