import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import {
  GetQueryOutput,
  TableSchema,
  ensureColumnAliasBatch,
  ensureTableSchemaAliasSql,
} from '@devrev/meerkat-core';

const getQueryOutput = async (
  query: string,
  connection: AsyncDuckDBConnection
) => {
  const queryOutput = await connection.query(query);
  return queryOutput.toArray().map((row) => row.toJSON() as Record<string, string>);
};

export interface EnsureTableSchemasAliasParams {
  connection: AsyncDuckDBConnection;
  tableSchemas: TableSchema[];
}

export const ensureTableSchemasAlias = async ({
  connection,
  tableSchemas,
}: EnsureTableSchemasAliasParams): Promise<TableSchema[]> => {
  const executeQuery: GetQueryOutput = (query) =>
    getQueryOutput(query, connection);

  return ensureTableSchemaAliasSql({
    tableSchemas,
    ensureExpressionAlias: async ({ items }) => {
      const aliasedItems = await ensureColumnAliasBatch({
        items: items.map((item) => ({
          sql: item.sql,
          tableName: item.context.tableName,
        })),
        executeQuery,
      });

      return aliasedItems.map((item) => item.sql);
    },
  });
};

export const ensureTableSchemaAlias = (
  connection: AsyncDuckDBConnection
) => {
  return async (tableSchemas: TableSchema[]): Promise<TableSchema[]> => {
    return ensureTableSchemasAlias({
      connection,
      tableSchemas,
    });
  };
};
