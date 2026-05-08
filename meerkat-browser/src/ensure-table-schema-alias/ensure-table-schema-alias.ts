import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import {
  GetQueryOutput,
  Query,
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
  /**
   * Query whose member references drive which members are aliased. Members
   * that the query does not reach are passed through untouched.
   */
  query: Query;
}

export const ensureTableSchemasAlias = async ({
  connection,
  tableSchemas,
  query,
}: EnsureTableSchemasAliasParams): Promise<TableSchema[]> => {
  const executeQuery: GetQueryOutput = (queryString) =>
    getQueryOutput(queryString, connection);

  return ensureTableSchemaAliasSql({
    tableSchemas,
    query,
    ensureExpressionAlias: async ({ items }) => {
      const aliasedItems = await ensureColumnAliasBatch({
        items: items.map((item) => ({
          sql: item.sql,
          tableName: item.context.tableName,
          knownTableNames: item.context.knownTableNames,
        })),
        executeQuery,
      });

      return aliasedItems.map((item) => item.sql);
    },
  });
};

export const ensureTableSchemaAlias = (connection: AsyncDuckDBConnection) => {
  return async (
    tableSchemas: TableSchema[],
    query: Query
  ): Promise<TableSchema[]> => {
    return ensureTableSchemasAlias({
      connection,
      tableSchemas,
      query,
    });
  };
};
