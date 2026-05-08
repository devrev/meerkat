import {
  Query,
  TableSchema,
  ensureColumnAliasBatch,
  ensureTableSchemaAliasSql,
} from '@devrev/meerkat-core';
import { duckdbExec } from '../duckdb-exec';

export interface EnsureTableSchemasAliasParams {
  tableSchemas: TableSchema[];
  /**
   * Query whose member references drive which members are aliased. Members
   * that the query does not reach are passed through untouched.
   */
  query: Query;
}

export const ensureTableSchemasAlias = async ({
  tableSchemas,
  query,
}: EnsureTableSchemasAliasParams): Promise<TableSchema[]> => {
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
        executeQuery: (queryString) =>
          duckdbExec<Record<string, string>[]>(queryString),
      });

      return aliasedItems.map((item) => item.sql);
    },
  });
};

export const ensureTableSchemaAlias = () => {
  return async (
    tableSchemas: TableSchema[],
    query: Query
  ): Promise<TableSchema[]> => {
    return ensureTableSchemasAlias({ tableSchemas, query });
  };
};
