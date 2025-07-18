import {
  ContextParams,
  createBaseTableSchema,
  generateResolutionJoinPaths,
  generateResolutionSchemas,
  generateResolvedDimensions,
  Query,
  ResolutionConfig,
  TableSchema,
} from '@devrev/meerkat-core';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import {
  cubeQueryToSQL,
  CubeQueryToSQLParams,
} from '../browser-cube-to-sql/browser-cube-to-sql';

export interface CubeQueryToSQLWithResolutionParams {
  connection: AsyncDuckDBConnection;
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}

export const cubeQueryToSQLWithResolution = async ({
  connection,
  query,
  tableSchemas,
  resolutionConfig,
  contextParams,
}: CubeQueryToSQLWithResolutionParams) => {
  const baseSql = await cubeQueryToSQL({
    connection,
    query,
    tableSchemas,
    contextParams,
  });

  if (resolutionConfig.columnConfigs.length === 0) {
    // If no resolution is needed, return the base SQL.
    return baseSql;
  }

  // Create a table schema for the base query.
  const baseTable: TableSchema = createBaseTableSchema(
    baseSql,
    tableSchemas,
    resolutionConfig,
    query.measures,
    query.dimensions
  );

  const resolutionSchemas: TableSchema[] = generateResolutionSchemas(
    resolutionConfig,
    tableSchemas
  );

  const resolveParams: CubeQueryToSQLParams = {
    connection: connection,
    query: {
      measures: [],
      dimensions: generateResolvedDimensions(query, resolutionConfig),
      joinPaths: generateResolutionJoinPaths(resolutionConfig, tableSchemas),
    },
    tableSchemas: [baseTable, ...resolutionSchemas],
  };
  const sql = await cubeQueryToSQL(resolveParams);

  return sql;
};
