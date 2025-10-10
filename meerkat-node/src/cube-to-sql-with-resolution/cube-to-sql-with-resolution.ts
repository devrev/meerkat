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
import {
  cubeQueryToSQL,
  CubeQueryToSQLParams,
} from '../cube-to-sql/cube-to-sql';

export interface CubeQueryToSQLWithResolutionParams {
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  columnProjections: string[];
  contextParams?: ContextParams;
}

export const cubeQueryToSQLWithResolution = async ({
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: CubeQueryToSQLWithResolutionParams) => {
  const baseSql = await cubeQueryToSQL({
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
    query: {
      measures: [],
      dimensions: generateResolvedDimensions(
        query,
        resolutionConfig,
        columnProjections
      ),
      joinPaths: generateResolutionJoinPaths(resolutionConfig, tableSchemas),
    },
    tableSchemas: [baseTable, ...resolutionSchemas],
  };
  const sql = await cubeQueryToSQL(resolveParams);

  return sql;
};
