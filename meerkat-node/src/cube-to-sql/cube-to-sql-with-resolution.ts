import { TableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQL, CubeQueryToSQLParams } from './cube-to-sql';
import {
  BASE_DATA_SOURCE_NAME,
  CubeQueryToSQLWithResolutionParams,
} from './types';
import {
  generateName,
  generateResolutionSchemas,
  generateResolvedDimensions,
  resolveDimension,
  resolveMeasure,
} from './utils';

export const cubeQueryToSQLWithResolution = async ({
  query,
  tableSchemas,
  resolutionConfig,
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
  const baseTable: TableSchema = {
    name: BASE_DATA_SOURCE_NAME,
    sql: baseSql,
    measures: [],
    dimensions: [
      ...(query.dimensions || []).map((dim) =>
        resolveDimension(dim, tableSchemas)
      ),
      ...(query.measures || []).map((meas) =>
        resolveMeasure(meas, tableSchemas)
      ),
    ],
    joins: resolutionConfig.columnConfigs.map((config) => ({
      sql: `${BASE_DATA_SOURCE_NAME}.${generateName(
        config.name
      )} = ${generateName(config.name)}.${config.joinColumn}`,
    })),
  };

  const resolutionSchemas: TableSchema[] =
    generateResolutionSchemas(resolutionConfig);

  const resolveParams: CubeQueryToSQLParams = {
    query: {
      measures: [],
      dimensions: generateResolvedDimensions(query, resolutionConfig),
      joinPaths: resolutionConfig.columnConfigs.map((config) => [
        {
          left: BASE_DATA_SOURCE_NAME,
          right: generateName(config.name),
          on: generateName(config.name),
        },
      ]),
    },
    tableSchemas: [baseTable, ...resolutionSchemas],
  };
  const sql = await cubeQueryToSQL(resolveParams);

  return sql;
};
