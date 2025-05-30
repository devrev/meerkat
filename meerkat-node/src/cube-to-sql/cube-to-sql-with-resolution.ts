import {
  ContextParams,
  Member,
  Query,
  splitIntoDataSourceAndFields,
  TableSchema,
} from '@devrev/meerkat-core';
import { cubeQueryToSQL, CubeQueryToSQLParams } from './cube-to-sql';

interface ResolutionColumnConfig {
  // Name of the column that needs resolution.
  // Should match a measure or dimension in the query.
  name: string;
  // Name of the data source to use for resolution.
  source: string;
  // Columns from the source table that should be included for resolution.
  resolutionColumns: string[];
}

interface ResolutionConfig {
  columnConfigs: ResolutionColumnConfig[];
  tableSchemas: TableSchema[];
}

interface CubeQueryToSQLWithResolutionParams {
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}

const BASE_DATA_SOURCE_NAME = '__base_query';

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
      sql: `${BASE_DATA_SOURCE_NAME}.${config.name.replace(
        '.',
        '__'
      )} = ${config.name.replace('.', '__')}.id`,
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
          right: config.name.replace('.', '__'),
          on: config.name.replace('.', '__'),
        },
      ]),
    },
    tableSchemas: [baseTable, ...resolutionSchemas],
  };
  const sql = await cubeQueryToSQL(resolveParams);

  return sql;
};

const resolveDimension = (dim: string, tableSchemas: TableSchema[]) => {
  const [tableName, columnName] = splitIntoDataSourceAndFields(dim);
  const tableSchema = tableSchemas.find((ts) => ts.name === tableName);
  if (!tableSchema) {
    throw new Error(`Table schema not found for ${tableName}`);
  }
  const dimension = tableSchema.dimensions.find((d) => d.name === columnName);
  if (!dimension) {
    throw new Error(`Dimension not found: ${dim}`);
  }

  return {
    name: `${tableName}__${columnName}`,
    sql: `${BASE_DATA_SOURCE_NAME}.${tableName}__${columnName}`,
    type: dimension.type,
  };
};

const resolveMeasure = (meas: string, tableSchemas: TableSchema[]) => {
  const [tableName, columnName] = splitIntoDataSourceAndFields(meas);
  const tableSchema = tableSchemas.find((ts) => ts.name === tableName);
  if (!tableSchema) {
    throw new Error(`Table schema not found for ${tableName}`);
  }
  const measure = tableSchema.measures.find((m) => m.name === columnName);
  if (!measure) {
    throw new Error(`Measure not found: ${meas}`);
  }

  return {
    name: `${tableName}__${columnName}`,
    sql: `${BASE_DATA_SOURCE_NAME}.${tableName}__${columnName}`,
    type: measure.type,
  };
};

const generateResolutionSchemas = (config: ResolutionConfig) => {
  const resolutionSchemas: TableSchema[] = [];
  config.columnConfigs.forEach((colConfig) => {
    const { source, resolutionColumns } = colConfig;
    const tableSchema = config.tableSchemas.find((ts) => ts.name === source);
    if (!tableSchema) {
      throw new Error(`Table schema not found for ${source}`);
    }

    const baseName = colConfig.name.replace('.', '__');

    // For each column that needs to be resolved, create a copy of the relevant table schema.
    const resolutionSchema: TableSchema = {
      name: baseName,
      sql: tableSchema.sql,
      measures: [],
      dimensions: resolutionColumns.map((col) => {
        const dimension = tableSchema.dimensions.find((d) => d.name === col);
        if (!dimension) {
          throw new Error(`Dimension not found: ${col}`);
        }
        return {
          name: col,
          sql: `${baseName}.${col}`,
          type: dimension.type,
        };
      }),
    };

    resolutionSchemas.push(resolutionSchema);
  });

  return resolutionSchemas;
};

const generateResolvedDimensions = (
  query: Query,
  config: ResolutionConfig
): Member[] => {
  const resolvedDimensions: Member[] = [];
  [...query.measures, ...(query.dimensions || [])].forEach((measure) => {
    const resolution = config.columnConfigs.find((c) => c.name === measure);

    const [tableName, columnName] = splitIntoDataSourceAndFields(measure);
    if (!resolution) {
      resolvedDimensions.push(
        `${BASE_DATA_SOURCE_NAME}.${tableName}__${columnName}`
      );
    } else {
      resolution.resolutionColumns.forEach((col) => {
        resolvedDimensions.push(`${tableName}__${columnName}.${col}`);
      });
    }
  });
  return resolvedDimensions;
};
