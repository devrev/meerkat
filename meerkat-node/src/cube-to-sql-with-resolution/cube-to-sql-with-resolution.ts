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
  columnProjections?: string[];
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

/**
 * Helper function to get array-type columns from resolution config
 */
const getArrayTypeColumns = (resolutionConfig: ResolutionConfig) => {
  return resolutionConfig.columnConfigs.filter(
    (config) => config.isArrayType === true
  );
};

export const cubeQueryToSQLWithResolutionWithArray = async ({
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: CubeQueryToSQLWithResolutionParams) => {
  debugger;
  if (resolutionConfig.columnConfigs.length === 0) {
    // If no resolution is needed, return the base SQL.
    const baseSql = await cubeQueryToSQL({
      query,
      tableSchemas,
      contextParams,
    });
    return baseSql;
  }

  // Phase 1: Setup and Unnest
  const arrayColumns = getArrayTypeColumns(resolutionConfig);

  if (arrayColumns.length === 0) {
    // No array columns to process, return base SQL
    const baseSql = await cubeQueryToSQL({
      query,
      tableSchemas,
      contextParams,
    });
    return baseSql;
  }

  // Step 1: Add row_id to the first table schema and generate base SQL (without unnesting)
  const modifiedTableSchemasWithRowId = tableSchemas.map((schema, index) => {
    // Add row_id to the first table only
    if (index !== 0) {
      return schema;
    }

    // Add row_id dimension (no unnest modifier yet)
    // TODO: Will this cause a problem of adding row_id to the first schema ?
    const newDimensions = [
      {
        name: 'row_id',
        sql: 'row_number() OVER ()',
        type: 'number' as const,
        alias: '__row_id',
      },
      ...schema.dimensions,
    ];

    return {
      ...schema,
      dimensions: newDimensions,
    };
  });

  // Use the first table for row_id reference
  const firstTable = tableSchemas[0];

  const queryWithRowId: Query = {
    measures: query.measures,
    dimensions: [`${firstTable.name}.row_id`, ...(query.dimensions || [])],
    joinPaths: query.joinPaths,
    filters: query.filters,
    order: query.order,
    limit: query.limit,
    offset: query.offset,
  };

  // Generate base SQL with row_id
  const baseSqlWithRowId = await cubeQueryToSQL({
    query: queryWithRowId,
    tableSchemas: modifiedTableSchemasWithRowId,
    contextParams,
  });

  // Step 2: Create a new table schema from the base SQL with row_id
  // This will be used to apply unnesting
  const baseTableName = '__base_query_with_row_id';

  // Create dimensions from the base SQL output (all measures become dimensions now)
  const baseDimensions: TableSchema['dimensions'] = [
    {
      name: 'row_id',
      sql: `${baseTableName}.__row_id`,
      type: 'number',
    },
    ...query.measures.map((measure) => ({
      name: measure.replace('.', '__'),
      sql: `${baseTableName}.${measure.replace('.', '__')}`,
      type: 'number' as const,
    })),
    ...(query.dimensions || []).map((dimension) => {
      // Check if this dimension is an array that needs unnesting
      const isArrayDimension = arrayColumns.some(
        (col) => col.name === dimension
      );

      const originalSchema = tableSchemas.find((s) =>
        dimension.startsWith(`${s.name}.`)
      );
      const dimName = dimension.split('.')[1];
      const originalDimension = originalSchema?.dimensions.find(
        (d) => d.name === dimName
      );

      return {
        name: dimension.replace('.', '__'),
        sql: `${baseTableName}.${dimension.replace('.', '__')}`,
        type: originalDimension?.type || ('string' as const),
        ...(isArrayDimension && {
          modifier: {
            shouldUnnestArray: true,
          },
        }),
      };
    }),
  ];

  const baseTableSchema: TableSchema = {
    name: baseTableName,
    sql: baseSqlWithRowId,
    measures: [],
    dimensions: baseDimensions,
  };

  // Step 3: Create query with unnest modifiers applied
  const unnestQuery: Query = {
    measures: [],
    dimensions: [
      `${baseTableName}.row_id`,
      ...query.measures.map((m) => `${baseTableName}.${m.replace('.', '__')}`),
      ...(query.dimensions || []).map(
        (d) => `${baseTableName}.${d.replace('.', '__')}`
      ),
    ],
    filters: query.filters,
    order: query.order,
    limit: query.limit,
    offset: query.offset,
  };

  // Generate the final SQL with unnesting applied
  const unnestedBaseSql = await cubeQueryToSQL({
    query: unnestQuery,
    tableSchemas: [baseTableSchema],
    contextParams,
  });

  return unnestedBaseSql;
};
