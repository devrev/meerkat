import {
  ContextParams,
  createBaseTableSchema,
  Dimension,
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
  columnProjections?: string[];
  contextParams?: ContextParams;
}

/**
 * Helper function to get array-type columns from resolution config
 */
const getArrayTypeColumns = (resolutionConfig: ResolutionConfig) => {
  return resolutionConfig.columnConfigs.filter(
    (config) => config.isArrayType === true
  );
};

export const cubeQueryToSQLWithResolution = async ({
  connection,
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
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

export const cubeQueryToSQLWithResolutionWithArray = async ({
  connection,
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: CubeQueryToSQLWithResolutionParams) => {
  // Phase 1: Generate SQL with row_id and unnested arrays
  const { sql: unnestBaseSql, baseTableSchema } = await getUnnestBaseSql({
    connection,
    query,
    tableSchemas,
    resolutionConfig,
    contextParams,
  });

  // // // Phase 2: Apply resolution (join with lookup tables)
  const { sql: resolvedSql, resolvedTableSchema } = await getResolvedSql({
    connection,
    unnestedSql: unnestBaseSql,
    baseTableSchema,
    query,
    tableSchemas,
    resolutionConfig,
    contextParams,
    columnProjections,
  });

  // Phase 3: Re-aggregate to reverse the unnest
  const aggregatedSql = await getAggregatedSql({
    connection,
    resolvedSql,
    resolvedTableSchema,
    query,
    resolutionConfig,
    contextParams,
  });

  return aggregatedSql;
};

export const getUnnestBaseSql = async ({
  connection,
  query,
  tableSchemas,
  resolutionConfig,
  contextParams,
}: CubeQueryToSQLWithResolutionParams): Promise<{
  sql: string;
  baseTableSchema: TableSchema;
}> => {
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
    connection,
    query: queryWithRowId,
    tableSchemas: modifiedTableSchemasWithRowId,
    contextParams,
  });

  // Step 2: Create a new table schema from the base SQL with row_id
  // This will be used to apply unnesting
  const baseTableName = '__base_query'; // Use standard name to work with helpers

  const baseTableSchema: TableSchema = createBaseTableSchema(
    baseSqlWithRowId,
    tableSchemas,
    resolutionConfig,
    query.measures,
    query.dimensions
  );

  baseTableSchema.dimensions.push({
    name: 'row_id',
    sql: '__row_id',
    type: 'number',
    alias: '__row_id',
  } as Dimension);

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
  };

  // Generate the final SQL with unnesting applied
  const unnestedBaseSql = await cubeQueryToSQL({
    connection,
    query: unnestQuery,
    tableSchemas: [baseTableSchema],
    contextParams,
  });

  return {
    sql: unnestedBaseSql,
    baseTableSchema,
  };
};

/**
 * Phase 2: Apply resolution (join with lookup tables)
 *
 * This function:
 * 1. Uses the base table schema from Phase 1 (source of truth)
 * 2. Generates resolution schemas for array fields
 * 3. Sets up join paths between unnested data and resolution tables
 * 4. Generates SQL with resolved values
 *
 * @param unnestedSql - SQL output from Phase 1 (with row_id and unnested arrays)
 * @param baseTableSchema - Schema from Phase 1 that describes the unnested SQL
 * @returns SQL with row_id, unnested arrays, and resolved values from lookup tables
 */
export const getResolvedSql = async ({
  connection,
  unnestedSql,
  baseTableSchema,
  query,
  tableSchemas,
  resolutionConfig,
  contextParams,
  columnProjections,
}: {
  connection: AsyncDuckDBConnection;
  unnestedSql: string;
  baseTableSchema: TableSchema;
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
  columnProjections?: string[];
}): Promise<{
  sql: string;
  resolvedTableSchema: TableSchema;
}> => {
  // Step 1: Use the base table schema from Phase 1 as source of truth
  // Update the SQL to point to the unnested SQL
  const updatedBaseTableSchema: TableSchema = {
    ...baseTableSchema,
    sql: unnestedSql,
  };

  debugger;
  // Step 2: Generate resolution schemas for array fields only
  // Use the existing generateResolutionSchemas helper

  const resolutionSchemas = generateResolutionSchemas(
    resolutionConfig,
    tableSchemas
  );

  // Step 3: Generate join paths using existing helper
  // Note: Pass the base table schema (from Phase 1) to generate correct join paths
  const joinPaths = generateResolutionJoinPaths(resolutionConfig, tableSchemas);

  query.dimensions?.push('row_id');
  // Step 4: Generate resolved dimensions using existing helper
  const resolvedDimensions = generateResolvedDimensions(
    query,
    resolutionConfig,
    columnProjections
  );

  // Step 5: Create query and generate SQL
  const resolutionQuery: Query = {
    measures: [],
    dimensions: resolvedDimensions,
    joinPaths,
  };

  const resolvedSql = await cubeQueryToSQL({
    connection,
    query: resolutionQuery,
    tableSchemas: [updatedBaseTableSchema, ...resolutionSchemas],
    contextParams,
  });

  // Create a simple schema describing Phase 2's output columns
  // cubeQueryToSQL outputs columns using their alias field, not table-prefixed names
  const resolvedTableSchema: TableSchema = {
    name: '__resolved_query',
    sql: resolvedSql,
    measures: [],
    dimensions: [
      // Row ID - comes from baseTableSchema with alias '__row_id'
      {
        name: 'row_id',
        sql: '__resolved_query."__row_id"',
        type: 'number',
        alias: '__row_id',
      },
      // Original measures from base query - use their aliases
      ...query.measures.map((measure) => {
        const measureName = measure.replace('.', '__');
        return {
          name: measureName,
          sql: `__resolved_query."${measureName}"`,
          type: 'number' as const,
          alias: measureName,
        };
      }),
      // Non-array dimensions from base query - use their aliases
      ...(query.dimensions || [])
        .filter(
          (dim) =>
            !resolutionConfig.columnConfigs.some((ac) => ac.name === dim) &&
            dim !== 'row_id'
        )
        .map((dimension) => {
          const dimName = dimension.replace('.', '__');
          return {
            name: dimName,
            sql: `__resolved_query."${dimName}"`,
            type: 'string' as const,
            alias: dimName,
          };
        }),
      // ALL resolved columns (both array and scalar) - these use the alias pattern "colname - rescolname"
      ...resolutionConfig.columnConfigs.flatMap((columnConfig) => {
        return columnConfig.resolutionColumns.map((resCol) => {
          // Find the resolution dimension to get its alias
          const resolutionDimName = `${columnConfig.name.replace(
            '.',
            '__'
          )}__${resCol}`;
          const resolutionSchema = resolutionSchemas.find(
            (s) => s.name === columnConfig.name.replace('.', '__')
          );
          const resDim = resolutionSchema?.dimensions.find((d) =>
            d.name.includes(resCol)
          );

          // The alias from the resolution schema already has the full format
          const aliasName = resDim?.alias || resolutionDimName;

          return {
            name: resolutionDimName,
            sql: `__resolved_query."${aliasName}"`,
            type: 'string' as const,
            alias: aliasName,
          };
        });
      }),
    ],
  };

  return {
    sql: resolvedSql,
    resolvedTableSchema,
  };
};

/**
 * Phase 3: Re-aggregate to reverse the unnest
 *
 * This function:
 * 1. Wraps Phase 2 SQL as a new base table
 * 2. Groups by row_id
 * 3. Uses MAX for non-array columns (they're duplicated)
 * 4. Uses ARRAY_AGG for resolved array columns
 *
 * @param resolvedSql - SQL output from Phase 2 (with resolved values)
 * @param query - Original query
 * @param resolutionConfig - Resolution configuration
 * @param contextParams - Optional context parameters
 * @returns Final SQL with arrays containing resolved values
 */
export const getAggregatedSql = async ({
  connection,
  resolvedSql,
  resolvedTableSchema,
  query,
  resolutionConfig,
  contextParams,
}: {
  connection: AsyncDuckDBConnection;
  resolvedSql: string;
  resolvedTableSchema: TableSchema;
  query: Query;
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
}): Promise<string> => {
  // Step 1: Use the resolved table schema from Phase 2 as source of truth
  // Update the SQL to point to the resolved SQL
  const aggregationBaseTableSchema: TableSchema = {
    ...resolvedTableSchema,
    sql: resolvedSql,
  };

  // Step 2: Identify which columns need ARRAY_AGG vs MAX
  const arrayColumns = getArrayTypeColumns(resolutionConfig);
  const baseTableName = aggregationBaseTableSchema.name;

  // Helper to check if a dimension is a resolved array column
  const isResolvedArrayColumn = (dimName: string) => {
    // Check if the dimension name matches a resolved column pattern
    // e.g., "tickets__owners__display_name"
    return arrayColumns.some((arrayCol) => {
      const arrayColPrefix = arrayCol.name.replace('.', '__');
      return (
        dimName.includes(`${arrayColPrefix}__`) &&
        !dimName.startsWith('__base_query__')
      );
    });
  };

  // Step 3: Create aggregation measures with proper aggregation functions
  // Get row_id dimension for GROUP BY
  const rowIdDimension = aggregationBaseTableSchema.dimensions.find(
    (d) => d.name === 'row_id' || d.name.endsWith('__row_id')
  );

  // Create measures with MAX or ARRAY_AGG based on column type
  const aggregationMeasures: TableSchema['measures'] = [];

  aggregationBaseTableSchema.dimensions
    .filter((dim) => dim.name !== rowIdDimension?.name)
    .forEach((dim) => {
      const isArrayColumn = isResolvedArrayColumn(dim.name);

      // The dimension's sql field already has the correct reference (e.g., __resolved_query."__row_id")
      // We just need to wrap it in the aggregation function
      const columnRef =
        dim.sql || `${baseTableName}."${dim.alias || dim.name}"`;

      // Use ARRAY_AGG for resolved array columns, MAX for others
      const aggregationFn = isArrayColumn
        ? `ARRAY_AGG(DISTINCT ${columnRef})`
        : `MAX(${columnRef})`;

      aggregationMeasures.push({
        name: dim.name,
        sql: aggregationFn,
        type: dim.type,
        alias: dim.alias,
      });
    });

  // Update the schema with aggregation measures
  const schemaWithAggregation: TableSchema = {
    ...aggregationBaseTableSchema,
    measures: aggregationMeasures,
    dimensions: rowIdDimension ? [rowIdDimension] : [],
  };

  // Step 4: Create the aggregation query
  const aggregationQuery: Query = {
    measures: aggregationMeasures.map((m) => `${baseTableName}.${m.name}`),
    dimensions: rowIdDimension
      ? [`${baseTableName}.${rowIdDimension.name}`]
      : [],
  };

  // Step 5: Generate the final SQL
  const aggregatedSql = await cubeQueryToSQL({
    connection,
    query: aggregationQuery,
    tableSchemas: [schemaWithAggregation],
    contextParams,
  });

  const rowIdExcludedSql = `select * exclude(__row_id) from (${aggregatedSql})`;
  return rowIdExcludedSql;
};
