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
  // Phase 1: Generate SQL with row_id and unnested arrays
  const { sql: unnestBaseSql, baseTableSchema } = await getUnnestBaseSql({
    query,
    tableSchemas,
    resolutionConfig,
    columnProjections,
    contextParams,
  });

  // Get array columns for Phase 2
  const arrayColumns = getArrayTypeColumns(resolutionConfig);

  if (arrayColumns.length === 0) {
    // No resolution needed
    return unnestBaseSql;
  }

  // // // Phase 2: Apply resolution (join with lookup tables)
  const resolvedSql = await getResolvedSql({
    unnestedSql: unnestBaseSql,
    baseTableSchema,
    query,
    tableSchemas,
    resolutionConfig,
    contextParams,
    columnProjections,
  });

  // TODO: Phase 3 - Re-aggregate to reverse the unnest

  return resolvedSql;
};

export const getUnnestBaseSql = async ({
  query,
  tableSchemas,
  resolutionConfig,
  columnProjections,
  contextParams,
}: CubeQueryToSQLWithResolutionParams): Promise<{
  sql: string;
  baseTableSchema: TableSchema;
}> => {
  if (resolutionConfig.columnConfigs.length === 0) {
    // If no resolution is needed, return the base SQL.
    const baseSql = await cubeQueryToSQL({
      query,
      tableSchemas,
      contextParams,
    });
    // Return a dummy schema since we won't use it
    return {
      sql: baseSql,
      baseTableSchema: { name: '', sql: '', measures: [], dimensions: [] },
    };
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
    // Return a dummy schema since we won't use it
    return {
      sql: baseSql,
      baseTableSchema: { name: '', sql: '', measures: [], dimensions: [] },
    };
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
  unnestedSql,
  baseTableSchema,
  query,
  tableSchemas,
  resolutionConfig,
  contextParams,
  columnProjections,
}: {
  unnestedSql: string;
  baseTableSchema: TableSchema;
  query: Query;
  tableSchemas: TableSchema[];
  resolutionConfig: ResolutionConfig;
  contextParams?: ContextParams;
  columnProjections?: string[];
}): Promise<string> => {
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

  debugger;
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
    query: resolutionQuery,
    tableSchemas: [updatedBaseTableSchema, ...resolutionSchemas],
    contextParams,
  });

  return resolvedSql;
};
