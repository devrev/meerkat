import { applyFilterParamsToBaseSQL } from "../filter-params/filter-params-ast";
import { getFilterParamsSQL } from "../get-filter-params-sql/get-filter-params-sql";
import { getWrappedBaseQueryWithProjections } from "../get-wrapped-base-query-with-projections/get-wrapped-base-query-with-projections";
import { AliasConfig } from "../member-formatters/get-alias";
import { Query } from "../types/cube-types/query";
import { TableSchema } from "../types/cube-types/table";

export const getFinalBaseSQL = async ({
  query,
  getQueryOutput,
  tableSchema,
  config,
}: {
  query: Query;
  tableSchema: TableSchema;
  getQueryOutput: (query: string) => Promise<any>;
  config?: AliasConfig;
}) => {
  /**
   * Apply transformation to the supplied base query.
   * This involves updating the filter placeholder with the actual filter values.
   */
  const baseFilterParamsSQL = await getFilterParamsSQL({
    query: query,
    tableSchema,
    filterType: 'BASE_FILTER',
    getQueryOutput,
    config,
  });
  const baseSQL = applyFilterParamsToBaseSQL(
    tableSchema.sql,
    baseFilterParamsSQL
  );
  const baseSQLWithFilterProjection = getWrappedBaseQueryWithProjections({
    baseQuery: baseSQL,
    tableSchema,
    query: query,
    config,
  });
  return baseSQLWithFilterProjection;
};
  
