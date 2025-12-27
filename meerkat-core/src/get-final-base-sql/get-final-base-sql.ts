import { applyFilterParamsToBaseSQL } from '../filter-params/filter-params-ast';
import { getFilterParamsSQL } from '../get-filter-params-sql/get-filter-params-sql';
import { getWrappedBaseQueryWithProjections } from '../get-wrapped-base-query-with-projections/get-wrapped-base-query-with-projections';
import { MeerkatQueryOptions, Query } from '../types/cube-types';
import { TableSchema } from '../types/cube-types/table';

export const getFinalBaseSQL = async ({
  query,
  getQueryOutput,
  tableSchema,
  options,
}: {
  query: Query;
  tableSchema: TableSchema;
  getQueryOutput: (query: string) => Promise<any>;
  options: MeerkatQueryOptions;
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
  });
  const baseSQL = applyFilterParamsToBaseSQL(
    tableSchema.sql,
    baseFilterParamsSQL
  );
  const baseSQLWithFilterProjection = getWrappedBaseQueryWithProjections({
    baseQuery: baseSQL,
    tableSchema,
    query: query,
    options,
  });
  return baseSQLWithFilterProjection;
};
