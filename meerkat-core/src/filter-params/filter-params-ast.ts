import { cubeToDuckdbAST } from '../ast-builder/ast-builder';
import {
  LogicalAndFilter,
  LogicalOrFilter,
  Query,
  QueryFilter,
  TableSchema,
} from '../types/cube-types';
import { SelectStatement } from '../types/duckdb-serialization-types/serialization/Statement';

/**
 * Get the query filter with only where filterKey matches
 */
type GenericFilter = QueryFilter | LogicalAndFilter | LogicalOrFilter;
const traverseAndFilter = (
  filter: GenericFilter,
  memberKey: string
): GenericFilter | null => {
  if ('member' in filter) {
    return filter.member === memberKey ? filter : null;
  }

  if ('and' in filter) {
    const filteredAndFilters = filter.and
      .map((subFilter) => traverseAndFilter(subFilter, memberKey))
      .filter(Boolean) as GenericFilter[];
    const obj =
      filteredAndFilters.length > 0
        ? { and: filteredAndFilters }
        : { and: filteredAndFilters };
    return obj as LogicalAndFilter;
  }

  if ('or' in filter) {
    const filteredOrFilters = filter.or
      .map((subFilter) => traverseAndFilter(subFilter, memberKey))
      .filter(Boolean);
    const obj = filteredOrFilters.length > 0 ? { or: filteredOrFilters } : null;
    return obj as LogicalOrFilter;
  }

  return null;
};

export const getFilterByMemberKey = (
  filters: GenericFilter[] | undefined,
  memberKey: string
): GenericFilter[] => {
  if (!filters) return [];
  return filters
    .map((filter) => traverseAndFilter(filter, memberKey))
    .filter(Boolean) as GenericFilter[];
};

/**
 * Syntax for filter params in SQL:
 * FILTER_PARAMS.cube_name.member_name.filter(sql_expression)
 * Example:
 *  SELECT *
 *  FROM orders
 *  WHERE ${FILTER_PARAMS.order_facts.date.filter('date')}
 * @param sql
 */
export const detectAllFilterParamsFromSQL = (
  sql: string
): {
  memberKey: string;
  filterExpression: string;
  matchKey: string;
}[] => {
  const regex = /\${FILTER_PARAMS\.(\w+\.\w+)\.filter\('(\w+)'\)}/g;
  const matches = [];
  let match;
  while ((match = regex.exec(sql)) !== null) {
    matches.push({
      memberKey: match[1],
      filterExpression: match[2],
      matchKey: match[0],
    });
  }
  return matches;
};

export const getFilterParamsAST = (
  query: Query,
  tableSchema: TableSchema
): {
  memberKey: string;
  ast: SelectStatement | null;
  matchKey: string;
}[] => {
  const filterParamKeys = detectAllFilterParamsFromSQL(tableSchema.sql);
  const filterParamsAST = [];

  for (const filterParamKey of filterParamKeys) {
    const filters = getFilterByMemberKey(
      query.filters,
      filterParamKey.memberKey
    );
    if (filters && filters.length > 0) {
      filterParamsAST.push({
        memberKey: filterParamKey.memberKey,
        matchKey: filterParamKey.matchKey,
        ast: cubeToDuckdbAST(
          { filters, measures: [], dimensions: [] },
          tableSchema
        ),
      });
    }
  }

  return filterParamsAST;
};

export const applyFilterParamsToBaseSQL = (
  baseSQL: string,
  filterParamsSQL: {
    memberKey: string;
    sql: string;
    matchKey: string;
  }[]
) => {
  let finalSQL = baseSQL;
  for (const filterParam of filterParamsSQL) {
    /**
     * Get SQL expression after WHERE clause
     */
    const whereClause = filterParam.sql.split('WHERE')[1];
    /**
     * Replace filter param with SQL expression
     */
    finalSQL = finalSQL.replace(filterParam.matchKey, whereClause);
  }
  /**
   * Find all remaining filter params and replace them with TRUE
   */
  const remainingFilterParams = detectAllFilterParamsFromSQL(finalSQL);
  for (const filterParam of remainingFilterParams) {
    finalSQL = finalSQL.replace(filterParam.matchKey, 'TRUE');
  }
  return finalSQL;
};
