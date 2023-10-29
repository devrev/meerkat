import { cubeToDuckdbAST } from '../ast-builder/ast-builder';
import { Query, TableSchema } from '../types/cube-types';
import { SelectStatement } from '../types/duckdb-serialization-types/serialization/Statement';
import { isFilterArray, isLogicalAnd, isLogicalOr } from '../utils/type-guards';

type QueryFilters = Query['filters'];
/**
 * Get the query filter with only where filterKey matches
 */
export function getFilterByMemberKey(
  filters: QueryFilters,
  memberKey: string
): QueryFilters {
  if (!filters) return [];

  return filters.filter((filter) => {
    if ('member' in filter) {
      return filter.member === memberKey;
    }
    if (!isFilterArray(filter) && isLogicalAnd(filter)) {
      return filter.and.some(
        (tmpFilter) => 'member' in tmpFilter && tmpFilter.member === memberKey
      );
    }
    if (!isFilterArray(filter) && isLogicalOr(filter)) {
      return filter.or.some(
        (tmpFilter) => 'member' in tmpFilter && tmpFilter.member === memberKey
      );
    }
    return false;
  });
}

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
  return finalSQL;
};
