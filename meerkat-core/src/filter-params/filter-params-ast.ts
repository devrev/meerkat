import { cubeToDuckdbAST } from '../ast-builder/ast-builder';
import { AliasConfig } from '../member-formatters/get-alias';
import {
  FilterType,
  LogicalAndFilter,
  LogicalOrFilter,
  MeerkatQueryFilter,
  Query,
  QueryFilter,
  TableSchema,
} from '../types/cube-types';
import { SelectStatement } from '../types/duckdb-serialization-types/serialization/Statement';

/*
 ** This function traverse the MeerkatQueryFilter JSON, and calls the callback for each leaf type.
 ** This way we need no rewrite the traversal logic again and again.
 */
export const traverseMeerkatQueryFilter = (
  filters: MeerkatQueryFilter[],
  callback: (value: QueryFilter) => void
) => {
  filters.forEach((filter: MeerkatQueryFilter) => {
    if ('member' in filter) {
      callback(filter);
      return;
    }
    if ('and' in filter) {
      filter.and.forEach((subFilter: MeerkatQueryFilter) =>
        traverseMeerkatQueryFilter([subFilter], callback)
      );
    }
    if ('or' in filter) {
      filter.or.forEach((subFilter: MeerkatQueryFilter) =>
        traverseMeerkatQueryFilter([subFilter], callback)
      );
    }
  });
};

/**
 * Get the query filter with only where filterKey matches
 */
export const traverseAndFilter = (
  filter: MeerkatQueryFilter,
  callback: (value: QueryFilter) => boolean
): MeerkatQueryFilter | null => {
  if ('member' in filter) {
    return callback(filter) ? filter : null;
  }

  if ('and' in filter) {
    const filteredAndFilters = filter.and
      .map((subFilter) => traverseAndFilter(subFilter, callback))
      .filter(Boolean) as MeerkatQueryFilter[];
    const obj =
      filteredAndFilters.length > 0 ? { and: filteredAndFilters } : null;
    return obj as LogicalAndFilter;
  }

  if ('or' in filter) {
    const filteredOrFilters = filter.or
      .map((subFilter) => traverseAndFilter(subFilter, callback))
      .filter(Boolean);
    const obj = filteredOrFilters.length > 0 ? { or: filteredOrFilters } : null;
    return obj as LogicalOrFilter;
  }

  return null;
};

export const getFilterByMemberKey = (
  filters: MeerkatQueryFilter[] | undefined,
  memberKey: string
): MeerkatQueryFilter[] => {
  if (!filters) return [];
  return filters
    .map((filter) =>
      traverseAndFilter(filter, (value) => value.member === memberKey)
    )
    .filter(Boolean) as MeerkatQueryFilter[];
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
  tableSchema: TableSchema,
  filterType: FilterType = 'PROJECTION_FILTER',
  config?: AliasConfig
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
          tableSchema,
          {
            filterType,
            config,
          }
        ),
      });
    }
  }

  return filterParamsAST;
};

type FilterParamsSQL = {
  memberKey: string;
  sql: string;
  matchKey: string;
};

const replaceWhereClauseWithFiltersParamsSQL = (
  baseSQL: string,
  filterParamsSQL: FilterParamsSQL[]
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

export const applyFilterParamsToBaseSQL = (
  baseSQL: string,
  filterParamsSQL: FilterParamsSQL[]
) => {
  let finalSQL = replaceWhereClauseWithFiltersParamsSQL(
    baseSQL,
    filterParamsSQL
  );
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
