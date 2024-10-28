import { getSelectReplacedSql } from '../cube-measure-transformer/cube-measure-transformer';
import { Query, TableSchema } from '../types/cube-types';
import { getAliasedColumnsFromFilters } from './get-aliased-columns-from-filters';
import {
  getProjectionClause
} from './get-projection-clause';

interface GetWrappedBaseQueryWithProjectionsParams {
  baseQuery: string;
  tableSchema: TableSchema;
  query: Query;
}

export const getWrappedBaseQueryWithProjections = ({
  baseQuery,
  tableSchema,
  query,
}: GetWrappedBaseQueryWithProjectionsParams) => {
  /*
   * Im order to be able to filter on computed metric from a query, we need to project the computed metric in the base query.
   * If theres filters supplied, we can safely return the original base query. Since nothing need to be projected and filtered in this case
   */
  // Wrap the query into another 'SELECT * FROM (baseQuery) AS baseTable'' in order to project everything in the base query, and other computed metrics to be able to filter on them
  const newBaseSql = `SELECT * FROM (${baseQuery}) AS ${tableSchema.name}`;
  const aliasedColumnSet = new Set<string>();

  const memberProjections = getProjectionClause(
    query,
    tableSchema,
    aliasedColumnSet
  );

  const aliasFromFilters = getAliasedColumnsFromFilters({
    aliasedColumnSet,
    baseSql: 'SELECT *',
    // setting measures to empty array, since we don't want to project measures present in the filters in the base query
    tableSchema: tableSchema,
    query,
    meerkatFilters: query.filters,
  });

  const formattedMemberProjection = memberProjections
    ? `, ${memberProjections}`
    : '';

  const finalAliasedColumnsClause =
    aliasFromFilters + formattedMemberProjection;

  const sqlWithFilterProjects = getSelectReplacedSql(
    newBaseSql,
    finalAliasedColumnsClause
  );
  return sqlWithFilterProjects;
};
