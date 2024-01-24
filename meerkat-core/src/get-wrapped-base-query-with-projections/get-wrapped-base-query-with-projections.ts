import { getSelectReplacedSql } from '../cube-measure-transformer/cube-measure-transformer';
import { getDimensionProjection, getFilterMeasureProjection as getFilterMeasureProjections, getProjectionClause } from '../get-projection-clause/get-projection-clause';
import { MeerkatQueryFilter, Query, TableSchema } from '../types/cube-types';
import { findInDimensionSchema, findInMeasureSchema } from '../utils/find-in-table-schema';

interface GetWrappedBaseQueryWithProjectionsParams { baseQuery: string, tableSchema: TableSchema, query: Query }


const getFilterProjections = ({ member, tableSchema, measures }: { member: string, tableSchema: TableSchema, measures: string[] }) => {
  const memberWithoutTable = member.split('.')[1];
  const isDimension = findInDimensionSchema(memberWithoutTable, tableSchema);
  if (isDimension) {
    return getDimensionProjection({ key: member, tableSchema })
  }
  const isMeasure = findInMeasureSchema(memberWithoutTable, tableSchema)
  if (isMeasure) {
    return getFilterMeasureProjections({ key: member, tableSchema, measures })
  }
  return {
    sql: undefined,
    foundMember: undefined,
    aliasKey: undefined,
  }
}

const getAliasedColumnsFromFilters = ({ baseSql, meerkatFilters, tableSchema, aliasedColumnSet, measures }: {
  meerkatFilters?: MeerkatQueryFilter[];
  tableSchema: TableSchema;
  baseSql: string;
  aliasedColumnSet: Set<string>;
  measures: string[];
}) => {
  let sql = baseSql
  meerkatFilters?.forEach((filter) => {
    if ('and' in filter) {
      // Traverse through the passed 'and' filters
      sql += getAliasedColumnsFromFilters({
        baseSql: '',
        meerkatFilters: filter.and,
        tableSchema,
        aliasedColumnSet,
        measures
      })
    }
    if ('or' in filter) {
      // Traverse through the passed 'or' filters
      sql += getAliasedColumnsFromFilters({
        baseSql: '',
        tableSchema,
        meerkatFilters: filter.or,
        aliasedColumnSet,
        measures
      })
    } 
    if ('member' in filter) {
      const { aliasKey, foundMember, sql: memberSql } = getFilterProjections({ member: filter.member, tableSchema, measures })
      if (!foundMember || aliasedColumnSet.has(aliasKey)) {
        // If the selected member is not found in the table schema or if it is already selected, continue.
        return;
      }
      if (aliasKey) {
        aliasedColumnSet.add(aliasKey)
      }
      // Add the alias key to the set. So we have a reference to all the previously selected members.
      sql += `, ${memberSql}`;
    }
  })
  return sql
}



export const getWrappedBaseQueryWithProjections = ({
  baseQuery,
  tableSchema,
  query
}: GetWrappedBaseQueryWithProjectionsParams) => {
    /*
    * Im order to be able to filter on computed metric from a query, we need to project the computed metric in the base query.
    * If theres filters supplied, we can safely return the original base query. Since nothing need to be projected and filtered in this case
    */
    // Wrap the query into another 'SELECT * FROM (baseQuery) AS baseTable'' in order to project everything in the base query, and other computed metrics to be able to filter on them
    const newBaseSql = `SELECT * FROM (${baseQuery}) AS ${tableSchema.name}`;
    const aliasedColumnSet = new Set<string>();
    
    const aliasFromFilters = getAliasedColumnsFromFilters({
      aliasedColumnSet,
      baseSql: 'SELECT *',
      // setting measures to empty array, since we don't want to project measures present in the filters in the base query
      tableSchema: tableSchema,
      meerkatFilters: query.filters,
      measures: query.measures,
    });

    const memberProjections = getProjectionClause(query.measures, query.dimensions ?? [], tableSchema, aliasedColumnSet);
    const formattedMemberProjection = memberProjections ? `, ${memberProjections}` : '';
    
    const finalAliasedColumnsClause = aliasFromFilters + formattedMemberProjection;

    const sqlWithFilterProjects = getSelectReplacedSql(newBaseSql, finalAliasedColumnsClause)
    return sqlWithFilterProjects
}
