import { getSelectReplacedSql } from '../cube-measure-transformer/cube-measure-transformer';
import { MeerkatQueryFilter, Member, Query, TableSchema } from '../types/cube-types';
import { findInSchema } from '../utils/find-in-table-schema';
import { memberKeyToSafeKey } from '../utils/member-key-to-safe-key';

interface GetWrappedBaseQueryWithProjectionsParams { baseQuery: string, tableSchema: TableSchema, query: Query }

const getMemberProjection = ({ key, tableSchema }: {
  key: string;
  tableSchema: TableSchema;
}) => {;
  // Find the table access key
  const measureWithoutTable = key.split('.')[1];
  const aliasKey = memberKeyToSafeKey(key);

  const foundMember = findInSchema(measureWithoutTable, tableSchema)
  if (!foundMember) {
    // If the selected member is not found in the table schema or if it is already selected, continue.
    return {
      sql: undefined,
      foundMember: undefined,
      aliasKey: undefined
    }
  }
  // Add the alias key to the set. So we have a reference to all the previously selected members.
  return { sql: `${foundMember.sql} AS ${aliasKey}` , foundMember, aliasKey }
}

export const getAliasedColumnsFromFilters = ({ baseSql, members, meerkatFilters, tableSchema, aliasedColumnSet }: {
  members: Member[];
  meerkatFilters: MeerkatQueryFilter[];
  tableSchema: TableSchema;
  baseSql: string;
  aliasedColumnSet: Set<string>;
}) => {
  /*  
   * Maintain a set to make sure already seen members are not added again. 
   */
  let sql = baseSql
  if (!meerkatFilters) {
    return baseSql;
  }
  for (let i = 0; i < meerkatFilters.length; i++) {
    const filter = meerkatFilters[i]
    if ('and' in filter) {
    // Traverse through the passed 'and' filters
      sql += getAliasedColumnsFromFilters({
        baseSql: '',
        members: [],
        meerkatFilters: filter.and,
        tableSchema,
        aliasedColumnSet
      })
    }
    if ('or' in filter) {
    // Traverse through the passed 'or' filters
      sql += getAliasedColumnsFromFilters({
        baseSql: '',
        tableSchema,
        members: [],
        meerkatFilters: filter.or,
        aliasedColumnSet
      })
    } 
    if ('member' in filter) {
      const { sql: memberSql, aliasKey, foundMember }  = getMemberProjection({ key: filter.member, tableSchema })
      console.log('memberSql', memberSql, aliasKey, foundMember)
      if (!foundMember  || aliasedColumnSet.has(aliasKey)) {
        // If the selected member is not found in the table schema or if it is already selected, continue.
        continue;
      }
      if (aliasKey) {
        aliasedColumnSet.add(aliasKey)
      }
      // Add the alias key to the set. So we have a reference to all the previously selected members.
      sql += `, ${memberSql}`;
    }
  }
  const memberProjections = members.reduce((acc, member) => {
    const { sql: memberSql }  = getMemberProjection({ key: member, tableSchema })
    acc += `, ${memberSql}`
    return acc
  }, '')
  console.log('sql', {sql, memberProjections})
  return sql + memberProjections
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
    if (!query?.filters?.length) {
      return baseQuery
    }
    // Wrap the query into another 'SELECT * FROM (baseQuery) AS baseTable'' in order to project everything in the base query, and other computed metrics to be able to filter on them
    const newBaseSql = `SELECT * FROM (${baseQuery}) AS ${tableSchema.name}`;
    const aliasedColumns = getAliasedColumnsFromFilters({
      meerkatFilters: query.filters,
      tableSchema, baseSql: 'SELECT *',
      members: [...(query.dimensions ?? [])],
      aliasedColumnSet: new Set<string>()
    })
    // Append the aliased columns to the base query select statement
    const sqlWithFilterProjects = getSelectReplacedSql(newBaseSql, aliasedColumns)
    return sqlWithFilterProjects
  }
  