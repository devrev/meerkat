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

const getProjectionSql = (members: string[], tableSchema: TableSchema, aliasedColumnSet: Set<string>) => {
  return members.reduce((acc, member) => {
    const { sql: memberSql }  = getMemberProjection({ key: member, tableSchema })
    if (aliasedColumnSet.has(member)) {
      return acc
    }
    acc += `, ${memberSql}`
    return acc
  }, '')
}

export const getAliasedColumns = ({ baseSql, members, meerkatFilters, tableSchema, aliasedColumnSet }: {
  members: Member[];
  meerkatFilters?: MeerkatQueryFilter[];
  tableSchema: TableSchema;
  baseSql: string;
  aliasedColumnSet: Set<string>;
}) => {
  /*  
   * Maintain a set to make sure already seen members are not added again. 
   */
  let sql = baseSql
  meerkatFilters?.forEach((filter) => {
    if ('and' in filter) {
      // Traverse through the passed 'and' filters
        sql += getAliasedColumns({
          baseSql: '',
          members: [],
          meerkatFilters: filter.and,
          tableSchema,
          aliasedColumnSet
        })
      }
      if ('or' in filter) {
      // Traverse through the passed 'or' filters
        sql += getAliasedColumns({
          baseSql: '',
          tableSchema,
          members: [],
          meerkatFilters: filter.or,
          aliasedColumnSet,
        })
      } 
      if ('member' in filter) {
        const { sql: memberSql, aliasKey, foundMember }  = getMemberProjection({ key: filter.member, tableSchema })
        if (!foundMember  || aliasedColumnSet.has(aliasKey)) {
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
  // Alias dimensions in the base query/
  const memberProjections = getProjectionSql(members, tableSchema, aliasedColumnSet);
  return sql + memberProjections;
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
    const aliasedColumns = getAliasedColumns({
      meerkatFilters: query.filters,
      tableSchema, baseSql: 'SELECT *',
      members: [...(query.dimensions ?? [])],
      aliasedColumnSet: new Set<string>()
    })
    // Append the aliased columns to the base query select statement
    const sqlWithFilterProjects = getSelectReplacedSql(newBaseSql, aliasedColumns)
    return sqlWithFilterProjects
}
  