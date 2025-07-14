import { getAlias } from '../member-formatters';
import { splitIntoDataSourceAndFields } from '../member-formatters/split-into-data-source-and-fields';
import { MeerkatQueryFilter, Query, TableSchema } from '../types/cube-types';
import {
  findInDimensionSchema,
  findInMeasureSchema,
} from '../utils/find-in-table-schema';
import { getModifiedSqlExpression, Modifier } from './sql-expression-modifiers';

export const getDimensionProjection = ({
  key,
  tableSchema,
  modifiers,
  query,
  aliases,
}: {
  key: string;
  tableSchema: TableSchema;
  modifiers: Modifier[];
  query: Query;
  aliases?: Record<string, string>;
}) => {
  // Find the table access key
  const [tableName, measureWithoutTable] = splitIntoDataSourceAndFields(key);

  const foundMember = findInDimensionSchema(measureWithoutTable, tableSchema);
  if (!foundMember || tableName !== tableSchema.name) {
    // If the selected member is not found in the table schema or if it is already selected, continue.
    // If the selected member is not from the current table, don't create an alias.
    return {
      sql: undefined,
      foundMember: undefined,
      aliasKey: undefined,
    };
  }

  const modifiedSql = getModifiedSqlExpression({
    dimension: foundMember,
    key: key,
    modifiers: modifiers,
    sqlExpression: foundMember.sql,
    query,
  });

  const aliasKey = getAlias(key, aliases, true);
  // Add the alias key to the set. So we have a reference to all the previously selected members.
  return { sql: `${modifiedSql} AS ${aliasKey}`, foundMember, aliasKey };
};

export const getFilterMeasureProjection = ({
  key,
  tableSchema,
  measures,
  aliases,
}: {
  key: string;
  tableSchema: TableSchema;
  measures: string[];
  aliases?: Record<string, string>;
}) => {
  const [tableName, measureWithoutTable] = splitIntoDataSourceAndFields(key);
  const foundMember = findInMeasureSchema(measureWithoutTable, tableSchema);
  const isMeasure = measures.includes(key);
  if (!foundMember || isMeasure || tableName !== tableSchema.name) {
    // If the selected member is not found in the table schema or if it is already selected, continue.
    // If the selected member is a measure, don't create an alias. Since measure computation is done in the outermost level of the query
    // If the selected member is not from the current table, don't create an alias.
    return {
      sql: undefined,
      foundMember: undefined,
      aliasKey: undefined,
    };
  }
  const aliasKey = getAlias(key, aliases, true);
  return { sql: `${key} AS ${aliasKey}`, foundMember, aliasKey };
};

const getFilterProjections = ({
  member,
  tableSchema,
  measures,
  query,
  aliases,
}: {
  member: string;
  tableSchema: TableSchema;
  measures: string[];
  query: Query;
  aliases?: Record<string, string>;
}) => {
  const [, memberWithoutTable] = splitIntoDataSourceAndFields(member);
  const isDimension = findInDimensionSchema(memberWithoutTable, tableSchema);
  if (isDimension) {
    return getDimensionProjection({
      key: member,
      tableSchema,
      modifiers: [],
      aliases,
      query,
    });
  }
  const isMeasure = findInMeasureSchema(memberWithoutTable, tableSchema);
  if (isMeasure) {
    return getFilterMeasureProjection({
      key: member,
      tableSchema,
      aliases,
      measures,
    });
  }
  return {
    sql: undefined,
    foundMember: undefined,
    aliasKey: undefined,
  };
};

export const getAliasedColumnsFromFilters = ({
  baseSql,
  meerkatFilters,
  tableSchema,
  aliases,
  aliasedColumnSet,
  query,
}: {
  meerkatFilters?: MeerkatQueryFilter[];
  tableSchema: TableSchema;
  baseSql: string;
  aliases?: Record<string, string>;
  aliasedColumnSet: Set<string>;
  query: Query;
}) => {
  let sql = baseSql;
  const { measures } = query;
  meerkatFilters?.forEach((filter) => {
    if ('and' in filter) {
      // Traverse through the passed 'and' filters
      sql += getAliasedColumnsFromFilters({
        baseSql: '',
        meerkatFilters: filter.and,
        tableSchema,
        aliases,
        aliasedColumnSet,
        query,
      });
    }
    if ('or' in filter) {
      // Traverse through the passed 'or' filters
      sql += getAliasedColumnsFromFilters({
        baseSql: '',
        tableSchema,
        meerkatFilters: filter.or,
        aliases,
        aliasedColumnSet,
        query,
      });
    }
    if ('member' in filter) {
      const {
        aliasKey,
        foundMember,
        sql: memberSql,
      } = getFilterProjections({
        member: filter.member,
        tableSchema,
        measures,
        aliases,
        query,
      });
      if (!foundMember || aliasedColumnSet.has(aliasKey)) {
        // If the selected member is not found in the table schema or if it is already selected, continue.
        return;
      }
      if (aliasKey) {
        aliasedColumnSet.add(aliasKey);
      }
      // Add the alias key to the set. So we have a reference to all the previously selected members.
      sql += `, ${memberSql}`;
    }
  });
  return sql;
};
