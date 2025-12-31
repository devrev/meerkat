import {
  AliasConfig,
  DEFAULT_ALIAS_CONFIG,
  getAliasForSQL,
} from '../member-formatters/get-alias';
import { splitIntoDataSourceAndFields } from '../member-formatters/split-into-data-source-and-fields';
import { MeerkatQueryFilter, Query, TableSchema } from '../types/cube-types';
import {
  findInDimensionSchema,
  findInMeasureSchema,
} from '../utils/find-in-table-schema';
import { getModifiedSqlExpression } from './sql-expression-modifiers';
import { Modifier } from './types';

export const getDimensionProjection = ({
  key,
  tableSchema,
  modifiers,
  query,
  config = DEFAULT_ALIAS_CONFIG,
}: {
  key: string;
  tableSchema: TableSchema;
  modifiers: Modifier[];
  query: Query;
  config?: AliasConfig;
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

  const aliasKey = getAliasForSQL(key, tableSchema, config);
  // Add the alias key to the set. So we have a reference to all the previously selected members.
  return { sql: `${modifiedSql} AS ${aliasKey}`, foundMember, aliasKey };
};

export const getFilterMeasureProjection = ({
  key,
  tableSchema,
  measures,
  config = DEFAULT_ALIAS_CONFIG,
}: {
  key: string;
  tableSchema: TableSchema;
  measures: string[];
  config?: AliasConfig;
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
  const aliasKey = getAliasForSQL(key, tableSchema, config);
  return { sql: `${key} AS ${aliasKey}`, foundMember, aliasKey };
};

const getFilterProjections = ({
  member,
  tableSchema,
  measures,
  query,
  config = DEFAULT_ALIAS_CONFIG,
}: {
  member: string;
  tableSchema: TableSchema;
  measures: string[];
  query: Query;
  config?: AliasConfig;
}) => {
  const [, memberWithoutTable] = splitIntoDataSourceAndFields(member);
  const isDimension = findInDimensionSchema(memberWithoutTable, tableSchema);
  if (isDimension) {
    return getDimensionProjection({
      key: member,
      tableSchema,
      modifiers: [],
      query,
      config,
    });
  }
  const isMeasure = findInMeasureSchema(memberWithoutTable, tableSchema);
  if (isMeasure) {
    return getFilterMeasureProjection({
      key: member,
      tableSchema,
      measures,
      config,
    });
  }
  return {
    sql: undefined,
    foundMember: undefined,
    aliasKey: undefined,
  };
};

export const getAliasedColumnsFromFilters = ({
  meerkatFilters,
  tableSchema,
  aliasedColumnSet,
  query,
  config = DEFAULT_ALIAS_CONFIG,
}: {
  meerkatFilters?: MeerkatQueryFilter[];
  tableSchema: TableSchema;
  aliasedColumnSet: Set<string>;
  query: Query;
  config?: AliasConfig;
}) => {
  const parts: string[] = [];
  const { measures } = query;
  meerkatFilters?.forEach((filter) => {
    if ('and' in filter) {
      // Traverse through the passed 'and' filters
      const sql = getAliasedColumnsFromFilters({
        meerkatFilters: filter.and,
        tableSchema,
        aliasedColumnSet,
        query,
        config,
      });
      if (sql) {
        parts.push(sql);
      }
    }
    if ('or' in filter) {
      // Traverse through the passed 'or' filters
      const sql = getAliasedColumnsFromFilters({
        tableSchema,
        meerkatFilters: filter.or,
        aliasedColumnSet,
        query,
        config,
      });
      if (sql) {
        parts.push(sql);
      }
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
        query,
        config,
      });
      if (!foundMember || aliasedColumnSet.has(aliasKey)) {
        // If the selected member is not found in the table schema or if it is already selected, continue.
        return;
      }
      if (aliasKey) {
        aliasedColumnSet.add(aliasKey);
      }
      // Add the alias key to the set. So we have a reference to all the previously selected members.
      const sql = memberSql;
      if (sql) {
        parts.push(sql);
      }
    }
  });
  return parts.join(', ');
};
