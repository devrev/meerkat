import { TableSchema } from '../types/cube-types';
import {
  findInDimensionSchema,
  findInMeasureSchema,
} from '../utils/find-in-table-schema';
import { memberKeyToSafeKey } from '../utils/member-key-to-safe-key';

export const getFilterMeasureProjection = ({
  key,
  tableSchema,
  measures,
}: {
  key: string;
  tableSchema: TableSchema;
  measures: string[];
}) => {
  const tableName = key.split('.')[0];
  const measureWithoutTable = key.split('.')[1];
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
  const aliasKey = memberKeyToSafeKey(key);
  return { sql: `${key} AS ${aliasKey}`, foundMember, aliasKey };
};

export const getDimensionProjection = ({
  key,
  tableSchema,
}: {
  key: string;
  tableSchema: TableSchema;
}) => {
  // Find the table access key
  const measureWithoutTable = key.split('.')[1];
  const tableName = key.split('.')[0];

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
  const aliasKey = memberKeyToSafeKey(key);
  // Add the alias key to the set. So we have a reference to all the previously selected members.
  return { sql: `${foundMember.sql} AS ${aliasKey}`, foundMember, aliasKey };
};

const aggregator = ({
  member,
  aliasedColumnSet,
  acc,
  currentIndex,
  members,
  sql,
}: {
  member: string;
  aliasedColumnSet: Set<string>;
  acc: string;
  sql?: string;
  currentIndex: number;
  members: string[];
}) => {
  if (aliasedColumnSet.has(member) || !sql) {
    return acc;
  }
  aliasedColumnSet.add(member);
  acc += sql;
  if (currentIndex !== members.length - 1) {
    acc += `, `;
  }
  return acc;
};

export const getProjectionClause = (
  measures: string[],
  dimensions: string[],
  tableSchema: TableSchema,
  aliasedColumnSet: Set<string>
) => {
  const filteredDimensions = dimensions.filter((dimension) => {
    return dimension.split('.')[0] === tableSchema.name;
  });
  const filteredMeasures = measures.filter((measure) => {
    return measure.split('.')[0] === tableSchema.name;
  });
  const dimensionsProjections = filteredDimensions.reduce(
    (acc, member, currentIndex, members) => {
      const { sql: memberSql } = getDimensionProjection({
        key: member,
        tableSchema,
      });
      return aggregator({
        member,
        aliasedColumnSet,
        acc,
        currentIndex,
        members,
        sql: memberSql,
      });
    },
    ''
  );
  const measureProjections = filteredMeasures.reduce(
    (acc, member, currentIndex, members) => {
      const { sql: memberSql } = getFilterMeasureProjection({
        key: member,
        tableSchema,
        measures,
      });
      return aggregator({
        member,
        aliasedColumnSet,
        acc,
        currentIndex,
        members,
        sql: memberSql,
      });
    },
    ''
  );
  console.log('dimensionsProjections', dimensionsProjections);
  console.log('measureProjections', measureProjections);
  return (
    dimensionsProjections +
    (dimensionsProjections.length && measureProjections.length ? ', ' : '') +
    measureProjections
  );
};
