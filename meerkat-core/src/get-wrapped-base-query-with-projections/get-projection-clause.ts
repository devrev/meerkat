import { getAllColumnUsedInMeasures } from '../cube-measure-transformer/cube-measure-transformer';
import { getAliasForSQL } from '../member-formatters/get-alias';
import { splitIntoDataSourceAndFields } from '../member-formatters/split-into-data-source-and-fields';
import { Query, TableSchema } from '../types/cube-types';
import {
  getDimensionProjection,
  getFilterMeasureProjection,
} from './get-aliased-columns-from-filters';
import { MODIFIERS } from './sql-expression-modifiers';

const memberClauseAggregator = ({
  member,
  aliasedColumnSet,
  acc,
  sql,
}: {
  member: string;
  aliasedColumnSet: Set<string>;
  acc: string[];
  sql?: string;
  currentIndex: number;
  members: string[];
}) => {
  if (aliasedColumnSet.has(member) || !sql) {
    return acc;
  }
  aliasedColumnSet.add(member);
  acc.push(sql);
  return acc;
};

export const getProjectionClause = (
  query: Query,
  tableSchema: TableSchema,
  aliasedColumnSet: Set<string>
) => {
  const { measures, dimensions = [], order } = query;
  const filteredDimensions = dimensions.filter((dimension) => {
    const [dimensionDataSource] = splitIntoDataSourceAndFields(dimension);
    return dimensionDataSource === tableSchema.name;
  });
  const filteredMeasures = measures.filter((measure) => {
    const [measureDataSource] = splitIntoDataSourceAndFields(measure);
    return measureDataSource === tableSchema.name;
  });
  const dimensionsProjectionsArr = filteredDimensions.reduce(
    (acc, member, currentIndex, members) => {
      const { sql: memberSql } = getDimensionProjection({
        key: member,
        tableSchema,
        modifiers: MODIFIERS,
        query,
      });
      return memberClauseAggregator({
        member: getAliasForSQL(member, tableSchema),
        aliasedColumnSet,
        acc,
        currentIndex,
        members,
        sql: memberSql,
      });
    },
    [] as string[]
  );
  const dimensionsProjections = dimensionsProjectionsArr.join(', ');

  const measureProjectionsArr = filteredMeasures.reduce(
    (acc, member, currentIndex, members) => {
      const { sql: memberSql } = getFilterMeasureProjection({
        key: member,
        tableSchema,
        measures,
      });
      return memberClauseAggregator({
        member,
        aliasedColumnSet,
        acc,
        currentIndex,
        members,
        sql: memberSql,
      });
    },
    [] as string[]
  );

  const measureProjections = measureProjectionsArr.join(', ');

  // Project order fields that are in the schema but not already in dimensions/measures
  const orderKeys = order ? Object.keys(order) : [];
  const filteredOrderKeys = orderKeys.filter((orderKey) => {
    const [orderDataSource] = splitIntoDataSourceAndFields(orderKey);
    return orderDataSource === tableSchema.name;
  });
  const orderProjectionsArr = filteredOrderKeys.reduce(
    (acc, member, currentIndex, members) => {
      const { sql: memberSql } = getDimensionProjection({
        key: member,
        tableSchema,
        modifiers: MODIFIERS,
        query,
      });
      return memberClauseAggregator({
        member: getAliasForSQL(member, tableSchema),
        aliasedColumnSet,
        acc,
        currentIndex,
        members,
        sql: memberSql,
      });
    },
    [] as string[]
  );
  const orderProjections = orderProjectionsArr.join(', ');

  /*
   * In a multi-table setting, two tables can expose measures of the same
   * `name` (e.g. `id___function__count` on both sides of a join). When
   * building this table's inner CTE we only want to scan the columns
   * referenced by cubeQuery keys belonging to THIS table — otherwise the
   * inner CTE for `srutiobj` would also try to project columns referenced
   * by `part`'s measure. Constrain by both bare-name AND call-site table.
   * Falls back to the legacy bare-name match when the schema has no name
   * (legacy single-table callers).
   */
  const usedMeasureObjects = tableSchema.measures.filter((measure) => {
    return (
      measures.findIndex((key) => {
        const [keyTableName, keyWithoutTable] = splitIntoDataSourceAndFields(
          key
        );
        if (keyWithoutTable !== measure.name) return false;
        if (!tableSchema.name) return true;
        return keyTableName === tableSchema.name;
      }) !== -1
    );
  });
  const columnsUsedInMeasures = getAllColumnUsedInMeasures(
    usedMeasureObjects,
    tableSchema
  );

  let columnsUsedInMeasuresInProjection = '';
  columnsUsedInMeasures.forEach((column, index) => {
    const safeKey = getAliasForSQL(column, tableSchema);
    columnsUsedInMeasuresInProjection += `${column} AS ${safeKey}`;
    if (index !== columnsUsedInMeasures.length - 1) {
      columnsUsedInMeasuresInProjection += ', ';
    }
  });

  const combinedStr = [
    dimensionsProjections,
    measureProjections,
    orderProjections,
    columnsUsedInMeasuresInProjection,
  ];

  return combinedStr.filter((str) => str.length > 0).join(', ');
};
