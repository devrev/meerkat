import { getAllColumnUsedInMeasures } from '../cube-measure-transformer/cube-measure-transformer';
import {
  getAliasFromSchema,
  shouldUseSafeAlias,
} from '../member-formatters/get-alias';
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
  const { measures, dimensions = [] } = query;
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
        member: getAliasFromSchema({
          name: member,
          tableSchema,
          safe: shouldUseSafeAlias({ isAstIdentifier: false }),
        }),
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

  const usedMeasureObjects = tableSchema.measures.filter((measure) => {
    return (
      measures.findIndex((key) => {
        const [, keyWithoutTable] = splitIntoDataSourceAndFields(key);
        return keyWithoutTable === measure.name;
      }) !== -1
    );
  });
  const columnsUsedInMeasures = getAllColumnUsedInMeasures(
    usedMeasureObjects,
    tableSchema
  );

  let columnsUsedInMeasuresInProjection = '';
  columnsUsedInMeasures.forEach((column, index) => {
    const safeKey = getAliasFromSchema({
      name: column,
      tableSchema,
      safe: true,
    });
    columnsUsedInMeasuresInProjection += `${column} AS ${safeKey}`;
    if (index !== columnsUsedInMeasures.length - 1) {
      columnsUsedInMeasuresInProjection += ', ';
    }
  });

  const combinedStr = [
    dimensionsProjections,
    measureProjections,
    columnsUsedInMeasuresInProjection,
  ];

  return combinedStr.filter((str) => str.length > 0).join(', ');
};
