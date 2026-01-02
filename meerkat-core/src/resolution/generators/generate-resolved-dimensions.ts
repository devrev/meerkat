import { getNamespacedKey, memberKeyToSafeKey } from '../../member-formatters';
import { QueryOptions } from '../../member-formatters/get-alias';
import { Member, Query } from '../../types/cube-types/query';
import { ResolutionConfig } from '../types';

export const generateResolvedDimensions = (
  baseDataSourceName: string,
  query: Query,
  config: ResolutionConfig,
  columnProjections: string[] | undefined,
  options: QueryOptions
): Member[] => {
  // If column projections are provided, use those.
  // Otherwise, use all measures and dimensions from the original query.
  const aggregatedDimensions = columnProjections
    ? columnProjections
    : [...query.measures, ...(query.dimensions || [])];

  const resolvedDimensions: Member[] = aggregatedDimensions.flatMap(
    (dimension) => {
      const columnConfig = config.columnConfigs.find(
        (c) => c.name === dimension
      );

      if (!columnConfig) {
        return [
          getNamespacedKey(
            baseDataSourceName,
            memberKeyToSafeKey(dimension, options)
          ),
        ];
      } else {
        return columnConfig.resolutionColumns.map((col) =>
          getNamespacedKey(
            memberKeyToSafeKey(dimension, options),
            memberKeyToSafeKey(
              getNamespacedKey(columnConfig.name, col),
              options
            )
          )
        );
      }
    }
  );
  return resolvedDimensions;
};
