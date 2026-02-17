import { getNamespacedKey, memberKeyToSafeKey } from '../../member-formatters';
import { Member, Query } from '../../types/cube-types/query';
import { ResolutionConfig } from '../types';

export const generateResolvedDimensions = (
  baseDataSourceName: string,
  query: Query,
  config: ResolutionConfig,
  columnProjections?: string[]
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
          getNamespacedKey(baseDataSourceName, memberKeyToSafeKey(dimension)),
        ];
      } else {
        return columnConfig.resolutionColumns.map((col) =>
          getNamespacedKey(
            memberKeyToSafeKey(dimension),
            memberKeyToSafeKey(getNamespacedKey(columnConfig.name, col))
          )
        );
      }
    }
  );
  return resolvedDimensions;
};
