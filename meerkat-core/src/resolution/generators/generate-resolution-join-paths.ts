import { constructAlias, memberKeyToSafeKey } from '../../member-formatters';
import { JoinPath } from '../../types/cube-types/query';
import { TableSchema } from '../../types/cube-types/table';
import { findInSchemas } from '../../utils/find-in-table-schema';
import { ResolutionConfig } from '../types';

export const generateResolutionJoinPaths = (
  baseDataSourceName: string,
  resolutionConfig: ResolutionConfig,
  baseTableSchemas: TableSchema[]
): JoinPath[] => {
  return resolutionConfig.columnConfigs.map((config) => [
    {
      left: baseDataSourceName,
      right: memberKeyToSafeKey(config.name),
      on: constructAlias({
        name: config.name,
        alias: findInSchemas(config.name, baseTableSchemas)?.alias,
        aliasContext: { isTableSchemaAlias: true },
      }),
    },
  ]);
};
