import { constructAliasForAST } from '../../member-formatters/get-alias';
import { memberKeyToSafeKey } from '../../member-formatters/member-key-to-safe-key';
import { JoinPath } from '../../types/cube-types/query';
import { TableSchema } from '../../types/cube-types/table';
import { findInSchemas } from '../../utils/find-in-table-schema';
import { ResolutionConfig } from '../types';

export const generateResolutionJoinPaths = (
  baseDataSourceName: string,
  resolutionConfig: ResolutionConfig,
  baseTableSchemas: TableSchema[]
): JoinPath[] => {
  const config = { useDotNotation: false };
  return resolutionConfig.columnConfigs.map((columnConfig) => [
    {
      left: baseDataSourceName,
      right: memberKeyToSafeKey(columnConfig.name, {
        useDotNotation: config.useDotNotation,
      }),
      on: constructAliasForAST(
        columnConfig.name,
        findInSchemas(columnConfig.name, baseTableSchemas)?.alias,
        config
      ),
    },
  ]);
};
