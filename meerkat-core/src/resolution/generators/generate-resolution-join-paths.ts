import { memberKeyToSafeKey } from '../../member-formatters';
import { JoinPath } from '../../types/cube-types/query';
import { ResolutionConfig } from '../types';

export const generateResolutionJoinPaths = (
  baseDataSourceName: string,
  resolutionConfig: ResolutionConfig
): JoinPath[] => {
  return resolutionConfig.columnConfigs.map((config) => [
    {
      left: baseDataSourceName,
      right: memberKeyToSafeKey(config.name),
      // Use safe keys internally
      on: memberKeyToSafeKey(config.name),
    },
  ]);
};
