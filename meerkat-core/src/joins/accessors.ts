import type { Query } from '../types/cube-types';

/** True when the query declares any join path (legacy or v2). */
export const hasAnyJoinPaths = (cubeQuery: Query): boolean =>
  Boolean(cubeQuery.joinPaths?.length) ||
  Boolean(cubeQuery.joinPathsV2?.length);

/** True when the query opts into the v2 structured-join emitter. */
export const hasJoinPathsV2 = (cubeQuery: Query): boolean =>
  Boolean(cubeQuery.joinPathsV2?.length);
