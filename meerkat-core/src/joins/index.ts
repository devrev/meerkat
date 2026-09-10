import type { Query } from '../types/cube-types';
import type { GetQueryOutput } from '../utils/duckdb-ast-parse-serialize';
import { hasJoinPathsV2 } from './accessors';
import { getCombinedTableSchemaV2 } from './v2/joins';
import { getCombinedTableSchema as getCombinedTableSchemaV1 } from './v1/joins';

export { hasAnyJoinPaths, hasJoinPathsV2 } from './accessors';

export const getCombinedTableSchema = async (
  tableSchema: Parameters<typeof getCombinedTableSchemaV1>[0],
  cubeQuery: Query,
  getQueryOutput?: GetQueryOutput
) => {
  if (hasJoinPathsV2(cubeQuery)) {
    return getCombinedTableSchemaV2(tableSchema, cubeQuery, getQueryOutput);
  }
  return getCombinedTableSchemaV1(tableSchema, cubeQuery);
};

export {
  Graph,
  checkLoopInJoinPath,
  createDirectedGraph,
  generateSqlQuery,
  parseTableColumnReference,
  quoteIdentifierIfNeeded,
} from './v1/joins';

export {
  createDirectedGraphV2,
  generateSqlQueryV2,
  getCollectedDimensionsV2,
} from './v2/joins';
