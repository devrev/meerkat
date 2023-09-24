import { Query, TableSchema } from '@devrev/cube-types';
import { SelectNode } from '@devrev/duckdb-serialization-types';
import { cubeFilterToDuckdbAST } from './cube-filter-transformer/factory';
import { getBaseAST } from './utils/base-ast';
import { cubeFiltersEnrichment } from './utils/cube-filter-enrichment';

export const cubeToDuckdbAST = (query: Query, tableSchema: TableSchema) => {
  const tableKey: string | null = 'base'; //tableKeyFromMeasuresDimension(query);
  /**
   * If no table key was found, return null.
   */
  if (!tableKey) {
    return null;
  }

  /**
   * Obviously, if no table schema was found, return null.
   */
  if (!tableSchema) {
    return null;
  }

  const baseAST = getBaseAST();
  /**
   * Make a copy of the query filters and enrich them with the table schema.
   */
  const queryFiltersWithInfo = cubeFiltersEnrichment(
    JSON.parse(JSON.stringify(query.filters)),
    tableSchema
  );

  if (!queryFiltersWithInfo) {
    return null;
  }

  const duckdbWhereClause = cubeFilterToDuckdbAST(
    queryFiltersWithInfo,
    baseAST
  );
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  //@ts-ignore
  (baseAST.node as SelectNode).where_clause = duckdbWhereClause;

  // console.info(JSON.stringify(baseAST, null, 2));

  return baseAST;
};
