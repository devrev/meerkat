import { cubeFilterToDuckdbAST } from '../cube-filter-transformer/factory';
import { cubeDimensionToGroupByAST } from '../cube-group-by-transformer/cube-group-by-transformer';
import { cubeLimitOffsetToAST } from '../cube-limit-offset-transformer/cube-limit-offset-transformer';
import { cubeOrderByToAST } from '../cube-order-by-transformer/cube-order-by-transformer';
import { QueryFiltersWithInfo } from '../cube-to-duckdb/cube-filter-to-duckdb';
import { FilterType, Query, QueryFilter } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import { SelectNode } from '../types/duckdb-serialization-types/serialization/QueryNode';
import { getBaseAST } from '../utils/base-ast';
import { cubeFiltersEnrichment } from '../utils/cube-filter-enrichment';

const modifyLeafMeerkatFilter = <T>(filters: QueryFiltersWithInfo, callback: (arg: QueryFilter) => T): T[] | undefined  => {
  if (!filters) return undefined;
  return filters.map((item) => {
    if ('member' in item) {
      return callback(item)
    } else {
      const andPayload: T[] | undefined = 'and' in item ? modifyLeafMeerkatFilter(item.and, callback) : undefined;
      const orPayload: T[] | undefined = 'or' in item ?  modifyLeafMeerkatFilter(item.or, callback) : undefined;
      
      return {
        ...(andPayload ? { and: andPayload } : {}),
        ...(orPayload ? { or: orPayload } : {}),
      } as T
    }
  })
};

export const cubeToDuckdbAST = (query: Query, tableSchema: TableSchema, options?: { filterType: FilterType }
) => {
  /**
   * Obviously, if no table schema was found, return null.
   */
  if (!tableSchema) {
    return null;
  }

  const baseAST = getBaseAST();
  const node = baseAST.node as SelectNode;

  if (query.filters && query.filters.length > 0) {
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

    const finalFilters = options?.filterType === 'BASE_FILTER' ? modifyLeafMeerkatFilter(queryFiltersWithInfo, (item) => {
      return {
        ...item,
        member: item.member.split('__').join('.')
      }
    }) as QueryFiltersWithInfo : queryFiltersWithInfo; 

    const duckdbWhereClause = cubeFilterToDuckdbAST(
      finalFilters,
      baseAST
    );
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    node.where_clause = duckdbWhereClause;
  }

  if (query.dimensions && query.dimensions?.length > 0) {
    node.group_expressions = cubeDimensionToGroupByAST(query.dimensions);
    const groupSets = [];
    /**
     * We only support one group set for now.
     */
    for (let i = 0; i < node.group_expressions.length; i++) {
      groupSets.push(i);
    }
    node.group_sets = [groupSets];
  }
  node.modifiers = [];
  if (query.order) {
    node.modifiers.push(cubeOrderByToAST(query.order));
  }
  if (query.limit || query.offset) {
    // Type assertion is needed here because the AST is not typed correctly.
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    node.modifiers.push(cubeLimitOffsetToAST(query.limit, query.offset));
  }

  return baseAST;
};
