import { cubeFilterToDuckdbAST } from '../cube-filter-transformer/factory';
import { cubeDimensionToGroupByAST } from '../cube-group-by-transformer/cube-group-by-transformer';
import { cubeLimitOffsetToAST } from '../cube-limit-offset-transformer/cube-limit-offset-transformer';
import { cubeOrderByToAST } from '../cube-order-by-transformer/cube-order-by-transformer';
import { QueryFiltersWithInfo, QueryFiltersWithInfoSingular, QueryOperatorsWithInfo } from '../cube-to-duckdb/cube-filter-to-duckdb';
import { traverseAndFilter } from '../filter-params/filter-params-ast';
import { FilterType, LogicalOrFilter, Query } from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import { SelectNode } from '../types/duckdb-serialization-types/serialization/QueryNode';
import { getBaseAST } from '../utils/base-ast';
import { cubeFiltersEnrichment } from '../utils/cube-filter-enrichment';
import { modifyLeafMeerkatFilter } from '../utils/modify-meerkat-filter';

const traverseFiltersAndModify = (filter: QueryFiltersWithInfoSingular, callback: (val: QueryOperatorsWithInfo) => boolean) => {
  if ('member' in filter) {
    return callback(filter) ? filter : null
  }
  if ('and' in filter) {
    const andFilters = filter.and
      .map(item => {
        return traverseFiltersAndModify(item, callback)
      })
      .filter(Boolean) as QueryFiltersWithInfoSingular[]
    const obj = andFilters.length > 0 ? { or: andFilters } : null;
    return obj
  }
  if ('or' in filter) {
    const orFilters = filter.or
      .map(item => {
        return traverseFiltersAndModify(item, callback)
      })
      .filter(Boolean) as QueryFiltersWithInfoSingular[]
    const obj = orFilters.length > 0 ? { or: orFilters } : null;
    return obj as LogicalOrFilter
  }
  return null
}



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

    /*
    * If the type of filter is set to base filter where 
    */
    const finalFilters = options?.filterType === 'BASE_FILTER' ? queryFiltersWithInfo : modifyLeafMeerkatFilter(queryFiltersWithInfo, (item) => {
      return {
        ...item,
        member: item.member.split('.').join('__')
      };
    }) as QueryFiltersWithInfo; 

    const havingFilters = finalFilters.map(item => traverseAndFilter(item, (value) =>  query.measures.includes(value.member))).filter(Boolean) as QueryFiltersWithInfoSingular[]
    const whereFilters = finalFilters.map(item => traverseAndFilter(item, (value) => !query.measures.includes(value.member))).filter(Boolean) as QueryFiltersWithInfoSingular[]

    const duckdbWhereClause = cubeFilterToDuckdbAST(
      whereFilters,
      baseAST
    );

    const duckdbHavingClause = cubeFilterToDuckdbAST(
      havingFilters,
      baseAST
    );
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    node.where_clause = duckdbWhereClause;
    node.having = duckdbHavingClause
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
