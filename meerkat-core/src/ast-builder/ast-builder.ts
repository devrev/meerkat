import { cubeFilterToDuckdbAST } from '../cube-filter-transformer/factory';
import { cubeDimensionToGroupByAST } from '../cube-group-by-transformer/cube-group-by-transformer';
import { cubeLimitOffsetToAST } from '../cube-limit-offset-transformer/cube-limit-offset-transformer';
import { cubeOrderByToAST } from '../cube-order-by-transformer/cube-order-by-transformer';
import {
  QueryFiltersWithInfo,
  QueryFiltersWithInfoSingular,
} from '../cube-to-duckdb/cube-filter-to-duckdb';
import { traverseAndFilter } from '../filter-params/filter-params-ast';
import {
  constructAliasForAST,
  QueryOptions,
} from '../member-formatters/get-alias';
import {
  FilterType,
  MeerkatQueryFilter,
  Query,
} from '../types/cube-types/query';
import { TableSchema } from '../types/cube-types/table';
import { SelectStatement } from '../types/duckdb-serialization-types';
import { SelectNode } from '../types/duckdb-serialization-types/serialization/QueryNode';
import { getBaseAST } from '../utils/base-ast';
import { cubeFiltersEnrichment } from '../utils/cube-filter-enrichment';
import { modifyLeafMeerkatFilter } from '../utils/modify-meerkat-filter';

const formatFilters = (
  queryFiltersWithInfo: QueryFiltersWithInfo,
  filterType: FilterType | undefined,
  config: QueryOptions
) => {
  /*
   * If the type of filter is set to base filter where
   */
  return filterType === 'BASE_FILTER'
    ? queryFiltersWithInfo
    : (modifyLeafMeerkatFilter(queryFiltersWithInfo, (item) => {
        return {
          ...item,
          member: constructAliasForAST(
            item.member,
            item.memberInfo.alias,
            config
          ),
        };
      }) as QueryFiltersWithInfo);
};

const getFormattedFilters = ({
  queryFiltersWithInfo,
  filterType,
  mapperFn,
  baseAST,
  config,
}: {
  queryFiltersWithInfo: QueryFiltersWithInfo;
  filterType?: FilterType;
  baseAST: SelectStatement;
  mapperFn: (val: QueryFiltersWithInfoSingular) => MeerkatQueryFilter | null;
  config: QueryOptions;
}) => {
  const filters = queryFiltersWithInfo
    .map((item) => mapperFn(item))
    .filter(Boolean) as QueryFiltersWithInfoSingular[];
  const formattedFilters = formatFilters(filters, filterType, config);
  // When it's a projection filter (not BASE_FILTER), we reference projected aliases.
  // When it's a BASE_FILTER, we reference table.column directly.
  const isProjectionFilter = filterType !== 'BASE_FILTER';
  const columnRefOptions = {
    isAlias: isProjectionFilter,
    useDotNotation: config.useDotNotation,
  };
  return cubeFilterToDuckdbAST(formattedFilters, baseAST, columnRefOptions);
};

export const cubeToDuckdbAST = (
  query: Query,
  tableSchema: TableSchema,
  options: { filterType: FilterType; config: QueryOptions }
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

    const whereClause = getFormattedFilters({
      baseAST,
      mapperFn: (item) =>
        traverseAndFilter(
          item,
          (value) => !query.measures.includes(value.member)
        ),
      queryFiltersWithInfo,
      filterType: options.filterType,
      config: options.config,
    });

    const havingClause = getFormattedFilters({
      baseAST,
      mapperFn: (item) =>
        traverseAndFilter(item, (value) =>
          query.measures.includes(value.member)
        ),
      queryFiltersWithInfo,
      filterType: options.filterType,
      config: options.config,
    });

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    node.where_clause = whereClause;
    node.having = havingClause;
  }
  if (
    query.measures.length &&
    query.dimensions &&
    query.dimensions?.length > 0
  ) {
    node.group_expressions = cubeDimensionToGroupByAST(
      query.dimensions,
      tableSchema,
      options.config
    );
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
  if (query.order && Object.keys(query.order).length > 0) {
    node.modifiers.push(
      cubeOrderByToAST(query.order, tableSchema, options.config)
    );
  }
  if (query.limit || query.offset) {
    // Type assertion is needed here because the AST is not typed correctly.
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    node.modifiers.push(cubeLimitOffsetToAST(query.limit, query.offset));
  }

  return baseAST;
};
