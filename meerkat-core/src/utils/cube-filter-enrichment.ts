import { TableSchema } from '@devrev/cube-types';
import {
  LogicalAndFilterWithInfo,
  LogicalOrFilterWithInfo,
  QueryFiltersWithInfo,
  QueryOperatorsWithInfo,
} from '../cube-to-duckdb/cube-filter-to-duckdb';
import { getMemberInfoFromTableSchema } from './key-from-measures-dimension';
import {
  isFilterArray,
  isLogicalAnd,
  isLogicalAndOR,
  isLogicalOr,
  isQueryFilter,
} from './type-guards';

export const cubeFiltersEnrichmentInternal = (
  filters:
    | (
        | QueryOperatorsWithInfo
        | LogicalAndFilterWithInfo
        | LogicalOrFilterWithInfo
      )
    | QueryFiltersWithInfo,
  tableSchema: TableSchema
) => {
  if (!isFilterArray(filters) && isLogicalAndOR(filters)) {
    if (isLogicalAnd(filters)) {
      return cubeFiltersEnrichment(filters.and, tableSchema);
    }
    if (isLogicalOr(filters)) {
      return cubeFiltersEnrichment(filters.or, tableSchema);
    }
  }
  if (!isFilterArray(filters) && isQueryFilter(filters)) {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    filters.memberInfo = getMemberInfoFromTableSchema(
      filters.member,
      tableSchema
    );
  }

  if (isFilterArray(filters)) {
    for (const filter of filters) {
      if (isLogicalAndOR(filter)) {
        if (isLogicalAnd(filter)) {
          cubeFiltersEnrichmentInternal(filter.and, tableSchema);
        }
        if (isLogicalOr(filter)) {
          cubeFiltersEnrichmentInternal(filter.or, tableSchema);
        }
      } else if (isQueryFilter(filter)) {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        //@ts-ignore
        filter.memberInfo = getMemberInfoFromTableSchema(
          filter.member,
          tableSchema
        );
      }
    }
  }

  return filters;
};

export const cubeFiltersEnrichment = (
  filters: QueryFiltersWithInfo,
  tableSchema: TableSchema
): QueryFiltersWithInfo | null => {
  if (!filters) {
    return null;
  }

  if (isFilterArray(filters)) {
    for (const filter of filters) {
      cubeFiltersEnrichmentInternal(filter, tableSchema);
    }
  }
  return filters;
};
