import { MeerkatQueryFilter, QueryFilter } from '../types/cube-types';
import { TableSchema } from '../types/cube-types/table';

export interface CategorizedFilters {
  /**
   * Filters that should be pushed down to the base query
   * These operate on columns that exist in the base table
   */
  baseFilters: MeerkatQueryFilter[];

  /**
   * Filters that need computed columns or projections
   * These operate on aliased or computed fields
   */
  projectionFilters: MeerkatQueryFilter[];

  /**
   * Filters that should be in HAVING clause
   * These operate on aggregate measures
   */
  aggregateFilters: MeerkatQueryFilter[];
}

/**
 * Determines if a filter can be safely pushed down to the base query
 */
const canPushDownFilter = (
  filter: QueryFilter,
  tableSchema: TableSchema
): boolean => {
  const memberName = filter.member.split('.').pop();
  if (!memberName) return false;

  // Check if this is a dimension that exists in the base schema
  const dimension = tableSchema.dimensions.find((d) => d.name === memberName);
  if (dimension) {
    // Only push down if the dimension doesn't require complex computation
    // (i.e., it's a simple column reference)
    return !dimension.sql.includes('(') && !dimension.sql.includes('CASE');
  }

  return false;
};

/**
 * Determines if a filter operates on an aggregate measure
 */
const isAggregateFilter = (
  filter: QueryFilter,
  tableSchema: TableSchema,
  measures: string[]
): boolean => {
  return measures.includes(filter.member);
};

/**
 * Categorizes filters into base, projection, and aggregate filters
 * for optimal query performance
 */
export const categorizeFilters = (
  filters: MeerkatQueryFilter[],
  tableSchema: TableSchema,
  measures: string[]
): CategorizedFilters => {
  const result: CategorizedFilters = {
    baseFilters: [],
    projectionFilters: [],
    aggregateFilters: [],
  };

  const categorizeFilter = (filter: MeerkatQueryFilter): void => {
    if ('member' in filter) {
      if (isAggregateFilter(filter, tableSchema, measures)) {
        result.aggregateFilters.push(filter);
      } else if (canPushDownFilter(filter, tableSchema)) {
        result.baseFilters.push(filter);
      } else {
        result.projectionFilters.push(filter);
      }
      return;
    }

    if ('and' in filter) {
      // For AND filters, recursively categorize each sub-filter
      // and group them by category
      const categorized: CategorizedFilters = {
        baseFilters: [],
        projectionFilters: [],
        aggregateFilters: [],
      };

      filter.and.forEach((subFilter) => {
        const subCategorized = categorizeFilters(
          [subFilter],
          tableSchema,
          measures
        );
        categorized.baseFilters.push(...subCategorized.baseFilters);
        categorized.projectionFilters.push(...subCategorized.projectionFilters);
        categorized.aggregateFilters.push(...subCategorized.aggregateFilters);
      });

      if (categorized.baseFilters.length > 0) {
        result.baseFilters.push(
          categorized.baseFilters.length === 1
            ? categorized.baseFilters[0]
            : { and: categorized.baseFilters }
        );
      }
      if (categorized.projectionFilters.length > 0) {
        result.projectionFilters.push(
          categorized.projectionFilters.length === 1
            ? categorized.projectionFilters[0]
            : { and: categorized.projectionFilters }
        );
      }
      if (categorized.aggregateFilters.length > 0) {
        result.aggregateFilters.push(
          categorized.aggregateFilters.length === 1
            ? categorized.aggregateFilters[0]
            : { and: categorized.aggregateFilters }
        );
      }
      return;
    }

    if ('or' in filter) {
      // For OR filters, all sub-filters must be in the same category
      // Otherwise, keep them together in projectionFilters
      const firstSubFilter = filter.or[0];
      if ('member' in firstSubFilter) {
        const allCanPushDown = filter.or.every((subFilter) => {
          return (
            'member' in subFilter && canPushDownFilter(subFilter, tableSchema)
          );
        });

        const allAreAggregate = filter.or.every((subFilter) => {
          return (
            'member' in subFilter &&
            isAggregateFilter(subFilter, tableSchema, measures)
          );
        });

        if (allCanPushDown) {
          result.baseFilters.push(filter);
        } else if (allAreAggregate) {
          result.aggregateFilters.push(filter);
        } else {
          result.projectionFilters.push(filter);
        }
      } else {
        // Complex OR filter, keep in projection
        result.projectionFilters.push(filter);
      }
    }
  };

  filters.forEach(categorizeFilter);
  return result;
};

/**
 * Estimates the performance impact of a filter
 * Used to prioritize which filters to apply first
 */
export const estimateFilterSelectivity = (filter: QueryFilter): number => {
  // Lower score = more selective = should apply earlier

  if (filter.operator === 'equals') {
    return 1; // Very selective
  }

  if (filter.operator === 'in' && filter.values) {
    // Selectivity decreases with more values
    return Math.min(filter.values.length, 100);
  }

  if (filter.operator === 'contains' || filter.operator === 'notContains') {
    return 50; // Moderately selective
  }

  if (filter.operator === 'set' || filter.operator === 'notSet') {
    return 30; // Can be quite selective
  }

  return 75; // Default for other operators
};

/**
 * Sorts filters by estimated selectivity for optimal application order
 */
export const sortFiltersBySelectivity = (
  filters: MeerkatQueryFilter[]
): MeerkatQueryFilter[] => {
  const scored = filters.map((filter) => ({
    filter,
    score: 'member' in filter ? estimateFilterSelectivity(filter) : 50,
  }));

  scored.sort((a, b) => a.score - b.score);
  return scored.map((s) => s.filter);
};
