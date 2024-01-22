import {
  LogicalAndFilterWithInfo,
  LogicalOrFilterWithInfo,
  QueryOperatorsWithInfo,
} from '../cube-to-duckdb/cube-filter-to-duckdb';
import { Dimension, Measure } from '../types/cube-types/table';
import { cubeFiltersEnrichmentInternal } from './cube-filter-enrichment';

describe('cubeFiltersEnrichmentInternal and cubeFiltersEnrichment', () => {
  const measure: Measure = {
    name: 'column1',
    sql: `table.column1`,
    type: 'number',
  };
  const measure2: Dimension = {
    name: 'column2',
    sql: `table.column2`,
    type: 'number',
  };
  const mockTableSchema = {
    name: 'test',
    cube: 'testCube',
    measures: [measure],
    dimensions: [measure2],
  };

  it('should enrich filters with member info', () => {
    const filters: QueryOperatorsWithInfo[] = [
      {
        member: `table.column1`,
        operator: 'equals',
      },
    ];

    cubeFiltersEnrichmentInternal(filters, mockTableSchema);
    expect(filters[0].memberInfo.sql).toBe(mockTableSchema.measures[0].sql);
  });

  it('should enrich nested logicalAnd/OR filter with member info', () => {
    const filters:
      | QueryOperatorsWithInfo
      | LogicalAndFilterWithInfo
      | LogicalOrFilterWithInfo = {
      and: [
        {
          member: `table__column1`,
          operator: 'equals',
          memberInfo: {
            name: 'column1',
            sql: 'table.country',
            type: 'string',
          },
        },
        {
          or: [
            {
              member: `table.column1`,
              operator: 'equals',
              memberInfo: {
                name: 'column1',
                sql: 'table.country',
                type: 'string',
              },
            },
            {
              member: `table.column2`,
              operator: 'equals',
              memberInfo: {
                name: 'column2',
                sql: 'table.country',
                type: 'string',
              },
            },
          ],
        },
      ],
    };

    cubeFiltersEnrichmentInternal(filters, mockTableSchema);
    expect(filters.and[0].memberInfo.sql).toBe(mockTableSchema.measures[0].sql);
    expect(filters.and[1].or[0].memberInfo.sql).toBe(
      mockTableSchema.measures[0].sql
    );
    expect(filters.and[1].or[1].memberInfo.sql).toBe(
      mockTableSchema.dimensions[0].sql
    );
  });
});
