import {
  QueryNodeType,
  TableReferenceType,
} from '../../types/duckdb-serialization-types';
import { AggregateHandling } from '../../types/duckdb-serialization-types/serialization/QueryNode';
import { ParsedSerialization } from '../types';
import { getSelectNode } from '../utils';
import { COLUMN_REF_NODE } from './test-data';

const PARSED_SERIALIZATION: ParsedSerialization = {
  error: false,
  statements: [
    {
      node: {
        type: QueryNodeType.SELECT_NODE,
        modifiers: [],
        cte_map: {
          map: [],
        },
        select_list: [COLUMN_REF_NODE],
        from_table: {
          type: TableReferenceType.BASE_TABLE,
          alias: '',
          sample: null,
        },
        group_expressions: [],
        group_sets: [],
        aggregate_handling: AggregateHandling.STANDARD_HANDLING,
        having: null,
        sample: null,
        qualify: null,
      },
    },
  ],
};

describe('getSelectNode', () => {
  it('should throw error if the statement if there is no statement', () => {
    expect(() =>
      getSelectNode({
        error: false,
        statements: [],
      })
    ).toThrow('No statement found');
  });

  it('should throw error if no statement is found', () => {
    expect(() =>
      getSelectNode({
        error: false,
        statements: [
          {
            node: {
              type: QueryNodeType.CTE_NODE,
              modifiers: [],
              cte_map: {
                map: [],
              },
            },
          },
        ],
      })
    ).toThrow('Statement must be a SELECT node');
  });

  it('should throw error if select list is not exactly one expression', () => {
    expect(() =>
      getSelectNode({
        error: false,
        statements: [
          {
            node: {
              type: QueryNodeType.SELECT_NODE,
              modifiers: [],
              cte_map: {
                map: [],
              },
              select_list: [],
            },
          },
        ],
      })
    ).toThrow('SELECT must contain exactly one expression');
  });

  it('should return true if the statement is valid', () => {
    expect(getSelectNode(PARSED_SERIALIZATION)).toBe(
      PARSED_SERIALIZATION.statements[0].node.select_list[0]
    );
  });
});
