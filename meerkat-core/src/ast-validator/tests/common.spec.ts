import {
  QueryNodeType,
  TableReferenceType,
} from '../../types/duckdb-serialization-types';
import { AggregateHandling } from '../../types/duckdb-serialization-types/serialization/QueryNode';
import { validateSelectNode } from '../common';
import { ParsedSerialization } from '../types';
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

describe('validateSelectNode', () => {
  it('should throw error if the statement if there is no statement', () => {
    expect(() =>
      validateSelectNode({
        error: false,
        statements: [],
      })
    ).toThrow('No statement found');
  });

  it('should throw error if no statement is found', () => {
    expect(() =>
      validateSelectNode({
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
      validateSelectNode({
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
    expect(validateSelectNode(PARSED_SERIALIZATION)).toBe(
      PARSED_SERIALIZATION.statements[0].node.select_list[0]
    );
  });
});
