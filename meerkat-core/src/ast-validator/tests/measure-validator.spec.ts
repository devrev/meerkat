import {
  AggregateHandling,
  QueryNodeType,
  TableReferenceType,
} from '../../types/duckdb-serialization-types';
import {
  containsAggregation,
  validateExpressionNode,
  validateMeasure,
} from '../measure-validator';
import { DIMENSION_TEST_CASES, MEASURE_TEST_CASES } from './test-data';

describe('validateMeasure', () => {
  it('should throw error if the statement if there is no statement', () => {
    expect(() =>
      validateMeasure(
        {
          error: false,
          statements: [],
        },
        [],
        []
      )
    ).toThrow('No statement found');
  });

  it('should return true if the statement is valid', () => {
    expect(
      validateMeasure(
        {
          error: false,
          statements: [
            {
              node: {
                type: QueryNodeType.SELECT_NODE,
                modifiers: [],
                cte_map: {
                  map: [],
                },
                select_list: [MEASURE_TEST_CASES[0].node],
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
        },
        [],
        []
      )
    ).toBe(true);
  });
});

describe('validateExpressionNode for measure expressions', () => {
  for (const data of MEASURE_TEST_CASES) {
    it(`should return ${data.expected} for measure expression: ${data.description}`, () => {
      if (data.error) {
        expect(() =>
          validateExpressionNode({
            node: data.node,
            validFunctions: data.validFunctions,
            parentNode: null,
            validScalarFunctions: data.validScalarFunctions,
          })
        ).toThrow(data.error);
      } else {
        expect(
          validateExpressionNode({
            node: data.node,
            validFunctions: data.validFunctions,
            parentNode: null,
            validScalarFunctions: data.validScalarFunctions,
          })
        ).toBe(data.expected);
      }
    });
  }
});

describe('containsAggregation', () => {
  for (const data of DIMENSION_TEST_CASES) {
    it(`should return false for dimension expression: ${data.description}`, () => {
      expect(containsAggregation(data.node, new Set(['sum']))).toBe(
        !data.expected
      );
    });
  }

  it('should return true for aggregation expression', () => {
    expect(
      containsAggregation(MEASURE_TEST_CASES[1].node, new Set(['sum']))
    ).toBe(true);
  });
});
