import { validateExpressionNode } from '../measure-validator';
import { DIMENSION_TEST_CASES, MEASURE_TEST_CASES } from './test-data';

describe('validateExpressionNode for measure expressions should return true', () => {
  for (const data of MEASURE_TEST_CASES) {
    it(`should return true for measure expression: ${data.description}`, () => {
      expect(
        validateExpressionNode({
          node: data.node,
          validFunctions: data.validFunctions,
          parentNode: null,
          validScalarFunctions: data.validScalarFunctions,
        })
      ).toBe(data.expected);
    });
  }
});

describe('validateExpressionNode for dimension expressions should return false', () => {
  for (const data of DIMENSION_TEST_CASES) {
    it(`should return false for dimension expression: ${data.description}`, () => {
      expect(
        validateExpressionNode({
          node: data.node,
          validFunctions: data.validFunctions,
          parentNode: null,
          validScalarFunctions: data.validScalarFunctions,
        })
      ).toBe(!data.expected);
    });
  }
});
