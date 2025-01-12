import {
  DIMENSION_TEST_CASES,
  MEASURE_TEST_CASES,
} from '../ast-validator/tests/test-data';
import { getColumnNamesFromAst } from './get-column-names-from-ast';

describe('get-column-names-from-ast', () => {
  for (const testCase of DIMENSION_TEST_CASES) {
    it(`should return the correct column names for ${testCase.description}`, () => {
      const result = getColumnNamesFromAst(testCase.node);

      expect(result).toEqual(testCase.columnNames);
    });
  }

  for (const testCase of MEASURE_TEST_CASES) {
    it(`should return the correct column names for ${testCase.description}`, () => {
      const result = getColumnNamesFromAst(testCase.node);

      expect(result).toEqual(testCase.columnNames);
    });
  }
});
