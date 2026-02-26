import { Dimension } from '../../types/cube-types/table';
import { isArrayTypeMember } from '../../utils/is-array-member-type';
import {
  arrayFieldUnNestModifier,
  shouldUnnest,
} from '../modifiers/array-unnest-modifier';
import {
  getModifiedSqlExpression,
  MODIFIERS,
} from '../sql-expression-modifiers';
import { DimensionModifier } from '../types';

jest.mock('../../utils/is-array-member-type', () => {
  return {
    isArrayTypeMember: jest.fn(),
  };
});

const QUERY = {
  measures: ['test_measure'],
  dimensions: ['test_dimension'],
};

describe('Dimension Modifier', () => {
  describe('arrayFieldUnNestModifier', () => {
    it('should return the correct unnested SQL expression with NULL/empty array handling', () => {
      const modifier: DimensionModifier = {
        sqlExpression: 'some_array_field',
        dimension: {} as Dimension,
        key: 'test_key',
        query: QUERY,
      };
      expect(arrayFieldUnNestModifier(modifier)).toBe(
        'array[unnest(CASE WHEN some_array_field IS NULL OR len(COALESCE(some_array_field, [])) = 0 THEN [NULL] ELSE some_array_field END)]'
      );
    });

    it('should handle complex SQL expressions with NULL/empty array handling', () => {
      const modifier: DimensionModifier = {
        sqlExpression: 'table.nested_array',
        dimension: {} as Dimension,
        key: 'test_key',
        query: QUERY,
      };
      expect(arrayFieldUnNestModifier(modifier)).toBe(
        'array[unnest(CASE WHEN table.nested_array IS NULL OR len(COALESCE(table.nested_array, [])) = 0 THEN [NULL] ELSE table.nested_array END)]'
      );
    });

    it('should preserve NULL values by converting NULL arrays to [NULL]', () => {
      const modifier: DimensionModifier = {
        sqlExpression: 'nullable_array',
        dimension: {} as Dimension,
        key: 'test_key',
        query: QUERY,
      };
      const result = arrayFieldUnNestModifier(modifier);
      expect(result).toContain('CASE WHEN nullable_array IS NULL');
      expect(result).toContain('THEN [NULL]');
    });

    it('should preserve rows with empty arrays by converting to [NULL]', () => {
      const modifier: DimensionModifier = {
        sqlExpression: 'empty_array',
        dimension: {} as Dimension,
        key: 'test_key',
        query: QUERY,
      };
      const result = arrayFieldUnNestModifier(modifier);
      expect(result).toContain('len(COALESCE(empty_array, [])) = 0');
      expect(result).toContain('THEN [NULL]');
    });
  });

  describe('shouldUnnest', () => {
    it('should return true when dimension is array type and has shouldUnnestGroupBy modifier', () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(true);
      const modifier: DimensionModifier = {
        sqlExpression: 'some_expression',
        dimension: {
          type: 'array',
          modifier: { shouldUnnestGroupBy: true },
        } as Dimension,
        key: 'test_key',
        query: QUERY,
      };
      expect(shouldUnnest(modifier)).toBe(true);
    });

    it('should return false when dimension is not array type', () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(false);
      const modifier: DimensionModifier = {
        sqlExpression: 'some_expression',
        dimension: {
          type: 'string',
          modifier: { shouldUnnestGroupBy: true },
        } as Dimension,
        key: 'test_key',
        query: QUERY,
      };
      expect(shouldUnnest(modifier)).toBe(false);
    });

    it("should return false when dimension doesn't have shouldUnnestGroupBy modifier", () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(true);
      const modifier: DimensionModifier = {
        sqlExpression: 'some_expression',
        dimension: {
          type: 'array',
          modifier: {},
        } as Dimension,
        key: 'test_key',
        query: QUERY,
      };
      expect(shouldUnnest(modifier)).toBe(false);
    });
    it('should return false when dimension when modifier undefined', () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(true);
      const modifier: DimensionModifier = {
        sqlExpression: 'some_expression',
        dimension: {
          type: 'array',
        } as Dimension,
        key: 'test_key',
        query: QUERY,
      };
      expect(shouldUnnest(modifier)).toBe(false);
    });
  });

  describe('getModifiedSqlExpression', () => {
    it('should not modify if no modifiers passed', () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(true);
      const input = {
        sqlExpression: 'array_field',
        dimension: {
          type: 'array',
          modifier: { shouldUnnestGroupBy: true },
        } as Dimension,
        query: QUERY,
        key: 'test_key',
        modifiers: [],
      };
      expect(getModifiedSqlExpression(input)).toBe('array_field');
    });
    it('should apply the modifier when conditions are met', () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(true);
      const input = {
        sqlExpression: 'array_field',
        dimension: {
          type: 'array',
          modifier: { shouldUnnestGroupBy: true },
        } as Dimension,
        query: QUERY,
        key: 'test_key',
        modifiers: MODIFIERS,
      };
      expect(getModifiedSqlExpression(input)).toBe(
        'array[unnest(CASE WHEN array_field IS NULL OR len(COALESCE(array_field, [])) = 0 THEN [NULL] ELSE array_field END)]'
      );
    });

    it('should not apply the modifier when conditions are not met', () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(false);
      const input = {
        sqlExpression: 'non_array_field',
        dimension: {
          type: 'string',
          modifier: {},
        } as Dimension,
        query: QUERY,
        key: 'test_key',
        modifiers: MODIFIERS,
      };
      expect(getModifiedSqlExpression(input)).toBe('non_array_field');
    });
  });
});
