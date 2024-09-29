import { Dimension } from '../../types/cube-types/table';
import { isArrayTypeMember } from '../../utils/is-array-member-type';
import { arrayFieldUnNestModifier, DimensionModifier, getModifiedSqlExpression, MODIFIERS, shouldUnnest } from "../sql-expression-modifiers";

jest.mock("../../utils/is-array-member-type", () => {
  return {
    isArrayTypeMember: jest.fn()
  }
});

const QUERY = {
  measures: ["test_measure"],
  dimensions: ["test_dimension"]
}

describe("Dimension Modifier", () => {
  describe("arrayFieldUnNestModifier", () => {
    it("should return the correct unnested SQL expression", () => {
      const modifier: DimensionModifier = {
        sqlExpression: "some_array_field",
        dimension: {} as Dimension,
        key: "test_key",
        query: QUERY
      };
      expect(arrayFieldUnNestModifier(modifier)).toBe("array[unnest(some_array_field)]");
    });
  });

  describe("shouldUnnest", () => {
    it("should return true when dimension is array type and has unNestedGroupBy modifier", () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(true);
      const modifier: DimensionModifier = {
        sqlExpression: "some_expression",
        dimension: { 
          type: "array",
          modifier: { unNestedGroupBy: true }
        } as Dimension,
        key: "test_key",
        query: QUERY
      };
      expect(shouldUnnest(modifier)).toBe(true);
    });

    it("should return false when dimension is not array type", () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(false);
      const modifier: DimensionModifier = {
        sqlExpression: "some_expression",
        dimension: { 
          type: "string",
          modifier: { unNestedGroupBy: true }
        } as Dimension,
        key: "test_key",
        query: QUERY
      };
      expect(shouldUnnest(modifier)).toBe(false);
    });

    it("should return false when dimension doesn't have unNestedGroupBy modifier", () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(true);
      const modifier: DimensionModifier = {
        sqlExpression: "some_expression",
        dimension: { 
          type: "array",
          modifier: {}
        } as Dimension,
        key: "test_key",
        query: QUERY
      };
      expect(shouldUnnest(modifier)).toBe(false);
    });
    it("should return false when dimension when modifier undefined", () => {
        (isArrayTypeMember as jest.Mock).mockReturnValue(true);
        const modifier: DimensionModifier = {
          sqlExpression: "some_expression",
          dimension: { 
            type: "array",
          } as Dimension,
          key: "test_key",
          query: QUERY
        };
        expect(shouldUnnest(modifier)).toBe(false);
      });
  });

  describe("getModifiedSqlExpression", () => {
    it("should not modify if no modifiers passed", () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(true);
      const input = {
        sqlExpression: "array_field",
        dimension: {
          type: "array",
          modifier: { unNestedGroupBy: true }
        } as Dimension,
        query: QUERY,
        key: "test_key",
        modifiers: []
      };
      expect(getModifiedSqlExpression(input)).toBe("array_field");
    });
    it("should apply the modifier when conditions are met", () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(true);
      const input = {
        sqlExpression: "array_field",
        dimension: {
          type: "array",
          modifier: { unNestedGroupBy: true }
        } as Dimension,
        query: QUERY,
        key: "test_key",
        modifiers: MODIFIERS
      };
      expect(getModifiedSqlExpression(input)).toBe("array[unnest(array_field)]");
    });

    it("should not apply the modifier when conditions are not met", () => {
      (isArrayTypeMember as jest.Mock).mockReturnValue(false);
      const input = {
        sqlExpression: "non_array_field",
        dimension: {
          type: "string",
          modifier: {}
        } as Dimension,
        query: QUERY,
        key: "test_key",
        modifiers: MODIFIERS
      };
      expect(getModifiedSqlExpression(input)).toBe("non_array_field");
    });
  });
});