import { DatasetRegistry } from '../dataset-registry/dataset-registry';
import { Column, Dataset } from '../dataset-registry/types';
import { DimensionType } from '../types/cube-types';
import { ColumnCompatibilityAnalyzer } from './column-compatibility-analyzer';
import {
  NAME_EXACT_MATCH,
  NAME_PARTIAL_MATCH,
  SCHEMA_COMPATIBILITY_MATCH,
  TYPE_COMPATIBILITY_MATCH,
} from './constants';

class TestableColumnCompatibilityAnalyzer extends ColumnCompatibilityAnalyzer {
  public testGetTypeCompatibilityScore(
    sourceType: DimensionType,
    targetType: DimensionType
  ): number {
    return this['getTypeCompatibilityScore'](sourceType, targetType);
  }

  public testGetNameSimilarityScore(
    sourceName: string,
    targetName: string
  ): number {
    return this['getNameSimilarityScore'](sourceName, targetName);
  }

  public testGetSchemaCompatibilityScore(
    sourceColumn: Column,
    targetColumn: Column
  ): number {
    return this['getSchemaCompatibilityScore'](sourceColumn, targetColumn);
  }

  public testNormalizeColumnName(name: string): string {
    return this['normalizeColumnName'](name);
  }

  public testAssessCompatibility(sourceColumn: Column, targetColumn: Column) {
    return this['assessCompatibilityScore'](sourceColumn, targetColumn);
  }
}

describe('ColumnCompatibilityAnalyzer', () => {
  let compatibleAnalyzer: ColumnCompatibilityAnalyzer;
  let mockRegistry: DatasetRegistry;

  const mockDatasets: Dataset[] = [
    {
      id: 'dataset1',
      name: 'Dataset 1',
      columns: [
        {
          name: 'user_id',
          dataType: 'number',
          schema: { type: 'integer' },
        },
        {
          name: 'email',
          dataType: 'string',
          schema: { type: 'string', format: 'email' },
        },
      ],
    },
    {
      id: 'dataset2',
      name: 'Dataset 2',
      columns: [
        {
          name: 'userId',
          dataType: 'number',
          schema: { type: 'integer' },
        },
        {
          name: 'name',
          dataType: 'string',
          schema: { type: 'string' },
        },
      ],
    },
  ];

  beforeEach(() => {
    mockRegistry = new DatasetRegistry();
    mockDatasets.forEach((dataset) => mockRegistry.registerDataset(dataset));
    compatibleAnalyzer = new ColumnCompatibilityAnalyzer(mockRegistry);
  });

  describe('findCompatibleColumns', () => {
    it('should find compatible columns based on type, name, and schema', () => {
      const result = compatibleAnalyzer.findCompatibleColumns({
        sourceDatasetId: 'dataset1',
        sourceColumnName: 'user_id',
      });

      expect(result).toHaveLength(1);
      expect(result[0].column.name).toBe('userId');
      expect(result[0].dataset.id).toBe('dataset2');
    });

    it('should throw error when source column not found', () => {
      expect(() =>
        compatibleAnalyzer.findCompatibleColumns({
          sourceDatasetId: 'dataset1',
          sourceColumnName: 'unique_column',
        })
      ).toThrow('Column unique_column not found in dataset dataset1');
    });
  });

  describe('name similarity scoring', () => {
    it('should match exact names ignoring case and special characters', () => {
      const result = compatibleAnalyzer.findCompatibleColumns({
        sourceDatasetId: 'dataset1',
        sourceColumnName: 'user_id',
      });

      expect(result).toHaveLength(1);
      expect(result[0].column.name).toBe('userId');
    });
  });

  describe('schema compatibility', () => {
    beforeEach(() => {
      mockRegistry.registerDataset({
        id: 'dataset3',
        name: 'Dataset 3',
        columns: [
          {
            name: 'email',
            dataType: 'string',
            schema: { type: 'string', format: 'email' },
          },
        ],
      });
    });

    it('should consider schema when scoring compatibility', () => {
      const result = compatibleAnalyzer.findCompatibleColumns({
        sourceDatasetId: 'dataset1',
        sourceColumnName: 'email',
      });

      expect(result).toHaveLength(2);
      expect(result[0].column.name).toBe('email');
      expect(result[0].dataset.id).toBe('dataset3');
    });

    it('should handle missing schema gracefully', () => {
      const noSchemaDataset: Dataset = {
        id: 'dataset4',
        name: 'Dataset 4',
        columns: [
          {
            name: 'id',
            dataType: 'number',
          },
        ],
      };
      mockRegistry.registerDataset(noSchemaDataset);

      const result = compatibleAnalyzer.findCompatibleColumns({
        sourceDatasetId: 'dataset4',
        sourceColumnName: 'id',
      });

      expect(result).toHaveLength(2);
    });
  });

  describe('ColumnCompatibilityAnalyzer PRIVATE METHODS', () => {
    let analyzer: TestableColumnCompatibilityAnalyzer;
    let registry: DatasetRegistry;

    beforeEach(() => {
      registry = new DatasetRegistry();
      analyzer = new TestableColumnCompatibilityAnalyzer(registry);
    });

    describe('getTypeCompatibilityScore', () => {
      it('should return full score for matching types', () => {
        expect(analyzer.testGetTypeCompatibilityScore('string', 'string')).toBe(
          TYPE_COMPATIBILITY_MATCH
        );
        expect(analyzer.testGetTypeCompatibilityScore('number', 'number')).toBe(
          TYPE_COMPATIBILITY_MATCH
        );
      });

      it('should return 0 for different types', () => {
        expect(analyzer.testGetTypeCompatibilityScore('string', 'number')).toBe(
          0
        );
        expect(
          analyzer.testGetTypeCompatibilityScore('boolean', 'string')
        ).toBe(0);
      });
    });

    describe('getNameSimilarityScore', () => {
      it('should return exact match score for identical names', () => {
        expect(analyzer.testGetNameSimilarityScore('user_id', 'user_id')).toBe(
          NAME_EXACT_MATCH
        );
        expect(analyzer.testGetNameSimilarityScore('userId', 'userId')).toBe(
          NAME_EXACT_MATCH
        );
      });

      it('should return partial match score for similar names', () => {
        expect(analyzer.testGetNameSimilarityScore('user_id', 'userId')).toBe(
          NAME_EXACT_MATCH
        );
        expect(analyzer.testGetNameSimilarityScore('customer_id', 'id')).toBe(
          NAME_PARTIAL_MATCH
        );
      });

      it('should return 0 for different names', () => {
        expect(
          analyzer.testGetNameSimilarityScore('user_id', 'product_name')
        ).toBe(0);
      });
    });

    describe('getSchemaCompatibilityScore', () => {
      it('should return full score for matching schemas', () => {
        const schema1 = { type: 'string', length: 255 };
        const column1: Column = {
          name: 'test1',
          dataType: 'string',
          schema: schema1,
        };
        const column2: Column = {
          name: 'test2',
          dataType: 'string',
          schema: schema1,
        };

        expect(analyzer.testGetSchemaCompatibilityScore(column1, column2)).toBe(
          SCHEMA_COMPATIBILITY_MATCH
        );
      });

      it('should return 0 for different schemas', () => {
        const column1: Column = {
          name: 'test1',
          dataType: 'string',
          schema: { type: 'string', length: 255 },
        };
        const column2: Column = {
          name: 'test2',
          dataType: 'string',
          schema: { type: 'string', length: 100 },
        };

        expect(analyzer.testGetSchemaCompatibilityScore(column1, column2)).toBe(
          0
        );
      });

      it('should return 0 when schemas are missing', () => {
        const column1: Column = { name: 'test1', dataType: 'string' };
        const column2: Column = { name: 'test2', dataType: 'string' };

        expect(analyzer.testGetSchemaCompatibilityScore(column1, column2)).toBe(
          0
        );
      });
    });

    describe('normalizeColumnName', () => {
      it('should convert to lowercase and remove special characters', () => {
        expect(analyzer.testNormalizeColumnName('User_ID')).toBe('userid');
        expect(analyzer.testNormalizeColumnName('customer-id')).toBe(
          'customerid'
        );
        expect(analyzer.testNormalizeColumnName('ProductName')).toBe(
          'productname'
        );
      });
    });

    describe('assessCompatibilityScore', () => {
      it('should calculate total compatibility score correctly', () => {
        const column1: Column = {
          name: 'user_id',
          dataType: 'string',
          schema: { type: 'string', length: 255 },
        };
        const column2: Column = {
          name: 'user_id',
          dataType: 'string',
          schema: { type: 'string', length: 255 },
        };

        const result = analyzer.testAssessCompatibility(column1, column2);

        expect(result).toEqual(
          TYPE_COMPATIBILITY_MATCH +
            NAME_EXACT_MATCH +
            SCHEMA_COMPATIBILITY_MATCH
        );
      });

      it('should return default score when types do not match', () => {
        const column1: Column = { name: 'test1', dataType: 'string' };
        const column2: Column = { name: 'test1', dataType: 'number' };

        const result = analyzer.testAssessCompatibility(column1, column2);

        expect(result).toBe(0);
      });
    });
  });
});
