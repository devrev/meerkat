import { DatasetRegistry } from './dataset-registry';
import { Dataset } from './types';

describe('DatasetRegistry', () => {
  let registry: DatasetRegistry;
  let sampleDatasets: Dataset[];

  beforeEach(() => {
    registry = new DatasetRegistry();

    sampleDatasets = [
      {
        id: 'dataset-1',
        name: 'Sales Data',
        columns: [
          {
            name: 'date',
            dataType: 'time',
            schema: { format: 'YYYY-MM-DD' },
          },
          {
            name: 'revenue',
            dataType: 'number',
            schema: { precision: 2 },
          },
        ],
      },
      {
        id: 'dataset-2',
        name: 'User Metrics',
        columns: [
          {
            name: 'user_id',
            dataType: 'string',
          },
          {
            name: 'visits',
            dataType: 'number',
          },
        ],
      },
      {
        id: 'dataset-3',
        name: 'Empty Dataset',
        columns: [],
      },
    ];
  });

  describe('registerDataset', () => {
    it('should handle registration of multiple datasets', () => {
      sampleDatasets.forEach((dataset) => {
        registry.registerDataset(dataset);
      });

      expect(registry.getDatasets()).toHaveLength(sampleDatasets.length);
      expect(registry.getDatasets()).toEqual(
        expect.arrayContaining(sampleDatasets)
      );
    });

    it('should handle datasets with special characters in IDs', () => {
      const specialDataset: Dataset = {
        id: 'special@#$%^&*()',
        name: 'Special Dataset',
        columns: [],
      };

      registry.registerDataset(specialDataset);
      expect(registry.getDataset(specialDataset.id)).toEqual(specialDataset);
    });

    it('should handle datasets with empty names', () => {
      const emptyNameDataset: Dataset = {
        id: 'empty-name',
        name: '',
        columns: [],
      };

      registry.registerDataset(emptyNameDataset);
      expect(registry.getDataset(emptyNameDataset.id)).toEqual(
        emptyNameDataset
      );
    });

    it('should maintain column schema information correctly', () => {
      const complexDataset: Dataset = {
        id: 'complex',
        name: 'Complex Dataset',
        columns: [
          {
            name: 'nested',
            dataType: 'string',
            schema: {
              properties: {
                field1: { type: 'string' },
                field2: { type: 'number' },
              },
            },
          },
        ],
      };

      registry.registerDataset(complexDataset);
      const retrieved = registry.getDataset(complexDataset.id);
      expect(retrieved?.columns[0].schema).toEqual(
        complexDataset.columns[0].schema
      );
    });
  });

  describe('getDataset', () => {
    beforeEach(() => {
      sampleDatasets.forEach((dataset) => registry.registerDataset(dataset));
    });

    it('should handle case-sensitive dataset IDs', () => {
      const upperCaseId = 'DATASET-1';
      expect(registry.getDataset(upperCaseId)).toBeUndefined();
      expect(registry.getDataset('dataset-1')).toBeDefined();
    });

    it('should handle concurrent access to datasets', async () => {
      const promises = sampleDatasets.map((dataset) =>
        Promise.resolve(registry.getDataset(dataset.id))
      );

      const results = await Promise.all(promises);
      expect(results).toEqual(expect.arrayContaining(sampleDatasets));
    });
  });

  describe('getDatasets', () => {
    it('should maintain dataset order consistently', () => {
      sampleDatasets.forEach((dataset) => registry.registerDataset(dataset));

      const firstRetrieval = registry.getDatasets();
      const secondRetrieval = registry.getDatasets();

      expect(firstRetrieval).toEqual(secondRetrieval);
    });

    it('should handle large number of datasets efficiently', () => {
      const largeNumber = 1000;
      const largeDatasetsArray = Array.from(
        { length: largeNumber },
        (_, index) => ({
          id: `large-dataset-${index}`,
          name: `Large Dataset ${index}`,
          columns: [],
        })
      );

      largeDatasetsArray.forEach((dataset) =>
        registry.registerDataset(dataset)
      );
      expect(registry.getDatasets()).toHaveLength(largeNumber);
    });
  });

  describe('deleteDataset', () => {
    beforeEach(() => {
      sampleDatasets.forEach((dataset) => registry.registerDataset(dataset));
    });

    it('should handle deletion of multiple datasets sequentially', () => {
      sampleDatasets.forEach((dataset) => {
        registry.deleteDataset(dataset.id);
        expect(registry.getDataset(dataset.id)).toBeUndefined();
      });
      expect(registry.getDatasets()).toHaveLength(0);
    });

    it('should maintain integrity of remaining datasets after deletion', () => {
      registry.deleteDataset(sampleDatasets[0].id);
      const remainingDatasets = registry.getDatasets();

      expect(remainingDatasets).toHaveLength(sampleDatasets.length - 1);
      expect(remainingDatasets).not.toContainEqual(sampleDatasets[0]);
      expect(remainingDatasets).toContainEqual(sampleDatasets[1]);
    });

    it('should handle repeated deletions of same dataset', () => {
      registry.deleteDataset('dataset-1');
      registry.deleteDataset('dataset-1');
      registry.deleteDataset('dataset-1');

      expect(registry.getDatasets()).toHaveLength(sampleDatasets.length - 1);
    });
  });

  describe('clear', () => {
    it('should handle clear operations on partially filled registry', () => {
      registry.registerDataset(sampleDatasets[0]);
      registry.clear();
      registry.registerDataset(sampleDatasets[1]);

      expect(registry.getDatasets()).toHaveLength(1);
      expect(registry.getDataset(sampleDatasets[1].id)).toEqual(
        sampleDatasets[1]
      );
    });

    it('should handle multiple clear operations', () => {
      sampleDatasets.forEach((dataset) => registry.registerDataset(dataset));
      registry.clear();
      registry.clear();
      registry.clear();

      expect(registry.getDatasets()).toHaveLength(0);
    });
  });

  describe('findColumnInDataset', () => {
    let testDataset: Dataset;

    beforeEach(() => {
      testDataset = {
        id: 'test-dataset',
        name: 'Test Dataset',
        columns: [
          {
            name: 'column1',
            dataType: 'string',
          },
          {
            name: 'column2',
            dataType: 'number',
            schema: { precision: 2 },
          },
        ],
      };
      // Register the dataset before each test
      registry.registerDataset(testDataset);
    });

    it('should find an existing column', () => {
      const column = registry.findColumnInDataset({
        datasetId: 'test-dataset',
        columnName: 'column1',
      });
      expect(column).toEqual(testDataset.columns[0]);
    });

    it('should find a column with schema', () => {
      const column = registry.findColumnInDataset({
        datasetId: 'test-dataset',
        columnName: 'column2',
      });
      expect(column).toEqual(testDataset.columns[1]);
    });

    it('should throw error for non-existent column', () => {
      expect(() => {
        registry.findColumnInDataset({
          datasetId: 'test-dataset',
          columnName: 'nonexistent',
        });
      }).toThrow('Column nonexistent not found in dataset test-dataset');
    });

    it('should throw error for non-existent dataset', () => {
      expect(() => {
        registry.findColumnInDataset({
          datasetId: 'non-existent-dataset',
          columnName: 'column1',
        });
      }).toThrow('Dataset non-existent-dataset not found');
    });

    it('should handle case-sensitive column names', () => {
      expect(() => {
        registry.findColumnInDataset({
          datasetId: 'test-dataset',
          columnName: 'COLUMN1',
        });
      }).toThrow('Column COLUMN1 not found in dataset test-dataset');
    });
  });
});
