import { Query } from '../../../types/cube-types/query';
import { ResolutionConfig } from '../../types';
import { generateResolvedDimensions } from '../generate-resolved-dimensions';

describe('generate-resolved-dimensions', () => {
  describe('generateResolvedDimensions', () => {
    const baseDataSourceName = '__base_query';

    it('should generate dimension key with underscores when no column config', () => {
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual(['__base_query.orders__customer_id']);
    });

    it('should generate resolved dimension keys with underscores', () => {
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['display_name', 'email'],
          },
        ],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual([
        'orders__customer_id.orders__customer_id__display_name',
        'orders__customer_id.orders__customer_id__email',
      ]);
    });

    it('should handle measures and dimensions together', () => {
      const query: Query = {
        measures: ['orders.total'],
        dimensions: ['orders.customer_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual([
        '__base_query.orders__total',
        '__base_query.orders__customer_id',
      ]);
    });

    it('should use column projections when provided', () => {
      const query: Query = {
        measures: ['orders.total'],
        dimensions: ['orders.customer_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };
      const columnProjections = ['orders.status'];

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config,
        columnProjections
      );

      expect(result).toEqual(['__base_query.orders__status']);
    });

    it('should handle nested dot notation', () => {
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer.nested_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual(['__base_query.orders__customer__nested_id']);
    });

    it('should handle mixed resolved and unresolved dimensions', () => {
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id', 'orders.status'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['name'],
          },
        ],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual([
        'orders__customer_id.orders__customer_id__name',
        '__base_query.orders__status',
      ]);
    });

    it('should return empty array when no dimensions or measures', () => {
      const query: Query = {
        measures: [],
        dimensions: [],
      };
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual([]);
    });

    it('should handle undefined dimensions array', () => {
      const query: Query = {
        measures: ['orders.total'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual(['__base_query.orders__total']);
    });

    it('should handle multiple column configs for different dimensions', () => {
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id', 'orders.product_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['name'],
          },
          {
            name: 'orders.product_id',
            type: 'string',
            source: 'products',
            joinColumn: 'id',
            resolutionColumns: ['title'],
          },
        ],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual([
        'orders__customer_id.orders__customer_id__name',
        'orders__product_id.orders__product_id__title',
      ]);
    });

    it('should preserve order of dimensions and measures', () => {
      const query: Query = {
        measures: ['orders.amount', 'orders.quantity'],
        dimensions: ['orders.date', 'orders.status'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual([
        '__base_query.orders__amount',
        '__base_query.orders__quantity',
        '__base_query.orders__date',
        '__base_query.orders__status',
      ]);
    });

    it('should handle column projections with resolution configs', () => {
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.customer_id',
            type: 'string',
            source: 'customers',
            joinColumn: 'id',
            resolutionColumns: ['name', 'email'],
          },
        ],
        tableSchemas: [],
      };
      const columnProjections = ['orders.customer_id'];

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config,
        columnProjections
      );

      expect(result).toEqual([
        'orders__customer_id.orders__customer_id__name',
        'orders__customer_id.orders__customer_id__email',
      ]);
    });

    it('should use different base data source name', () => {
      const customBaseName = 'custom_base';
      const query: Query = {
        measures: [],
        dimensions: ['orders.customer_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(customBaseName, query, config);

      expect(result).toEqual(['custom_base.orders__customer_id']);
    });

    it('should handle empty column projections array', () => {
      const query: Query = {
        measures: ['orders.total'],
        dimensions: ['orders.customer_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [],
        tableSchemas: [],
      };
      const columnProjections: string[] = [];

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config,
        columnProjections
      );

      expect(result).toEqual([]);
    });

    it('should flatten multiple resolution columns for a single dimension', () => {
      const query: Query = {
        measures: [],
        dimensions: ['orders.owner_id'],
      };
      const config: ResolutionConfig = {
        columnConfigs: [
          {
            name: 'orders.owner_id',
            type: 'number',
            source: 'users',
            joinColumn: 'id',
            resolutionColumns: ['first_name', 'last_name', 'email'],
          },
        ],
        tableSchemas: [],
      };

      const result = generateResolvedDimensions(
        baseDataSourceName,
        query,
        config
      );

      expect(result).toEqual([
        'orders__owner_id.orders__owner_id__first_name',
        'orders__owner_id.orders__owner_id__last_name',
        'orders__owner_id.orders__owner_id__email',
      ]);
    });
  });
});
