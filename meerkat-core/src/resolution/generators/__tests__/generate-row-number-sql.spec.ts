import { generateRowNumberSql } from '../generate-row-number-sql';

describe('generate-row-number-sql', () => {
  describe('generateRowNumberSql', () => {
    const baseTableName = '__base_query';

    it('should generate empty ORDER BY when no order specified', () => {
      const query = {};
      const dimensions: { name: string; alias?: string }[] = [];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe('row_number() OVER ()');
    });

    it('should generate ORDER BY with converted member key (underscores)', () => {
      const query = { order: { 'orders.customer_id': 'asc' } };
      const dimensions = [{ name: 'orders__customer_id' }];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."orders__customer_id" ASC)'
      );
    });

    it('should use dimension alias when available', () => {
      const query = { order: { 'orders.customer_id': 'desc' } };
      const dimensions = [
        { name: 'orders__customer_id', alias: 'Customer ID' },
      ];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."Customer ID" DESC)'
      );
    });

    it('should handle multiple order clauses', () => {
      const query = {
        order: {
          'orders.customer_id': 'asc',
          'orders.total': 'desc',
        },
      };
      const dimensions = [
        { name: 'orders__customer_id' },
        { name: 'orders__total' },
      ];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."orders__customer_id" ASC, __base_query."orders__total" DESC)'
      );
    });

    it('should use safe member when dimension not found', () => {
      const query = { order: { 'orders.unknown_field': 'asc' } };
      const dimensions: { name: string; alias?: string }[] = [];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."orders__unknown_field" ASC)'
      );
    });

    it('should handle nested dot notation', () => {
      const query = { order: { 'orders.customer.nested_id': 'asc' } };
      const dimensions = [{ name: 'orders__customer__nested_id' }];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."orders__customer__nested_id" ASC)'
      );
    });

    it('should find dimension by alias match', () => {
      const query = { order: { 'orders.customer_id': 'asc' } };
      const dimensions = [
        { name: 'some_other_name', alias: 'orders__customer_id' },
      ];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."orders__customer_id" ASC)'
      );
    });

    it('should handle empty order object', () => {
      const query = { order: {} };
      const dimensions: { name: string; alias?: string }[] = [];

      const result = generateRowNumberSql(query, dimensions, '__base_query');

      expect(result).toBe('row_number() OVER ()');
    });

    it('should uppercase direction', () => {
      const query = { order: { 'orders.field': 'ASC' } };
      const dimensions = [{ name: 'orders__field' }];

      const result = generateRowNumberSql(query, dimensions, '__base_query');

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."orders__field" ASC)'
      );
    });

    it('should lowercase direction before uppercasing', () => {
      const query = { order: { 'orders.field': 'Desc' } };
      const dimensions = [{ name: 'orders__field' }];

      const result = generateRowNumberSql(query, dimensions, '__base_query');

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."orders__field" DESC)'
      );
    });

    it('should use different base table name', () => {
      const query = { order: { 'orders.id': 'asc' } };
      const dimensions = [{ name: 'orders__id' }];

      const result = generateRowNumberSql(query, dimensions, 'custom_table');

      expect(result).toBe(
        'row_number() OVER (ORDER BY custom_table."orders__id" ASC)'
      );
    });

    it('should prefer name over alias when both match', () => {
      const query = { order: { 'orders.customer_id': 'asc' } };
      const dimensions = [
        { name: 'orders__customer_id', alias: 'Different Alias' },
      ];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."Different Alias" ASC)'
      );
    });

    it('should handle three order clauses', () => {
      const query = {
        order: {
          'orders.date': 'desc',
          'orders.customer_id': 'asc',
          'orders.priority': 'desc',
        },
      };
      const dimensions = [
        { name: 'orders__date' },
        { name: 'orders__customer_id' },
        { name: 'orders__priority' },
      ];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."orders__date" DESC, __base_query."orders__customer_id" ASC, __base_query."orders__priority" DESC)'
      );
    });

    it('should handle dimension with underscore in original name', () => {
      const query = { order: { 'orders.customer_nested_id': 'asc' } };
      const dimensions = [{ name: 'orders__customer_nested_id' }];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."orders__customer_nested_id" ASC)'
      );
    });

    it('should handle mixed found and not found dimensions', () => {
      const query = {
        order: {
          'orders.known_field': 'asc',
          'orders.unknown_field': 'desc',
        },
      };
      const dimensions = [{ name: 'orders__known_field', alias: 'Known Field' }];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."Known Field" ASC, __base_query."orders__unknown_field" DESC)'
      );
    });

    it('should handle dimension name without alias', () => {
      const query = { order: { 'data.value': 'asc' } };
      const dimensions = [{ name: 'data__value' }];

      const result = generateRowNumberSql(query, dimensions, baseTableName);

      expect(result).toBe(
        'row_number() OVER (ORDER BY __base_query."data__value" ASC)'
      );
    });

    it('should handle undefined order property', () => {
      const query = { order: undefined };
      const dimensions: { name: string; alias?: string }[] = [];

      const result = generateRowNumberSql(
        query as { order?: Record<string, string> },
        dimensions,
        baseTableName
      );

      expect(result).toBe('row_number() OVER ()');
    });
  });
});
