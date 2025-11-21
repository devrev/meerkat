/**
 * Comprehensive SQL Functions Tests
 * 
 * Tests SQL function support in cube queries:
 * - Date/time functions (DATE_TRUNC, EXTRACT, date arithmetic)
 * - String functions (UPPER, LOWER, CONCAT, LENGTH)
 * - Aggregation functions (COUNT DISTINCT, SUM, AVG, MIN, MAX)
 * - Type casting and conversions
 * 
 * These tests verify that common SQL functions used in widget queries
 * are correctly translated and executed.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: SQL Functions', () => {
  beforeAll(async () => {
    console.log('🚀 Starting SQL function tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('Date/Time Functions', () => {
    it('should use DATE_TRUNC to truncate to month', async () => {
      const sql = `
        SELECT 
          DATE_TRUNC('month', created_date) as month,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY DATE_TRUNC('month', created_date)
        ORDER BY month
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(row.month).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use DATE_TRUNC to truncate to day', async () => {
      const sql = `
        SELECT 
          DATE_TRUNC('day', created_timestamp) as day,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY DATE_TRUNC('day', created_timestamp)
        ORDER BY day
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(row.day).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use DATE_TRUNC to truncate to year', async () => {
      const sql = `
        SELECT 
          DATE_TRUNC('year', created_date) as year,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY DATE_TRUNC('year', created_date)
        ORDER BY year
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(row.year).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use EXTRACT to get year from date', async () => {
      const sql = `
        SELECT 
          EXTRACT(YEAR FROM created_date) as year,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY EXTRACT(YEAR FROM created_date)
        ORDER BY year
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(Number(row.year)).toBeGreaterThan(2019);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use EXTRACT to get month from date', async () => {
      const sql = `
        SELECT 
          EXTRACT(MONTH FROM created_date) as month,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY EXTRACT(MONTH FROM created_date)
        ORDER BY month
      `;
      const result = await duckdbExec(sql);

      // Should have 12 months
      expect(result.length).toBe(12);

      result.forEach((row) => {
        expect(Number(row.month)).toBeGreaterThanOrEqual(1);
        expect(Number(row.month)).toBeLessThanOrEqual(12);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use EXTRACT to get day of week', async () => {
      const sql = `
        SELECT 
          EXTRACT(DOW FROM created_date) as day_of_week,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY EXTRACT(DOW FROM created_date)
        ORDER BY day_of_week
      `;
      const result = await duckdbExec(sql);

      // Should have 7 days (0=Sunday, 6=Saturday)
      expect(result.length).toBe(7);

      result.forEach((row) => {
        expect(Number(row.day_of_week)).toBeGreaterThanOrEqual(0);
        expect(Number(row.day_of_week)).toBeLessThanOrEqual(6);
      });
    });

    it('should use MONTHNAME function', async () => {
      const sql = `
        SELECT 
          MONTHNAME(created_date) as month_name,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY MONTHNAME(created_date)
        ORDER BY MIN(created_date)
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      const validMonths = [
        'January',
        'February',
        'March',
        'April',
        'May',
        'June',
        'July',
        'August',
        'September',
        'October',
        'November',
        'December',
      ];

      result.forEach((row) => {
        expect(validMonths).toContain(row.month_name);
      });
    });

    it('should use DAYNAME function', async () => {
      const sql = `
        SELECT 
          DAYNAME(created_date) as day_name,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY DAYNAME(created_date)
        ORDER BY MIN(created_date)
        LIMIT 7
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      const validDays = [
        'Monday',
        'Tuesday',
        'Wednesday',
        'Thursday',
        'Friday',
        'Saturday',
        'Sunday',
      ];

      result.forEach((row) => {
        expect(validDays).toContain(row.day_name);
      });
    });

    it('should perform date arithmetic (date difference)', async () => {
      const sql = `
        SELECT 
          (created_timestamp - identified_timestamp) as diff,
          COUNT(*) as count
        FROM fact_all_types
        WHERE created_timestamp IS NOT NULL
          AND identified_timestamp IS NOT NULL
        GROUP BY (created_timestamp - identified_timestamp)
        ORDER BY diff
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use AGE function for timestamp difference', async () => {
      const sql = `
        SELECT 
          COUNT(*) as count,
          AVG(EXTRACT(EPOCH FROM (created_timestamp - identified_timestamp))) as avg_diff_seconds
        FROM fact_all_types
        WHERE created_timestamp IS NOT NULL
          AND identified_timestamp IS NOT NULL
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
      expect(Number(result[0].count)).toBe(1000000);
    });
  });

  describe('String Functions', () => {
    it('should use UPPER function', async () => {
      const sql = `
        SELECT 
          UPPER(priority) as priority_upper,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY UPPER(priority)
        ORDER BY priority_upper
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(row.priority_upper).toBe(row.priority_upper.toUpperCase());
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use LOWER function', async () => {
      const sql = `
        SELECT 
          LOWER(status) as status_lower,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY LOWER(status)
        ORDER BY status_lower
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.status_lower).toBe(row.status_lower.toLowerCase());
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use LENGTH function', async () => {
      const sql = `
        SELECT 
          LENGTH(priority) as priority_length,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY LENGTH(priority)
        ORDER BY priority_length
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.priority_length)).toBeGreaterThan(0);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use CONCAT function', async () => {
      const sql = `
        SELECT 
          CONCAT(priority, '-', status) as combined,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY CONCAT(priority, '-', status)
        ORDER BY combined
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.combined).toContain('-');
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use SUBSTRING function', async () => {
      const sql = `
        SELECT 
          SUBSTRING(priority, 1, 3) as priority_prefix,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY SUBSTRING(priority, 1, 3)
        ORDER BY priority_prefix
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.priority_prefix.length).toBeLessThanOrEqual(3);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use TRIM function', async () => {
      const sql = `
        SELECT 
          TRIM(priority) as priority_trimmed,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY TRIM(priority)
        ORDER BY priority_trimmed
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Aggregate Functions', () => {
    it('should use COUNT DISTINCT', async () => {
      const sql = `
        SELECT 
          COUNT(DISTINCT user_id) as distinct_users,
          COUNT(DISTINCT part_id) as distinct_parts
        FROM fact_all_types
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
      expect(Number(result[0].distinct_users)).toBe(10000);
      expect(Number(result[0].distinct_parts)).toBe(5000);
    });

    it('should use SUM aggregate', async () => {
      const sql = `
        SELECT 
          priority,
          SUM(id_bigint) as total_ids,
          SUM(metric_double) as total_metric
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.total_ids)).toBeGreaterThan(0);
        expect(Number(row.total_metric)).toBeGreaterThan(0);
      });
    });

    it('should use AVG aggregate', async () => {
      const sql = `
        SELECT 
          status,
          AVG(id_bigint) as avg_id,
          AVG(metric_double) as avg_metric
        FROM fact_all_types
        GROUP BY status
        ORDER BY status
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(Number(row.avg_id)).toBeGreaterThan(0);
        expect(Number(row.avg_metric)).toBeGreaterThan(0);
      });
    });

    it('should use MIN and MAX aggregates', async () => {
      const sql = `
        SELECT 
          MIN(id_bigint) as min_id,
          MAX(id_bigint) as max_id,
          MIN(metric_double) as min_metric,
          MAX(metric_double) as max_metric,
          MIN(created_date) as min_date,
          MAX(created_date) as max_date
        FROM fact_all_types
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
      expect(Number(result[0].min_id)).toBe(0);
      expect(Number(result[0].max_id)).toBe(999999);
      expect(Number(result[0].min_metric)).toBeGreaterThan(0);
      expect(Number(result[0].max_metric)).toBeGreaterThan(0);
    });

    it('should use STDDEV aggregate', async () => {
      const sql = `
        SELECT 
          priority,
          STDDEV(metric_double) as stddev_metric
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.stddev_metric)).toBeGreaterThanOrEqual(0);
      });
    });

    it('should use MEDIAN aggregate', async () => {
      const sql = `
        SELECT 
          priority,
          MEDIAN(metric_double) as median_metric
        FROM fact_all_types
        GROUP BY priority
        ORDER BY priority
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);

      result.forEach((row) => {
        expect(Number(row.median_metric)).toBeGreaterThan(0);
      });
    });
  });

  describe('Type Casting and Conversions', () => {
    it('should cast numeric to string', async () => {
      const sql = `
        SELECT 
          CAST(id_bigint AS VARCHAR) as id_string,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 10
        GROUP BY CAST(id_bigint AS VARCHAR)
        ORDER BY id_string
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);

      result.forEach((row) => {
        expect(typeof row.id_string).toBe('string');
      });
    });

    it('should cast date to timestamp', async () => {
      const sql = `
        SELECT 
          CAST(created_date AS TIMESTAMP) as created_timestamp_casted,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY CAST(created_date AS TIMESTAMP)
        ORDER BY created_timestamp_casted
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.created_timestamp_casted).toBeTruthy();
      });
    });

    it('should cast string to numeric', async () => {
      const sql = `
        SELECT 
          CAST('123' AS BIGINT) as numeric_value,
          COUNT(*) as count
        FROM fact_all_types
        LIMIT 1
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);
      expect(Number(result[0].numeric_value)).toBe(123);
    });

    it('should use COALESCE for NULL handling', async () => {
      const sql = `
        SELECT 
          COALESCE(mitigated_date, created_date) as effective_date,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY COALESCE(mitigated_date, created_date)
        ORDER BY effective_date
        LIMIT 5
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);

      result.forEach((row) => {
        expect(row.effective_date).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Conditional Functions', () => {
    it('should use CASE WHEN for categorization', async () => {
      const sql = `
        SELECT 
          CASE 
            WHEN id_bigint < 100000 THEN 'low'
            WHEN id_bigint < 500000 THEN 'medium'
            ELSE 'high'
          END as id_category,
          COUNT(*) as count
        FROM fact_all_types
        GROUP BY 
          CASE 
            WHEN id_bigint < 100000 THEN 'low'
            WHEN id_bigint < 500000 THEN 'medium'
            ELSE 'high'
          END
        ORDER BY id_category
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);

      const categories = result.map((r) => r.id_category);
      expect(categories).toContain('low');
      expect(categories).toContain('medium');
      expect(categories).toContain('high');

      // Low: 100K, Medium: 400K, High: 500K
      result.forEach((row) => {
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should use NULLIF function', async () => {
      const sql = `
        SELECT 
          COUNT(NULLIF(priority, 'low')) as non_low_count,
          COUNT(*) as total_count
        FROM fact_all_types
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(1);

      const nonLowCount = Number(result[0].non_low_count);
      const totalCount = Number(result[0].total_count);

      // 'low' priority is 20% of rows
      expect(nonLowCount).toBeLessThan(totalCount);
      expect(nonLowCount).toBeGreaterThan(totalCount * 0.75);
    });
  });

  describe('Performance', () => {
    it('should execute complex SQL functions quickly (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          DATE_TRUNC('month', created_date) as month,
          UPPER(priority) as priority,
          COUNT(*) as count,
          AVG(metric_double) as avg_metric,
          SUM(id_bigint) as total_ids
        FROM fact_all_types
        GROUP BY DATE_TRUNC('month', created_date), UPPER(priority)
        ORDER BY month, priority
        LIMIT 50
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });
  });
});

