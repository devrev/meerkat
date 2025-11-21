/**
 * Advanced Date/Time Operations Tests
 * 
 * Tests advanced date/time functionality:
 * - INTERVAL arithmetic
 * - AGE function
 * - Timezone handling
 * - Current date/time functions (CURRENT_DATE, NOW())
 * - TO_TIMESTAMP / TO_DATE conversions
 * - Additional date parts (EPOCH, DOY, QUARTER)
 * - Date overlap calculations
 * - Date range queries
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Advanced Date/Time Operations', () => {
  beforeAll(async () => {
    console.log('🚀 Starting advanced date/time tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('INTERVAL Arithmetic', () => {
    it('should add days using INTERVAL', async () => {
      const sql = `
        SELECT 
          created_date,
          created_date + INTERVAL '7 days' as plus_7_days,
          created_date + INTERVAL '1 week' as plus_1_week
        FROM fact_all_types
        WHERE id_bigint < 5
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        const original = new Date(row.created_date);
        const plus7 = new Date(row.plus_7_days);
        const plus1week = new Date(row.plus_1_week);
        
        const diffDays = (plus7.getTime() - original.getTime()) / (1000 * 60 * 60 * 24);
        expect(Math.abs(diffDays - 7)).toBeLessThan(1);
        
        // plus_7_days and plus_1_week should be the same
        expect(plus7.getTime()).toBe(plus1week.getTime());
      });
    });

    it('should subtract months using INTERVAL', async () => {
      const sql = `
        SELECT 
          created_date,
          created_date - INTERVAL '1 month' as minus_1_month,
          created_date - INTERVAL '30 days' as minus_30_days
        FROM fact_all_types
        WHERE id_bigint < 5
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        const original = new Date(row.created_date);
        const minus1month = new Date(row.minus_1_month);
        const minus30days = new Date(row.minus_30_days);
        
        // Both should be earlier than original
        expect(minus1month.getTime()).toBeLessThan(original.getTime());
        expect(minus30days.getTime()).toBeLessThan(original.getTime());
      });
    });

    it('should use INTERVAL in WHERE clause', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE created_date >= DATE '2020-01-01'
          AND created_date < DATE '2020-01-01' + INTERVAL '90 days'
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should add hours and minutes to timestamps', async () => {
      const sql = `
        SELECT 
          created_timestamp,
          created_timestamp + INTERVAL '2 hours' as plus_2_hours,
          created_timestamp + INTERVAL '30 minutes' as plus_30_min
        FROM fact_all_types
        WHERE id_bigint < 5
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        expect(row.created_timestamp).toBeTruthy();
        expect(row.plus_2_hours).toBeTruthy();
        expect(row.plus_30_min).toBeTruthy();
      });
    });
  });

  describe('AGE Function', () => {
    it('should calculate age between two timestamps', async () => {
      const sql = `
        SELECT 
          id_bigint,
          created_timestamp,
          identified_timestamp,
          AGE(identified_timestamp, created_timestamp) as time_diff
        FROM fact_all_types
        WHERE id_bigint < 10
          AND created_timestamp IS NOT NULL
          AND identified_timestamp IS NOT NULL
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(row.time_diff).toBeTruthy();
      });
    });

    it('should calculate age from current time', async () => {
      const sql = `
        SELECT 
          created_date,
          AGE(created_date) as age_from_now
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.age_from_now).toBeTruthy();
      });
    });
  });

  describe('Current Date/Time Functions', () => {
    it('should use CURRENT_DATE', async () => {
      const sql = `
        SELECT 
          CURRENT_DATE as today,
          COUNT(*) as count
        FROM fact_all_types
        WHERE created_date < CURRENT_DATE
          AND id_bigint < 1000
      `;
      const result = await duckdbExec(sql);

      expect(result[0].today).toBeTruthy();
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use NOW() and CURRENT_TIMESTAMP', async () => {
      const sql = `
        SELECT 
          NOW() as current_time,
          CURRENT_TIMESTAMP as current_ts,
          COUNT(*) as count
        FROM fact_all_types
        WHERE created_timestamp < NOW()
          AND id_bigint < 1000
      `;
      const result = await duckdbExec(sql);

      expect(result[0].current_time).toBeTruthy();
      expect(result[0].current_ts).toBeTruthy();
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should calculate days since using CURRENT_DATE', async () => {
      const sql = `
        SELECT 
          created_date,
          CURRENT_DATE - created_date as days_since
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        // days_since should be a positive number
        expect(Number(row.days_since)).toBeGreaterThanOrEqual(0);
      });
    });
  });

  describe('Date Part Extraction', () => {
    it('should extract EPOCH (Unix timestamp)', async () => {
      const sql = `
        SELECT 
          created_timestamp,
          EXTRACT(EPOCH FROM created_timestamp) as epoch_seconds
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(Number(row.epoch_seconds)).toBeGreaterThan(0);
      });
    });

    it('should extract DOY (day of year)', async () => {
      const sql = `
        SELECT 
          created_date,
          EXTRACT(DOY FROM created_date) as day_of_year
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const doy = Number(row.day_of_year);
        expect(doy).toBeGreaterThanOrEqual(1);
        expect(doy).toBeLessThanOrEqual(366);
      });
    });

    it('should extract QUARTER', async () => {
      const sql = `
        SELECT 
          created_date,
          EXTRACT(QUARTER FROM created_date) as quarter
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(100);
      result.forEach((row) => {
        const quarter = Number(row.quarter);
        expect(quarter).toBeGreaterThanOrEqual(1);
        expect(quarter).toBeLessThanOrEqual(4);
      });
    });

    it('should extract WEEK', async () => {
      const sql = `
        SELECT 
          created_date,
          EXTRACT(WEEK FROM created_date) as week_number
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const week = Number(row.week_number);
        expect(week).toBeGreaterThanOrEqual(1);
        expect(week).toBeLessThanOrEqual(53);
      });
    });
  });

  describe('String to Date/Time Conversions', () => {
    it('should convert string to DATE using CAST', async () => {
      const sql = `
        SELECT 
          CAST('2024-06-15' AS DATE) as parsed_date,
          COUNT(*) as count
        FROM fact_all_types
        WHERE created_date >= CAST('2020-01-01' AS DATE)
          AND id_bigint < 1000
      `;
      const result = await duckdbExec(sql);

      expect(result[0].parsed_date).toBeTruthy();
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should convert string to TIMESTAMP', async () => {
      const sql = `
        SELECT 
          CAST('2024-06-15 12:30:45' AS TIMESTAMP) as parsed_ts,
          COUNT(*) as count
        FROM fact_all_types
        WHERE created_timestamp >= CAST('2020-01-01 00:00:00' AS TIMESTAMP)
          AND id_bigint < 1000
      `;
      const result = await duckdbExec(sql);

      expect(result[0].parsed_ts).toBeTruthy();
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use STRPTIME for custom date formats', async () => {
      const sql = `
        SELECT STRPTIME('15-06-2024', '%d-%m-%Y') as parsed_date
      `;
      const result = await duckdbExec(sql);

      expect(result[0].parsed_date).toBeTruthy();
    });
  });

  describe('Date Range and Overlap Queries', () => {
    it('should find records within date range', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE created_date BETWEEN DATE '2020-03-01' AND DATE '2020-06-30'
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should calculate date ranges using INTERVAL', async () => {
      const sql = `
        SELECT 
          DATE '2020-01-01' as start_date,
          DATE '2020-01-01' + INTERVAL '90 days' as end_date,
          COUNT(*) as count
        FROM fact_all_types
        WHERE created_date >= DATE '2020-01-01'
          AND created_date < DATE '2020-01-01' + INTERVAL '90 days'
      `;
      const result = await duckdbExec(sql);

      expect(result[0].start_date).toBeTruthy();
      expect(result[0].end_date).toBeTruthy();
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should find overlapping date periods', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types f1
        WHERE EXISTS (
          SELECT 1
          FROM fact_all_types f2
          WHERE f2.id_bigint != f1.id_bigint
            AND f2.created_date <= f1.record_date
            AND f1.created_date <= f2.record_date
            AND f1.id_bigint < 100
            AND f2.id_bigint < 100
        )
      `;
      const result = await duckdbExec(sql);

      expect(result[0].count).toBeDefined();
    });
  });

  describe('Complex Date Calculations', () => {
    it('should calculate business days (weekdays)', async () => {
      const sql = `
        SELECT 
          created_date,
          EXTRACT(DOW FROM created_date) as day_of_week,
          CASE 
            WHEN EXTRACT(DOW FROM created_date) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
          END as day_type
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 20
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(20);
      result.forEach((row) => {
        const dow = Number(row.day_of_week);
        expect(dow).toBeGreaterThanOrEqual(0);
        expect(dow).toBeLessThanOrEqual(6);
        expect(['Weekend', 'Weekday']).toContain(row.day_type);
      });
    });

    it('should group by calendar quarter', async () => {
      const sql = `
        SELECT 
          EXTRACT(YEAR FROM created_date) as year,
          EXTRACT(QUARTER FROM created_date) as quarter,
          COUNT(*) as count
        FROM fact_all_types
        WHERE id_bigint < 10000
        GROUP BY year, quarter
        ORDER BY year, quarter
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(Number(row.year)).toBeGreaterThanOrEqual(2020);
        expect(Number(row.quarter)).toBeGreaterThanOrEqual(1);
        expect(Number(row.quarter)).toBeLessThanOrEqual(4);
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });

    it('should calculate date differences in various units', async () => {
      const sql = `
        SELECT 
          created_timestamp,
          identified_timestamp,
          EXTRACT(EPOCH FROM (identified_timestamp - created_timestamp)) as diff_seconds,
          EXTRACT(EPOCH FROM (identified_timestamp - created_timestamp)) / 60 as diff_minutes,
          EXTRACT(EPOCH FROM (identified_timestamp - created_timestamp)) / 3600 as diff_hours
        FROM fact_all_types
        WHERE id_bigint < 10
          AND created_timestamp IS NOT NULL
          AND identified_timestamp IS NOT NULL
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(row.diff_seconds).toBeDefined();
        expect(row.diff_minutes).toBeDefined();
        expect(row.diff_hours).toBeDefined();
      });
    });
  });

  describe('Performance', () => {
    it('should execute INTERVAL queries efficiently (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE created_date >= DATE '2020-01-01'
          AND created_date < DATE '2020-01-01' + INTERVAL '6 months'
          AND id_bigint < 100000
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });

    it('should execute complex date calculations efficiently (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          EXTRACT(YEAR FROM created_date) as year,
          EXTRACT(QUARTER FROM created_date) as quarter,
          COUNT(*) as count,
          AVG(EXTRACT(EPOCH FROM (identified_timestamp - created_timestamp))) as avg_diff
        FROM fact_all_types
        WHERE id_bigint < 100000
        GROUP BY year, quarter
        ORDER BY year, quarter
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });
  });
});

