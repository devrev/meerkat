/**
 * Advanced String Operations Tests
 * 
 * Tests advanced string manipulation:
 * - LIKE with escape characters
 * - SIMILAR TO (SQL regex)
 * - REGEXP_MATCHES / REGEXP_REPLACE
 * - String splitting operations
 * - POSITION / STRPOS
 * - LPAD / RPAD
 * - REPLACE
 * - REVERSE
 * - Multi-byte characters (Unicode)
 * - Complex string patterns
 */

import { beforeAll, describe, expect, it } from 'vitest';
import { duckdbExec } from '../../duckdb-exec';
import {
  createAllSyntheticTables,
  dropSyntheticTables,
  verifySyntheticTables,
} from '../synthetic/schema-setup';

describe('Comprehensive: Advanced String Operations', () => {
  beforeAll(async () => {
    console.log('🚀 Starting advanced string operation tests...');
    await dropSyntheticTables();
    await createAllSyntheticTables();
    await verifySyntheticTables();
  }, 120000);

  describe('LIKE with Escape Characters', () => {
    it('should use LIKE with underscore wildcard', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM dim_user
        WHERE user_id LIKE 'user__'
      `;
      const result = await duckdbExec(sql);

      // Should match user_0 through user_9
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use LIKE with percent wildcard', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM dim_user
        WHERE user_email LIKE '%@example.com'
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should escape special characters in LIKE', async () => {
      // Testing with literal underscore
      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE description LIKE '%\\_%' ESCAPE '\\'
          AND id_bigint < 1000
      `;
      const result = await duckdbExec(sql);

      expect(result[0].count).toBeDefined();
    });
  });

  describe('Regular Expression Matching', () => {
    it('should use REGEXP_MATCHES for pattern matching', async () => {
      const sql = `
        SELECT 
          description,
          REGEXP_MATCHES(description, '[A-Za-z]+') as has_letters
        FROM fact_all_types
        WHERE id_bigint < 100
        ORDER BY id_bigint
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(typeof row.has_letters).toBe('boolean');
      });
    });

    it('should use REGEXP_MATCHES with complex patterns', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM dim_user
        WHERE REGEXP_MATCHES(user_email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
      `;
      const result = await duckdbExec(sql);

      // All emails should match valid email pattern
      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use REGEXP_REPLACE for substitution', async () => {
      const sql = `
        SELECT 
          user_id,
          REGEXP_REPLACE(user_id, '[0-9]+', 'XXX') as masked_id
        FROM dim_user
        WHERE user_id LIKE 'user_%'
        ORDER BY user_id
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.masked_id).toContain('XXX');
        expect(row.masked_id).not.toMatch(/[0-9]/);
      });
    });

    it('should use REGEXP_EXTRACT to extract patterns', async () => {
      const sql = `
        SELECT 
          user_email,
          REGEXP_EXTRACT(user_email, '@([a-z]+)\\.', 1) as domain
        FROM dim_user
        WHERE user_email LIKE '%@%'
        ORDER BY user_id
        LIMIT 10
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.domain).toBeTruthy();
      });
    });
  });

  describe('String Splitting', () => {
    it('should split strings using STRING_SPLIT', async () => {
      const sql = `
        SELECT 
          'apple,banana,orange' as fruits,
          STRING_SPLIT('apple,banana,orange', ',') as fruit_array
      `;
      const result = await duckdbExec(sql);

      expect(result[0].fruit_array).toBeDefined();
    });

    it('should split and unnest strings', async () => {
      const sql = `
        SELECT UNNEST(STRING_SPLIT('tag1,tag2,tag3', ',')) as tag
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);
      expect(result.map(r => r.tag)).toContain('tag1');
      expect(result.map(r => r.tag)).toContain('tag2');
      expect(result.map(r => r.tag)).toContain('tag3');
    });
  });

  describe('POSITION and STRPOS', () => {
    it('should find substring position with POSITION', async () => {
      const sql = `
        SELECT 
          description,
          POSITION('Test' IN description) as test_position
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        const pos = Number(row.test_position);
        expect(pos).toBeGreaterThanOrEqual(0);
      });
    });

    it('should find substring position with STRPOS', async () => {
      const sql = `
        SELECT 
          user_email,
          STRPOS(user_email, '@') as at_position
        FROM dim_user
        WHERE user_id IN ('user_0', 'user_1', 'user_2')
        ORDER BY user_id
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);
      result.forEach((row) => {
        expect(Number(row.at_position)).toBeGreaterThan(0);
      });
    });
  });

  describe('LPAD and RPAD', () => {
    it('should left-pad strings with LPAD', async () => {
      const sql = `
        SELECT 
          user_id,
          LPAD(user_id, 15, '0') as padded_id
        FROM dim_user
        WHERE user_id IN ('user_0', 'user_1', 'user_2')
        ORDER BY user_id
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);
      result.forEach((row) => {
        expect(row.padded_id.length).toBe(15);
        expect(row.padded_id).toMatch(/^0+/); // Starts with zeros
      });
    });

    it('should right-pad strings with RPAD', async () => {
      const sql = `
        SELECT 
          priority,
          RPAD(priority, 15, '-') as padded_priority
        FROM fact_all_types
        WHERE id_bigint < 5
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        expect(row.padded_priority.length).toBe(15);
      });
    });
  });

  describe('REPLACE', () => {
    it('should replace substring occurrences', async () => {
      const sql = `
        SELECT 
          description,
          REPLACE(description, 'Test', 'Demo') as updated_description
        FROM fact_all_types
        WHERE id_bigint < 10
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(10);
      result.forEach((row) => {
        expect(row.updated_description).not.toContain('Test');
        expect(row.updated_description).toContain('Demo');
      });
    });

    it('should replace multiple occurrences', async () => {
      const sql = `
        SELECT 
          REPLACE('banana', 'a', 'o') as replaced,
          REPLACE('aaa', 'a', 'b') as all_replaced
      `;
      const result = await duckdbExec(sql);

      expect(result[0].replaced).toBe('bonono');
      expect(result[0].all_replaced).toBe('bbb');
    });
  });

  describe('REVERSE', () => {
    it('should reverse strings', async () => {
      const sql = `
        SELECT 
          priority,
          REVERSE(priority) as reversed_priority
        FROM fact_all_types
        WHERE id_bigint < 5
        ORDER BY id_bigint
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(5);
      result.forEach((row) => {
        const original = row.priority;
        const reversed = row.reversed_priority;
        expect(reversed.split('').reverse().join('')).toBe(original);
      });
    });
  });

  describe('Complex String Operations', () => {
    it('should combine multiple string functions', async () => {
      const sql = `
        SELECT 
          user_email,
          UPPER(SUBSTRING(user_email, 1, STRPOS(user_email, '@') - 1)) as username_upper,
          LOWER(SUBSTRING(user_email, STRPOS(user_email, '@') + 1)) as domain_lower
        FROM dim_user
        WHERE user_id IN ('user_0', 'user_1', 'user_2')
        ORDER BY user_id
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBe(3);
      result.forEach((row) => {
        expect(row.username_upper).toBeTruthy();
        expect(row.domain_lower).toBeTruthy();
        expect(row.username_upper).toBe(row.username_upper.toUpperCase());
        expect(row.domain_lower).toBe(row.domain_lower.toLowerCase());
      });
    });

    it('should use string operations in WHERE clause', async () => {
      const sql = `
        SELECT COUNT(*) as count
        FROM dim_user
        WHERE LENGTH(user_name) > 5
          AND POSITION('@' IN user_email) > 0
          AND LOWER(user_department) = 'engineering'
      `;
      const result = await duckdbExec(sql);

      expect(Number(result[0].count)).toBeGreaterThan(0);
    });

    it('should use string operations in GROUP BY', async () => {
      const sql = `
        SELECT 
          SUBSTRING(user_email, STRPOS(user_email, '@') + 1) as email_domain,
          COUNT(*) as count
        FROM dim_user
        GROUP BY email_domain
        ORDER BY count DESC
      `;
      const result = await duckdbExec(sql);

      expect(result.length).toBeGreaterThan(0);
      result.forEach((row) => {
        expect(row.email_domain).toBeTruthy();
        expect(Number(row.count)).toBeGreaterThan(0);
      });
    });
  });

  describe('Performance', () => {
    it('should execute string operations efficiently (< 500ms)', async () => {
      const start = Date.now();

      const sql = `
        SELECT 
          UPPER(description) as upper_desc,
          LOWER(priority) as lower_priority,
          CONCAT(priority, '-', status) as combined,
          LENGTH(description) as desc_length
        FROM fact_all_types
        WHERE id_bigint < 10000
        LIMIT 1000
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(500);
    });

    it('should execute regex operations efficiently (< 1s)', async () => {
      const start = Date.now();

      const sql = `
        SELECT COUNT(*) as count
        FROM fact_all_types
        WHERE REGEXP_MATCHES(description, '[A-Za-z]+')
          AND id_bigint < 50000
      `;

      await duckdbExec(sql);
      const duration = Date.now() - start;

      expect(duration).toBeLessThan(1000);
    });
  });
});

