/**
 * Test Helper Utilities
 * 
 * Common utilities for Vitest-based comprehensive tests
 */

import { duckdbExec } from '../../duckdb-exec';

/**
 * Batch Error Reporter
 * 
 * Collects multiple test failures and reports them together
 * instead of failing on the first error. Useful for data-driven tests.
 */
export class BatchErrorReporter {
  private errors: Array<{ testCase: string; error: Error; context?: any }> = [];
  
  /**
   * Add an error to the batch
   */
  addError(testCase: string, error: Error, context?: any): void {
    this.errors.push({ testCase, error, context });
  }
  
  /**
   * Check if there are any errors
   */
  hasErrors(): boolean {
    return this.errors.length > 0;
  }
  
  /**
   * Get count of errors
   */
  getErrorCount(): number {
    return this.errors.length;
  }
  
  /**
   * Get all errors
   */
  getErrors() {
    return this.errors;
  }
  
  /**
   * Throw if there are any errors, with a comprehensive report
   */
  throwIfErrors(): void {
    if (this.errors.length === 0) {
      return;
    }
    
    const report = this.errors
      .map((e, idx) => {
        let msg = `\n${idx + 1}. ❌ ${e.testCase}\n   Error: ${e.error.message}`;
        if (e.context) {
          msg += `\n   Context: ${JSON.stringify(e.context, null, 2)}`;
        }
        if (e.error.stack) {
          const stackLines = e.error.stack.split('\n').slice(1, 3);
          msg += `\n   ${stackLines.join('\n   ')}`;
        }
        return msg;
      })
      .join('\n');
    
    throw new Error(
      `\n${'='.repeat(80)}\n` +
      `${this.errors.length} TEST(S) FAILED:\n` +
      `${'='.repeat(80)}` +
      report +
      `\n${'='.repeat(80)}\n`
    );
  }
  
  /**
   * Clear all errors
   */
  clear(): void {
    this.errors = [];
  }
}

/**
 * Compare numeric values with tolerance for floating-point precision
 */
export function compareNumbers(
  actual: number,
  expected: number,
  tolerance: number = 0.0001
): boolean {
  return Math.abs(actual - expected) <= tolerance;
}

/**
 * Compare query results with tolerance for numeric fields
 */
export function compareResults(
  actual: any[],
  expected: any[],
  options: {
    numericTolerance?: number;
    ignoreOrder?: boolean;
    ignoreFields?: string[];
  } = {}
): { match: boolean; diff?: string } {
  const { numericTolerance = 0.0001, ignoreOrder = false, ignoreFields = [] } = options;
  
  if (actual.length !== expected.length) {
    return {
      match: false,
      diff: `Row count mismatch: expected ${expected.length}, got ${actual.length}`,
    };
  }
  
  // Sort if ignoring order
  const actualSorted = ignoreOrder ? [...actual].sort() : actual;
  const expectedSorted = ignoreOrder ? [...expected].sort() : expected;
  
  for (let i = 0; i < actualSorted.length; i++) {
    const actualRow = actualSorted[i];
    const expectedRow = expectedSorted[i];
    
    for (const key in expectedRow) {
      if (ignoreFields.includes(key)) continue;
      
      const actualVal = actualRow[key];
      const expectedVal = expectedRow[key];
      
      // Handle null/undefined
      if (actualVal === null && expectedVal === null) continue;
      if (actualVal === undefined && expectedVal === undefined) continue;
      if (actualVal === null && expectedVal === undefined) continue;
      if (actualVal === undefined && expectedVal === null) continue;
      
      // Handle numeric comparison with tolerance
      if (typeof expectedVal === 'number' && typeof actualVal === 'number') {
        if (!compareNumbers(actualVal, expectedVal, numericTolerance)) {
          return {
            match: false,
            diff: `Row ${i}, field '${key}': expected ${expectedVal}, got ${actualVal} (diff: ${Math.abs(actualVal - expectedVal)})`,
          };
        }
      } else if (actualVal !== expectedVal) {
        return {
          match: false,
          diff: `Row ${i}, field '${key}': expected ${JSON.stringify(expectedVal)}, got ${JSON.stringify(actualVal)}`,
        };
      }
    }
  }
  
  return { match: true };
}

/**
 * Create a reference SQL query for validation
 * This is the "oracle" pattern - we create direct SQL to validate Meerkat's output
 */
export function createReferenceSQL(options: {
  select: string[];
  from: string;
  where?: string;
  groupBy?: string[];
  orderBy?: string[];
  limit?: number;
}): string {
  const { select, from, where, groupBy, orderBy, limit } = options;
  
  let sql = `SELECT ${select.join(', ')} FROM ${from}`;
  
  if (where) {
    sql += ` WHERE ${where}`;
  }
  
  if (groupBy && groupBy.length > 0) {
    sql += ` GROUP BY ${groupBy.join(', ')}`;
  }
  
  if (orderBy && orderBy.length > 0) {
    sql += ` ORDER BY ${orderBy.join(', ')}`;
  }
  
  if (limit) {
    sql += ` LIMIT ${limit}`;
  }
  
  return sql;
}

/**
 * Measure execution time of a function
 */
export async function measureExecutionTime<T>(
  fn: () => Promise<T>
): Promise<{ result: T; duration: number }> {
  const startTime = Date.now();
  const result = await fn();
  const duration = Date.now() - startTime;
  
  return { result, duration };
}

/**
 * Validate query performance against a budget
 */
export function validatePerformance(
  duration: number,
  budgetMs: number,
  queryDescription: string
): void {
  if (duration > budgetMs) {
    throw new Error(
      `Performance budget exceeded for "${queryDescription}": ` +
      `expected < ${budgetMs}ms, got ${duration}ms`
    );
  }
}

/**
 * Execute a reference SQL and validate against Meerkat result
 */
export async function validateAgainstReference(
  meerkatResult: any[],
  referenceSQL: string,
  options?: {
    numericTolerance?: number;
    ignoreOrder?: boolean;
    ignoreFields?: string[];
  }
): Promise<void> {
  const referenceResult = await duckdbExec(referenceSQL);
  const comparison = compareResults(meerkatResult, referenceResult, options);
  
  if (!comparison.match) {
    throw new Error(
      `Result mismatch with reference SQL:\n` +
      `  Reference SQL: ${referenceSQL}\n` +
      `  Difference: ${comparison.diff}\n` +
      `  Meerkat rows: ${meerkatResult.length}\n` +
      `  Reference rows: ${referenceResult.length}`
    );
  }
}

/**
 * Generate test cases for a data-driven test
 */
export function generateTestCases<T>(
  template: T,
  variations: Array<Partial<T>>
): T[] {
  return variations.map((variation) => ({ ...template, ...variation }));
}

/**
 * Format SQL for error messages (truncate if too long)
 */
export function formatSQL(sql: string, maxLength: number = 200): string {
  if (sql.length <= maxLength) {
    return sql;
  }
  return sql.substring(0, maxLength) + '...';
}

/**
 * Retry a function with exponential backoff
 * Useful for flaky operations
 */
export async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  options: {
    maxRetries?: number;
    initialDelay?: number;
    maxDelay?: number;
    backoffMultiplier?: number;
  } = {}
): Promise<T> {
  const {
    maxRetries = 3,
    initialDelay = 100,
    maxDelay = 5000,
    backoffMultiplier = 2,
  } = options;
  
  let lastError: Error | undefined;
  let delay = initialDelay;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;
      
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, delay));
        delay = Math.min(delay * backoffMultiplier, maxDelay);
      }
    }
  }
  
  throw lastError;
}

