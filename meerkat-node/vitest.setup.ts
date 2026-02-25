/**
 * Vitest Global Setup
 * 
 * This file runs once before all tests.
 * Use it to initialize shared resources like DuckDB singleton.
 */

import { beforeAll, afterAll } from 'vitest';

// Global setup
beforeAll(async () => {
  console.log('🚀 Starting Vitest test suite...');
  
  // DuckDB singleton will be initialized on first use
  // No need to do anything here unless you want to pre-warm it
});

// Global teardown
afterAll(async () => {
  console.log('✅ Test suite completed');
});

