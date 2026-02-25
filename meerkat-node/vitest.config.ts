import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
  resolve: {
    alias: {
      '@devrev/meerkat-core': resolve(__dirname, '../meerkat-core/src/index.ts'),
    },
  },
  test: {
    globals: true,
    environment: 'node',
    include: ['src/**/*.{test,spec}.ts'],
    exclude: [
      'node_modules/**',
    ],
    setupFiles: ['./vitest.setup.ts'],
    pool: 'forks', // Safer for DuckDB (C++ addon)
    // Suites share global DuckDB tables, so run serially for deterministic results.
    maxConcurrency: 1,
    testTimeout: 60000, // 60 seconds for data generation tests
    retry: 1, // Retry once for flaky tests
    reporters: ['verbose'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      exclude: [
        'node_modules/',
        'src/**/*.spec.ts',
        'src/**/*.test.ts',
        'src/**/__tests__/',
      ],
      thresholds: {
        lines: 90,
        functions: 90,
        branches: 85,
        statements: 90,
      },
    },
  },
});

