import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/__vitest__/**/*.{test,spec}.ts'], // Only run Vitest tests
    exclude: [
      'node_modules/**',
      'src/**/__tests__/**', // Exclude old Jest tests
      'src/**/!(__)vitest__/**/*.spec.ts', // Exclude Jest .spec.ts files
    ],
    setupFiles: ['./vitest.setup.ts'],
    pool: 'forks', // Safer for DuckDB (C++ addon)
    maxConcurrency: 4, // Tune based on CI machine
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

