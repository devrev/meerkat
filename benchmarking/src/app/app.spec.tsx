import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { PropsWithChildren } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './app';

vi.mock('./dbm-context/indexed-dbm-context', () => ({
  IndexedDBMProvider: ({ children }: PropsWithChildren) => children,
}));
vi.mock('./dbm-context/memory-dbm-context', () => ({
  MemoryDBMProvider: ({ children }: PropsWithChildren) => children,
}));
vi.mock('./dbm-context/native-dbm-context', () => ({
  NativeDBMProvider: ({ children }: PropsWithChildren) => children,
}));
vi.mock('./dbm-context/parallel-indexed-dbm-context', () => ({
  ParallelIndexedDBMProvider: ({ children }: PropsWithChildren) => children,
}));
vi.mock('./dbm-context/parallel-memory-dbm-context', () => ({
  ParallelMemoryDBMProvider: ({ children }: PropsWithChildren) => children,
}));
vi.mock('./dbm-context/raw-dbm-context', () => ({
  RawDBMProvider: ({ children }: PropsWithChildren) => children,
}));
vi.mock('./file-loader/file-loader', () => ({
  FileLoader: ({ children }: PropsWithChildren) => children,
}));
vi.mock('./file-loader/native-app-file-loader', () => ({
  NativeAppFileLoader: ({ children }: PropsWithChildren) => children,
}));
vi.mock('./query-benchmarking/query-benchmarking', () => ({
  QueryBenchmarking: () => <div>Benchmark ready</div>,
}));

describe('App routing', () => {
  beforeEach(() => {
    window.history.pushState({}, '', '/raw-dbm');
  });

  afterEach(() => {
    cleanup();
  });

  it('renders the route selected from the browser location', () => {
    render(<App />);

    expect(screen.getByRole('heading', { name: 'Raw DuckDB' })).toBeTruthy();
    expect(screen.getByText('Benchmark ready')).toBeTruthy();
  });

  it('navigates between benchmark routes without a page load', async () => {
    render(<App />);

    fireEvent.click(
      screen.getByRole('link', { name: 'In Memory Sequence DuckDB' })
    );

    await waitFor(() => {
      expect(window.location.pathname).toBe('/memory-dbm');
      expect(
        screen.getByRole('heading', { name: 'In Memory Sequence DuckDB' })
      ).toBeTruthy();
    });
  });
});
