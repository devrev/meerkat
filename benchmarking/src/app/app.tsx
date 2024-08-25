import { Route, BrowserRouter as Router, Routes } from 'react-router-dom';
import { IndexedDBMProvider } from './dbm-context/indexed-dbm-context';
import { MemoryDBMProvider } from './dbm-context/memory-dbm-context';
import { ParallelMemoryDBMProvider } from './dbm-context/parallel-memory-dbm-context';
import { RawDBMProvider } from './dbm-context/raw-dbm-context';
import { FileLoader } from './file-loader/file-loader';
import { QueryBenchmarking } from './query-benchmarking/query-benchmarking';

export function App() {
  return (
    <Router>
      <Routes>
        <Route
          path="/raw-dbm"
          element={
            <div>
              <h1>Raw DuckDB</h1>
              <RawDBMProvider>
                <FileLoader>
                  <QueryBenchmarking />
                </FileLoader>
              </RawDBMProvider>
            </div>
          }
        />
        <Route
          path="/memory-dbm"
          element={
            <div>
              <h1>In Memory Sequence DuckDB</h1>

              <MemoryDBMProvider>
                <FileLoader>
                  <QueryBenchmarking />
                </FileLoader>
              </MemoryDBMProvider>
            </div>
          }
        />
        <Route
          path="/indexed-dbm"
          element={
            <div>
              <h1>IndexedDB DuckDB</h1>
              <IndexedDBMProvider>
                <FileLoader>
                  <QueryBenchmarking />
                </FileLoader>
              </IndexedDBMProvider>
            </div>
          }
        />
        <Route
          path="/parallel-memory-dbm"
          element={
            <div>
              <h1>Parallel Memory DuckDB</h1>
              <ParallelMemoryDBMProvider>
                <FileLoader>
                  <QueryBenchmarking />
                </FileLoader>
              </ParallelMemoryDBMProvider>
            </div>
          }
        />
      </Routes>
    </Router>
  );
}

export default App;
