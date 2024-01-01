import { Route, BrowserRouter as Router, Routes } from 'react-router-dom';
import { JSON_TEST_QUERIES } from './constants';
import { IndexedDBMProvider } from './dbm-context/indexed-dbm-context';
import { MemoryDBMProvider } from './dbm-context/memory-dbm-context';
import { RawDBMProvider } from './dbm-context/raw-dbm-context';
import { FileLoader } from './file-loader/file-loader';
import { JsonLoader } from './file-loader/json-loader';
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
          path="/register-json"
          element={
            <div>
              <h1>Register JSON with Memory DBM</h1>
              <MemoryDBMProvider>
                <JsonLoader>
                  <QueryBenchmarking testQueries={JSON_TEST_QUERIES} />
                </JsonLoader>
              </MemoryDBMProvider>
            </div>
          }
        />
      </Routes>
    </Router>
  );
}

export default App;
