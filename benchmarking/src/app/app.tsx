import { Link, Route, BrowserRouter as Router, Routes } from 'react-router-dom';
import { IndexedDBMProvider } from './dbm-context/indexed-dbm-context';
import { MemoryDBMProvider } from './dbm-context/memory-dbm-context';
import { ParallelIndexedDBMProvider } from './dbm-context/parallel-indexed-dbm-context';
import { ParallelMemoryDBMProvider } from './dbm-context/parallel-memory-dbm-context';
import { RawDBMProvider } from './dbm-context/raw-dbm-context';
import { FileLoader } from './file-loader/file-loader';
import { QueryBenchmarking } from './query-benchmarking/query-benchmarking';

export function App() {
  return (
    <Router>
      <nav>
        <ul>
          <li>
            <Link to="/raw-dbm">Raw DuckDB</Link>
          </li>
          <li>
            <Link to="/memory-dbm">In Memory Sequence DuckDB</Link>
          </li>
          <li>
            <Link to="/indexed-dbm">IndexedDB DuckDB</Link>
          </li>
          <li>
            <Link to="/parallel-memory-dbm">Parallel Memory DuckDB</Link>
          </li>
          <li>
            <Link to="/parallel-indexed-dbm">Parallel Indexed DuckDB</Link>
          </li>
        </ul>
      </nav>
      <Routes>
        <Route
          path="/raw-dbm"
          element={
            <div>
              <h1>Raw DuckDB</h1>
              <RawDBMProvider>
                <FileLoader bufferType="uint8Array">
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
                <FileLoader bufferType="uint8Array">
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
                <FileLoader bufferType="uint8Array">
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
                <FileLoader bufferType="sharedArrayBuffer">
                  <QueryBenchmarking />
                </FileLoader>
              </ParallelMemoryDBMProvider>
            </div>
          }
        />
        <Route
          path="/parallel-indexed-dbm"
          element={
            <div>
              <h1>Parallel Indexed DuckDB</h1>
              <ParallelIndexedDBMProvider>
                <FileLoader bufferType="uint8Array">
                  <QueryBenchmarking />
                </FileLoader>
              </ParallelIndexedDBMProvider>
            </div>
          }
        />
      </Routes>
    </Router>
  );
}

export default App;
