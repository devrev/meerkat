import { Route, BrowserRouter as Router, Routes } from 'react-router-dom';
import { IndexedDBMProvider } from './dbm-context/indexed-dbm-context';
import { MemoryDBMProvider } from './dbm-context/memory-dbm-context';
import { RawDBMProvider } from './dbm-context/raw-dbm-context';
import { FileLoader } from './file-loader/file-loader';
import { QueryBenchmarking } from './query-benchmarking/query-benchmarking';

export function App() {
  const worker = new Worker('http://localhost:4200/assets/duckdb-worker.js');

  worker.onmessage = function (e) {
    console.log('Received message from worker:', e.data);
    if (e.data.type === 'ready') {
      console.log('DuckDB is ready');
      // You can start querying here
      worker.postMessage({ type: 'query', sql: 'SELECT 1' });
    } else if (e.data.type === 'result') {
      console.log('Query result:', e.data.data);
    } else if (e.data.type === 'error') {
      console.error('Error from worker:', e.data.message);
    }
  };

  worker.onerror = function (error) {
    console.error('Worker error:', error);
  };

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
      </Routes>
    </Router>
  );
}

export default App;
