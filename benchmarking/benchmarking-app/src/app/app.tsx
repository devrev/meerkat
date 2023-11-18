import { Route, BrowserRouter as Router, Routes } from 'react-router-dom';
import { MemoryDBMProvider } from './dbm-context/memory-dbm-context';
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
              <MemoryDBMProvider>
                <FileLoader>
                  <QueryBenchmarking />
                </FileLoader>
              </MemoryDBMProvider>
            </div>
          }
        />
      </Routes>
    </Router>
  );
}

export default App;
