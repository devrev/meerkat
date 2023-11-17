import { Route, BrowserRouter as Router, Routes } from 'react-router-dom';
import { MemoryDBMProvider } from './dbm-context/memory-dbm-context';
import { FileLoader } from './file-loader/file-loader';
import { QueryBenchmarking } from './query-benchmarking/query-benchmarking';

export function App() {
  return (
    <Router>
      <Routes>
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
