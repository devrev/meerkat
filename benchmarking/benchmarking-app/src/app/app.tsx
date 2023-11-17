import { DBMProvider } from './dbm-context/dbm-context';
import { FileLoader } from './file-loader/file-loader';
import { QueryBenchmarking } from './query-benchmarking/query-benchmarking';

export function App() {
  return (
    <div>
      <DBMProvider>
        <FileLoader>
          <QueryBenchmarking></QueryBenchmarking>
        </FileLoader>
      </DBMProvider>
    </div>
  );
}

export default App;
