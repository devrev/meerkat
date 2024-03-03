// eslint-disable-next-line @typescript-eslint/no-unused-vars

import { QueryBenchmarking } from '../components/query-benchmarking';
import { Sidebar } from '../components/sidebar';
import { IndexedDBMProvider } from '../duckdb/indexed-dbm-context';

export function App() {
  return (
    <div className="bg-background ">
      <div className="grid lg:grid-cols-10">
        <Sidebar playlists={[]} />

        <div className="col-span-3 lg:col-span-4 lg:border-l">
          <IndexedDBMProvider>
            <div>
              <div className="p-2">Hello World Inside</div>
              <QueryBenchmarking />
            </div>
          </IndexedDBMProvider>
        </div>
      </div>
    </div>
  );
}

export default App;
