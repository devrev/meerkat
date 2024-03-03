// eslint-disable-next-line @typescript-eslint/no-unused-vars

import { QueryBenchmarking } from '../components/query-benchmarking';
import { Sidebar } from '../components/sidebar';
import { IndexedDBMProvider } from '../duckdb/indexed-dbm-context';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '../ui/dialog';

export function App() {
  return (
    <div className="bg-background ">
      <div className="grid lg:grid-cols-10">
        <Sidebar playlists={[]} />

        <div className="col-span-3 lg:col-span-4 lg:border-l">
          <IndexedDBMProvider>
            <div>
              <div className="p-2">Hello World Inside</div>
              <Dialog>
                <DialogTrigger>Open</DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Are you absolutely sure?</DialogTitle>
                    <DialogDescription>
                      This action cannot be undone. This will permanently delete
                      your account and remove your data from our servers.
                    </DialogDescription>
                  </DialogHeader>
                </DialogContent>
              </Dialog>

              <QueryBenchmarking />
            </div>
          </IndexedDBMProvider>
        </div>
      </div>
    </div>
  );
}

export default App;
