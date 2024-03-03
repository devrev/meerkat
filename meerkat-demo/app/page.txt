'use client';

import { DataTable } from './components/data-table';
import { QueryBenchmarking } from './components/query-benchmarking';
import { Sidebar } from './components/sidebar';
import { columns, data } from './constants/table-data';
import { IndexedDBMProvider } from './duckdb/indexed-dbm-context';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from './ui/dialog';

export default async function Index() {
  /*
   * Replace the elements below with your own.
   *
   * Note: The corresponding styles are in the ./index.scss file.
   */
  return (
    <div className="bg-background ">
      <div className="grid lg:grid-cols-10">
        <Sidebar playlists={[]} />

        <div className="col-span-3 lg:col-span-4 lg:border-l flex flex-col flex-grow">
          <IndexedDBMProvider>
            <>
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
            </>
          </IndexedDBMProvider>
        </div>
        <div className="col-span-3 lg:col-span-5 lg:border-l flex flex-col">
          <DataTable data={data} columns={columns} />
        </div>
      </div>
    </div>
  );
}
