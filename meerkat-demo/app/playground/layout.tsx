'use client';

import { Sidebar } from '../../components/sidebar';
import { IndexedDBMProvider } from '../../duckdb/indexed-dbm-context';

export default function PlaygroundLayout({
  children, // will be a page or nested layout
}: {
  children: React.ReactNode;
}) {
  return (
    <IndexedDBMProvider>
      <div className="grid h-screen lg:grid-cols-10">
        <Sidebar playlists={[]} />
        <div className="col-span-9">{children}</div>
      </div>
    </IndexedDBMProvider>
  );
}
