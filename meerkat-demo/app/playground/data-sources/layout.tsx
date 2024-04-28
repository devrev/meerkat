'use client';

import { DataSourcesList } from './components/data-sources-list';

export default function DataSourcesLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="grid h-full lg:grid-cols-12">
      <DataSourcesList />
      <div className="col-span-9">{children}</div>
    </div>
  );
}
