'use client';

import { FilesList } from './components/files-list';

export default function FileLayout({
  children, // will be a page or nested layout
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="grid h-full lg:grid-cols-12">
      <FilesList />
      <div className="col-span-9">{children}</div>
    </div>
  );
}
