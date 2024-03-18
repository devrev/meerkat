'use client';

import { Sidebar } from '../../components/sidebar';

export default function PlaygroundLayout({
  children, // will be a page or nested layout
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="grid h-screen lg:grid-cols-10">
      <Sidebar playlists={[]} />
      <div className="col-span-9">{children}</div>
    </div>
  );
}
