import { Sidebar } from '../components/sidebar';

export default function PlaygroundLayout({
  children, // will be a page or nested layout
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="grid lg:grid-cols-10">
      <Sidebar playlists={[]} />
      {children}
    </div>
  );
}
