import { Button } from '../ui/button';
import { cn } from '../utils/utils';

interface SidebarProps extends React.HTMLAttributes<HTMLDivElement> {
  playlists: any[];
}

export function Sidebar({ className, playlists }: SidebarProps) {
  return (
    <div className={cn('pb-12', className)}>
      <div className="space-y-4 py-4">
        <div className="px-3 py-2 h-full">
          <h2 className="mb-2 px-4 text-lg font-semibold tracking-tight">
            Meerkat
          </h2>
          <div className="space-y-1">
            <Button variant="secondary" className="w-full justify-start">
              Playground
            </Button>
            <Button variant="ghost" className="w-full justify-start">
              Files
            </Button>
            <Button variant="ghost" className="w-full justify-start">
              Data Sources
            </Button>
            <Button variant="ghost" className="w-full justify-start">
              Docs
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
