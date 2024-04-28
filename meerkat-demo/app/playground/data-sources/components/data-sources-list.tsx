'use client';

import Link, { LinkProps } from 'next/link';
import { usePathname } from 'next/navigation';
import { Button } from '../../../../ui/button';
import { cn } from '../../../../utils/utils';
import { DEFAULT_DATA_SOURCES } from '../constants/data-sources';

type FilesListProps = React.HTMLAttributes<HTMLDivElement>;

const ActiveLink = ({
  renderer,
  ...rest
}: { renderer: (isActive: boolean) => React.ReactNode } & LinkProps) => {
  const { href } = rest;
  const pathName = usePathname();

  const isActive = pathName === href;
  return <Link {...rest}>{renderer(isActive)}</Link>;
};

const NavItem = ({ label, isActive }: { label: string; isActive: boolean }) => {
  return (
    <Button
      variant={isActive ? 'secondary' : 'ghost'}
      className="w-full justify-start"
    >
      {label}
    </Button>
  );
};

export function DataSourcesList({ className }: FilesListProps) {
  return (
    <div className={cn('pb-12 col-span-3 lg:border-r', className)}>
      <div className="space-y-4 py-4">
        <div className="px-3 py-2 h-full">
          <div className="border-b  pb-2 flex justify-between items-center">
            <p className="text-small  text-muted-foreground">Data Sources</p>
          </div>
          <div className="space-y-1 pt-2">
            {DEFAULT_DATA_SOURCES.files?.map((file) => (
              <ActiveLink
                key={file}
                href={`/playground/data-sources/${file}`}
                renderer={(isActive) => {
                  return <NavItem label={file} isActive={isActive} />;
                }}
              ></ActiveLink>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
