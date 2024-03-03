'use client';

import Link, { LinkProps } from 'next/link';
import { usePathname } from 'next/navigation';
import { Button } from '../ui/button';
import { cn } from '../utils/utils';

interface SidebarProps extends React.HTMLAttributes<HTMLDivElement> {
  playlists: any[];
}

const ActiveLink = ({
  renderer,
  ...rest
}: { renderer: (isActive: boolean) => React.ReactNode } & LinkProps) => {
  const { href } = rest;
  const pathName = usePathname();

  const isActive = pathName === href;
  return (
    // you get a global isActive class name, it is better than
    // nothing, but it means you do not have scoping ability in
    // certain cases
    <Link {...rest}>{renderer(isActive)}</Link>
  );
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

export function Sidebar({ className, playlists }: SidebarProps) {
  return (
    <div className={cn('pb-12 lg:border-r', className)}>
      <div className="space-y-4 py-4">
        <div className="px-3 py-2 h-full">
          <h2 className="mb-2 px-4 text-lg font-semibold tracking-tight">
            Meerkat
          </h2>
          <div className="space-y-1">
            <ActiveLink
              href={'/playground/explore'}
              renderer={(isActive) => {
                return <NavItem label="Playground" isActive={isActive} />;
              }}
            ></ActiveLink>

            <ActiveLink
              href={'/playground/files'}
              renderer={(isActive) => {
                return <NavItem label="Files" isActive={isActive} />;
              }}
            ></ActiveLink>

            <ActiveLink
              href={'/playground/data-sources'}
              renderer={(isActive) => {
                return <NavItem label="Data Sources" isActive={isActive} />;
              }}
            ></ActiveLink>
          </div>
        </div>
      </div>
    </div>
  );
}
