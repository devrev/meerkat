'use client';

import Link, { LinkProps } from 'next/link';
import { usePathname } from 'next/navigation';
import { Button } from '../../../ui/button';
import { cn } from '../../../utils/utils';

type FilesListProps = React.HTMLAttributes<HTMLDivElement>;

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

const fileList = [
  {
    fileName: 'orders.csv',
    fileType: 'csv',
    fileId: '1',
  },
  {
    fileName: 'customers.csv',
    fileType: 'csv',
    fileId: '2',
  },
  {
    fileName: 'products.csv',
    fileType: 'csv',
    fileId: '3',
  },
];

export function FilesList({ className }: FilesListProps) {
  return (
    <div className={cn('pb-12 col-span-2 lg:border-r', className)}>
      <div className="space-y-4 py-4">
        <div className="px-3 py-2 h-full">
          <div className="border-b">
            <p className="text-small pb-2 text-muted-foreground">Files</p>
          </div>
          <div className="space-y-1 pt-2">
            {fileList.map((file) => (
              <ActiveLink
                key={file.fileId}
                href={`/playground/files/${file.fileId}`}
                renderer={(isActive) => {
                  return <NavItem label={file.fileName} isActive={isActive} />;
                }}
              ></ActiveLink>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
