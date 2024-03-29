'use client';

import { useDefaultFileLoader } from './hooks/use-default-file-loader';

/* eslint-disable-next-line */
export interface FilesProps {}

export function Files(props: FilesProps) {
  const isFileLoader = useDefaultFileLoader();

  return (
    <div className="grid h-full lg:grid-cols-12">
      <div className="col-span-12 flex h-full items-center justify-center ">
        Select a file to view
      </div>
    </div>
  );
}

export default Files;
