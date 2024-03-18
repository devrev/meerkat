'use client';

import { FilesList } from './components/files-list';

/* eslint-disable-next-line */
export interface FilesProps {}

export function Files(props: FilesProps) {
  return (
    <div className="grid h-full lg:grid-cols-12">
      <FilesList />
      <div className="col-span-10">
        {/* <JsonEditor onChange={(val) => console.log('change:-', val)} /> */}
      </div>
    </div>
  );
}

export default Files;
