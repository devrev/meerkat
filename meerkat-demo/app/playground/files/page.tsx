import { FilesList } from './components/files-list';

/* eslint-disable-next-line */
export interface FilesProps {}

export function Files(props: FilesProps) {
  return (
    <div className="grid h-full lg:grid-cols-12">
      <FilesList />
      <h1>Welcome to Files!</h1>
    </div>
  );
}

export default Files;
