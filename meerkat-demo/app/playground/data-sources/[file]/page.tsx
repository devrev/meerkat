'use client';

import axios from 'axios';
import { useState } from 'react';
import { CubeEditor } from '../../../../components/cube-editor';
import { useClassicEffect } from '../../../../hooks/use-classic-effect';
export interface FilesProps {
  params: {
    file: string;
  };
}

export function File({ params }: FilesProps) {
  const [fileContent, setFileContent] = useState<string | null>(null);

  useClassicEffect(() => {
    (async () => {
      const file = await axios.get(`/files/${params.file}.txt`);
      setFileContent(file.data);
    })();
  }, []);

  return (
    <div className="grid h-full lg:grid-cols-12">
      <div className="col-span-10">
        {fileContent && <CubeEditor txt={fileContent} />}
      </div>
    </div>
  );
}

export default File;
