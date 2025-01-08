import axios from 'axios';

export const fetchParquetFile = async (url: string) => {
  const file = await axios.get(url, { responseType: 'arraybuffer' });
  const fileBuffer = file.data;

  const fileBufferView = new Uint8Array(fileBuffer);

  return fileBufferView;
};
