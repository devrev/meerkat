import * as crypto from 'crypto';

export const hashString = (text: string): string => {
  return crypto.createHash('md5').update(text).digest('hex');
};
