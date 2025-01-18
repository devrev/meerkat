import * as crypto from 'crypto';

/**
 * Hash a string using MD5.
 */
export const hashString = (text: string): string => {
  return crypto.createHash('md5').update(text).digest('hex');
};
