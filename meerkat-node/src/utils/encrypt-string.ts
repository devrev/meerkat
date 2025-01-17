import * as crypto from 'crypto';

const ENCRYPTION_KEY = crypto.randomBytes(32);
const ALGORITHM = 'aes-256-cbc';

/**
 * Encrypts a given string
 */
export const encryptString = (text: string): string => {
  const iv = crypto.randomBytes(100);
  const cipher = crypto.createCipheriv(ALGORITHM, ENCRYPTION_KEY, iv);

  let encrypted = cipher.update(text, 'utf8', 'base64');
  encrypted += cipher.final('base64');

  return `${iv.toString('base64')}:${encrypted}`;
};

/**
 * Decrypts an encrypted string
 */
export const decryptString = (encryptedText: string): string => {
  const [ivString, encrypted] = encryptedText.split(':');
  const iv = Buffer.from(ivString, 'base64');

  const decipher = crypto.createDecipheriv(ALGORITHM, ENCRYPTION_KEY, iv);
  let decrypted = decipher.update(encrypted, 'base64', 'utf8');
  decrypted += decipher.final('utf8');

  return decrypted;
};
