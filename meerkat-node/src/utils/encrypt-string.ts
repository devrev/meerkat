import * as crypto from 'crypto';

const ENCRYPTION_KEY = crypto.randomBytes(32);
const ALGORITHM = 'aes-256-cbc';

/**
 * Encrypts a given string
 */
export const encryptString = (text: string): string => {
  const iv = crypto.randomBytes(16);

  const cipher = crypto.createCipheriv(ALGORITHM, ENCRYPTION_KEY, iv);

  const encrypted = Buffer.concat([cipher.update(text), cipher.final()]);

  return iv.toString('hex') + encrypted.toString('hex');
};

/**
 * Decrypts an encrypted string
 */
export const decryptString = (encryptedText: string): string => {
  // First 32 chars are IV (16 bytes in hex)
  const iv = encryptedText.slice(0, 32);

  const encrypted = Buffer.from(encryptedText.slice(32), 'hex');

  const decipher = crypto.createDecipheriv(ALGORITHM, ENCRYPTION_KEY, iv);

  return Buffer.concat([
    decipher.update(encrypted),
    decipher.final(),
  ]).toString();
};
