import { FileManager } from '@devrev/meerkat-node';
import { BASE_DIR } from './constants';

export const fileManager = new FileManager({ baseDir: BASE_DIR });
