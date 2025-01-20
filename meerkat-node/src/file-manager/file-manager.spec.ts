import axios from 'axios';
import { createWriteStream, promises as fs } from 'fs';
import { hashString } from '../utils/hash-string';
import { FileManager } from './file-manager';

// Mock external dependencies
jest.mock('axios');

jest.mock('fs', () => ({
  promises: {
    mkdir: jest.fn().mockImplementation(() => Promise.resolve()),
    writeFile: jest.fn().mockImplementation(() => Promise.resolve()),
    readdir: jest.fn().mockImplementation(() => Promise.resolve([])),
    unlink: jest.fn().mockImplementation(() => Promise.resolve()),
  },
  createWriteStream: jest.fn(),
}));

jest.mock('path', () => ({
  join: jest.fn((...args) => args.join('/')),
  dirname: jest.fn((path) => path.split('/').slice(0, -1).join('/')),
}));

jest.mock('../utils/hash-string', () => ({
  hashString: jest.fn((str) => `hashed_${str}`),
}));

describe('FileManager', () => {
  let fileManager: FileManager;
  const mockBaseDir = './test-data';

  beforeEach(() => {
    fileManager = new FileManager({ baseDir: mockBaseDir });
    jest.clearAllMocks();
  });

  describe('should create base directory', () => {
    it('should use default baseDir if not provided', () => {
      const defaultManager = new FileManager({});
      expect(fs.mkdir).toHaveBeenCalledWith('./data', { recursive: true });
    });

    it('should use provided baseDir', () => {
      const defaultManager = new FileManager({ baseDir: mockBaseDir });

      expect(fs.mkdir).toHaveBeenCalledWith(mockBaseDir, { recursive: true });
    });
  });

  describe('getPath', () => {
    it('should return path with fileName if provided', () => {
      const result = fileManager.getPath('table', 'file.txt');
      expect(result).toBe('./test-data/table/file.txt');
    });

    it('should return path without fileName if not provided', () => {
      const result = fileManager.getPath('table');
      expect(result).toBe('./test-data/table');
    });
  });

  describe('writeFileBuffer', () => {
    const mockFile = {
      tableName: 'test-table',
      fileName: 'test-file.txt',
      buffer: new Uint8Array([1, 2, 3]),
    };

    it('should write file buffer successfully', async () => {
      await fileManager.writeFileBuffer(mockFile);

      expect(hashString).toHaveBeenCalledWith(mockFile.fileName);
      expect(fs.mkdir).toHaveBeenCalledWith(
        `${mockBaseDir}/${mockFile.tableName}`,
        { recursive: true }
      );
      expect(fs.writeFile).toHaveBeenCalledWith(
        `${mockBaseDir}/${mockFile.tableName}/hashed_${mockFile.fileName}`,
        mockFile.buffer
      );
    });
  });

  describe('getTableFilePaths', () => {
    it('should return file paths for existing table', async () => {
      const mockFiles = ['file1.txt', 'file2.txt'];
      (fs.readdir as jest.Mock).mockResolvedValueOnce(mockFiles);

      const result = await fileManager.getTableFilePaths('test-table');

      expect(result).toEqual([
        './test-data/test-table/file1.txt',
        './test-data/test-table/file2.txt',
      ]);
    });
  });

  describe('deleteTableFiles', () => {
    it('should delete specified files', async () => {
      const files = ['file1.txt', 'file2.txt'];

      await fileManager.deleteTableFiles('test-table', files);

      expect(fs.unlink).toHaveBeenCalledTimes(2);

      files.forEach((file) => {
        expect(fs.unlink).toHaveBeenCalledWith(
          `${mockBaseDir}/test-table/hashed_${file}`
        );
      });
    });
  });

  describe('streamAndRegisterFile', () => {
    const mockParams = {
      tableName: 'test-table',
      url: 'https://example.com/file',
      headers: { Authorization: 'Bearer token' },
      fileName: 'test-file.txt',
    };

    beforeEach(() => {
      const mockWriteStream: any = {
        close: jest.fn(),
        pipe: jest.fn(),
        on: jest.fn((event, callback) => {
          if (event === 'finish') setTimeout(callback, 0);
          return mockWriteStream;
        }),
      };
      (createWriteStream as jest.Mock).mockReturnValue(mockWriteStream);
    });

    it('should stream and register file successfully', async () => {
      const mockResponse = {
        data: {
          pipe: jest.fn(),
        },
      };
      (axios as unknown as jest.Mock).mockResolvedValueOnce(mockResponse);

      await fileManager.streamAndRegisterFile(mockParams);

      expect(axios).toHaveBeenCalledWith({
        headers: mockParams.headers,
        method: 'get',
        responseType: 'stream',
        url: mockParams.url,
      });
      expect(fs.mkdir).toHaveBeenCalledWith(
        `${mockBaseDir}/${mockParams.tableName}`,
        { recursive: true }
      );
      expect(createWriteStream).toHaveBeenCalled();
    });

    it('should handle stream errors', async () => {
      const streamError = new Error('stream error');
      const mockWriteStream: any = {
        close: jest.fn(),
        pipe: jest.fn(),
        on: jest.fn((event, callback) => {
          if (event === 'error') setTimeout(() => callback(streamError), 0);
          return mockWriteStream;
        }),
      };

      (createWriteStream as jest.Mock).mockReturnValue(mockWriteStream);
      (axios as unknown as jest.Mock).mockResolvedValueOnce({
        data: {
          pipe: jest.fn(),
        },
      });

      await expect(
        fileManager.streamAndRegisterFile(mockParams)
      ).rejects.toThrow('stream error');
    });
  });
});
