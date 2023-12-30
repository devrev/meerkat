import { InstanceManagerType } from '../../../dbm/instance-manager';
import { FileRegisterer } from '../file-registerer';

const mockDB = {
  registerFileBuffer: jest.fn(async (fileName: string, buffer: Uint8Array) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve([fileName]);
      }, 200);
    });
  }),
  registerEmptyFileBuffer: jest.fn(async (fileName: string) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve([fileName]);
      }, 200);
    });
  }),
};

describe('FileRegisterer', () => {
  let db: typeof mockDB;
  let instanceManager: InstanceManagerType;
  let fileRegisterer: FileRegisterer;

  beforeEach(() => {
    jest.clearAllMocks();
    db = mockDB;
    instanceManager = {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-ignore
      getDB: async () => {
        return db;
      },
      terminateDB: async () => {
        return;
      },
    };
    fileRegisterer = new FileRegisterer({ instanceManager });
  });

  it('should register file buffer', async () => {
    const fileName = 'test.txt';
    const buffer = new Uint8Array();
    await fileRegisterer.registerFileBuffer(fileName, buffer);
    expect(db.registerFileBuffer).toHaveBeenCalledWith(fileName, buffer);
    expect(fileRegisterer.isFileRegisteredInDB(fileName)).toBeTruthy();
  });

  it('should register empty file buffer', async () => {
    const fileName = 'test.txt';
    await fileRegisterer.registerEmptyFileBuffer(fileName);
    expect(db.registerEmptyFileBuffer).toHaveBeenCalledWith(fileName);
    expect(fileRegisterer.isFileRegisteredInDB(fileName)).toBeFalsy();
  });

  it('should check if file is registered in DB', () => {
    const fileName = 'test.txt';
    expect(fileRegisterer.isFileRegisteredInDB(fileName)).toBeFalsy();
  });

  it('should flush file cache', () => {
    fileRegisterer.flushFileCache();
    expect(fileRegisterer.getAllFilesInDB()).toEqual([]);
  });

  it('should get total byte length', () => {
    const fileName = 'test.txt';
    const buffer = new Uint8Array(5);
    fileRegisterer.registerFileBuffer(fileName, buffer);
    expect(fileRegisterer.totalByteLength()).toEqual(5);
  });

  it('should get all files in DB', () => {
    const fileName1 = 'test1.txt';
    const fileName2 = 'test2.txt';
    const buffer = new Uint8Array();
    fileRegisterer.registerFileBuffer(fileName1, buffer);
    fileRegisterer.registerFileBuffer(fileName2, buffer);
    expect(fileRegisterer.getAllFilesInDB()).toEqual([fileName1, fileName2]);
  });
});
