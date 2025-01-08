import {
  DBM,
  DBMNative,
  DBMParallel,
  FileManagerType,
} from '@devrev/meerkat-dbm';
import React from 'react';

export type DBMContextType<T> = {
  dbm: DBM | DBMParallel<T> | DBMNative;
  fileManager: FileManagerType;
};

export const DBMContext = React.createContext<DBMContextType<any> | undefined>(
  undefined
);

export const useDBM = <T,>() => {
  const context = React.useContext(DBMContext) as DBMContextType<T> | undefined;
  if (context === undefined) {
    throw new Error('useDBM must be used within a DBMProvider');
  }
  return context;
};
