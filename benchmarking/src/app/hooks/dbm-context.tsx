import {
  DBM,
  DBMNative,
  DBMParallel,
  FileManagerType,
} from '@devrev/meerkat-dbm';
import React from 'react';

export type DBMContextType = {
  dbm: DBM | DBMParallel | DBMNative;
  fileManager: FileManagerType;
};

export const DBMContext = React.createContext<DBMContextType | undefined>(
  undefined
);

export const useDBM = () => {
  const context = React.useContext(DBMContext) as DBMContextType | undefined;
  if (context === undefined) {
    throw new Error('useDBM must be used within a DBMProvider');
  }
  return context;
};
