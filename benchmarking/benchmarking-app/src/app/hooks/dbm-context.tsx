import { DBM, DBMParallel, FileManagerType } from '@devrev/meerkat-dbm';
import React from 'react';

export const DBMContext = React.createContext<{
  dbm: DBM | DBMParallel;
  fileManager: FileManagerType;
}>(null as any);

export const useDBM = () => {
  const context = React.useContext(DBMContext);
  if (context === undefined) {
    throw new Error('useDBM must be used within a DBMProvider');
  }
  return context;
};
