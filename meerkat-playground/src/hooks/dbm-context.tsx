import { DBM, FileManagerType } from '@devrev/meerkat-dbm';
import React from 'react';
import { InstanceManager } from '../duckdb/instance-manager';

export const DBMContext = React.createContext<{
  dbm: DBM;
  fileManager: FileManagerType;
  instanceManager: InstanceManager;
}>(null as any);

export const useDBM = () => {
  const context = React.useContext(DBMContext);
  if (context === undefined) {
    throw new Error('useDBM must be used within a DBMProvider');
  }
  return context;
};
