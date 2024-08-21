import { QueryOptions, TableConfig } from '../dbm/types';
import { TableWiseFiles } from '../types';

export const BROWSER_RUNNER_TYPE = {
  RUNNER_ON_READY: 'RUNNER_ON_READY',
  EXEC_QUERY: 'EXEC_QUERY',
  DESTROY: 'DESTROY',
  RUNNER_GET_FILE_BUFFERS: 'RUNNER_GET_FILE_BUFFERS',
  RUNNER_PRE_QUERY: 'RUNNER_PRE_QUERY',
} as const;

export type BrowserRunnerMessageType =
  (typeof BROWSER_RUNNER_TYPE)[keyof typeof BROWSER_RUNNER_TYPE];

export interface BrowserRunnerOnReadyMessage {
  type: typeof BROWSER_RUNNER_TYPE.RUNNER_ON_READY;
}

export interface BrowserRunnerExecQueryMessage {
  type: typeof BROWSER_RUNNER_TYPE.EXEC_QUERY;
  payload: {
    query: string;
    tables: TableConfig[];
    options?: Omit<QueryOptions, 'preQuery'>;
  };
}

export interface BrowserRunnerDestroyMessage {
  type: typeof BROWSER_RUNNER_TYPE.DESTROY;
}

export interface BrowserRunnerGetFileBuffersMessage {
  type: typeof BROWSER_RUNNER_TYPE.RUNNER_GET_FILE_BUFFERS;
  payload: {
    tables: TableConfig[];
  };
}

export interface BrowserRunnerPreQueryMessage {
  type: typeof BROWSER_RUNNER_TYPE.RUNNER_PRE_QUERY;
  payload: {
    tablesFileData: TableWiseFiles[];
  };
}

export type BrowserRunnerMessage =
  | BrowserRunnerOnReadyMessage
  | BrowserRunnerExecQueryMessage
  | BrowserRunnerDestroyMessage
  | BrowserRunnerGetFileBuffersMessage
  | BrowserRunnerPreQueryMessage;
