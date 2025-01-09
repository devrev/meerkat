export enum NativeAppEvent {
  REGISTER_FILES = 'register-files',
  QUERY = 'query',
  DROP_FILES_BY_TABLE = 'drop-files-by-table',
  GET_FILE_PATHS_FOR_TABLE = 'get-file-paths-for-table',
}
export type Channels = NativeAppEvent;

export type Electron = {
  ipcRenderer: {
    invoke: (channel: Channels, ...args: unknown[]) => void;
    send(channel: Channels, ...args: unknown[]): void;
    sendMessage(channel: Channels, ...args: unknown[]): void;
    on(channel: Channels, func: (...args: unknown[]) => void): () => void;
    once(channel: Channels, func: (...args: unknown[]) => void): void;
  };
};

declare global {
  interface Window {
    electron?: Electron;
  }
}
