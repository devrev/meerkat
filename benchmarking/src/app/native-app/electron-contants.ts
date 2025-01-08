import { NativeBridge } from 'meerkat-dbm/src/dbm/dbm-native/native-bridge';

declare global {
  interface Window {
    electron?: NativeBridge;
  }
}
