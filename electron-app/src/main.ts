import { app, BrowserWindow } from 'electron';
import App from './app/app';
import ElectronEvents from './app/events/electron.events';

export default class Main {
  static initialize() {}

  static bootstrapApp() {
    App.main(app, BrowserWindow);
  }

  static bootstrapAppEvents() {
    ElectronEvents.bootstrapElectronEvents();
  }
}

// handle setup events as quickly as possible
Main.initialize();

// bootstrap app
Main.bootstrapApp();
Main.bootstrapAppEvents();
