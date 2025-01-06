import { BrowserWindow, screen } from 'electron';
import { join } from 'path';
import { format } from 'url';
import { environment } from '../environments/environment';
import { RENDER_APP_PORT, RENDERER_APP_NAME } from './constants';

export default class App {
  static mainWindow: Electron.BrowserWindow;
  static application: Electron.App;
  static BrowserWindow;

  public static isDevelopmentMode() {
    const isEnvironmentSet: boolean = 'ELECTRON_IS_DEV' in process.env;
    const getFromEnvironment: boolean =
      parseInt(process.env.ELECTRON_IS_DEV, 10) === 1;

    return isEnvironmentSet ? getFromEnvironment : !environment.production;
  }

  private static onWindowAllClosed() {
    if (process.platform !== 'darwin') {
      App.application.quit();
    }
  }

  private static onClose() {
    App.mainWindow = null;
  }

  private static onReady() {
    if (RENDERER_APP_NAME) {
      App.initMainWindow();
      App.loadMainWindow();
    }
  }

  private static onActivate() {
    if (App.mainWindow === null) {
      App.onReady();
    }
  }

  private static initMainWindow() {
    const workAreaSize = screen.getPrimaryDisplay().workAreaSize;
    const width = Math.min(1280, workAreaSize.width || 1280);
    const height = Math.min(720, workAreaSize.height || 720);

    // Create the browser window.
    App.mainWindow = new BrowserWindow({
      width: width,
      height: height,
      show: false,
      webPreferences: {
        contextIsolation: true,
        backgroundThrottling: false,
        preload: join(__dirname, 'main.preload.js'),
      },
    });
    App.mainWindow.setMenu(null);
    App.mainWindow.center();

    App.mainWindow.once('ready-to-show', () => {
      App.mainWindow.show();
    });

    // Emitted when the window is closed.
    App.mainWindow.on('closed', () => {
      App.mainWindow = null;
    });
  }

  private static loadMainWindow() {
    if (!App.application.isPackaged) {
      App.mainWindow.loadURL(`http://localhost:${RENDER_APP_PORT}`);
    } else {
      App.mainWindow.loadURL(
        format({
          pathname: join(__dirname, '..', RENDERER_APP_NAME, 'index.html'),
          protocol: 'file:',
          slashes: true,
        })
      );
    }
  }

  static main(app: Electron.App, browserWindow: typeof BrowserWindow) {
    App.BrowserWindow = browserWindow;
    App.application = app;

    App.application.on('window-all-closed', App.onWindowAllClosed);
    App.application.on('ready', App.onReady);
    App.application.on('activate', App.onActivate);
  }
}
