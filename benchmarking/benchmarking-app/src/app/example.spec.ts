import { spawn } from 'child_process';
import * as puppeteer from 'puppeteer';

describe('Example', () => {
  let page;
  let browser;
  let appProcess;

  beforeAll(async () => {
    appProcess = spawn('npx', ['nx', 'serve', 'benchmarking-app'], {
      stdio: 'inherit',
    });

    // Wait for the server to start
    let serverStarted = false;
    while (!serverStarted) {
      try {
        const response = await fetch('http://localhost:4200'); // Replace with your server's URL
        if (response.ok) {
          serverStarted = true;
        } else {
          await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait for 1 second before retrying
        }
      } catch (error) {
        await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait for 1 second before retrying
      }
    }

    browser = await puppeteer.launch({
      headless: 'new',
    });
    page = await browser.newPage();
    await page.goto('http://localhost:4200/memory-dbm');
  }, 30000);

  it('Should display "DevRev" text on page', async () => {
    /**
     * wait data-query='4' to be rendered
     */
    await page.waitForSelector('[data-query="4"]', { timeout: 120000 });
    const pageContent = await page.content();
    console.info('pageContent', pageContent);
    expect(pageContent).toContain('query');
  }, 120000);

  afterAll(async () => {
    await browser.close();
    appProcess.kill('SIGTERM');
  });
});
