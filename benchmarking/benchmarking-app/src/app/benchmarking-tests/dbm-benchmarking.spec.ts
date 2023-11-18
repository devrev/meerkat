import axios from 'axios';
import { spawn } from 'child_process';
import * as puppeteer from 'puppeteer';

describe('Benchmarking DBMs', () => {
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
        const response = await axios.get('http://localhost:4200');
        if (response.status === 200) {
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
  }, 30000);

  it('Benchmark raw duckdb with memory sequence duckdb', async () => {
    await page.goto('http://localhost:4200/raw-dbm');
    /**
     * wait for total time to be render
     */
    await page.waitForSelector('#total_time', { timeout: 120000 });
    /**
     * Get the total time as number
     */
    const totalTimeForRawDB = await page.$eval('#total_time', (el) =>
      Number(el.textContent)
    );

    console.info('totalTimeForRawDB', totalTimeForRawDB);

    await page.goto('http://localhost:4200/memory-dbm');

    /**
     * wait for total time to be render
     */
    await page.waitForSelector('#total_time', { timeout: 120000 });

    /**
     * Get the total time as number
     */
    const totalTimeForMemoryDB = await page.$eval('#total_time', (el) =>
      Number(el.textContent)
    );

    console.info('totalTimeForMemoryDB', totalTimeForMemoryDB);

    /**
     * The total diff between the two DBs should be less than 10%
     */
    expect(totalTimeForRawDB).toBeLessThan(totalTimeForMemoryDB * 1.1);
  }, 220000);

  afterAll(async () => {
    await browser.close();
    appProcess.kill('SIGTERM');
  });
});
