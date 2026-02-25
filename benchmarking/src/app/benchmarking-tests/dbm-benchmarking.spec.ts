import { ChildProcess, spawn } from 'child_process';
import * as puppeteer from 'puppeteer';

const isCi = process.env.CI === 'true';

describe('Benchmarking DBMs', () => {
  let page: puppeteer.Page;
  let browser: puppeteer.Browser;
  let appProcess: ChildProcess;
  let appProcessRunner: ChildProcess;

  let totalTimeForMemoryDB: number;

  beforeAll(async () => {
    appProcess = spawn('npx', ['nx', 'serve', 'benchmarking-app'], {
      stdio: 'inherit',
    });

    appProcessRunner = spawn('npx', ['nx', 'serve', 'meerkat-browser-runner'], {
      stdio: 'inherit',
    });

    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    page = await browser.newPage();

    //Wait for the server to start by visiting the page
    let serverStarted = false;
    while (!serverStarted) {
      console.info('Waiting for server to start');
      try {
        await page.goto('http://localhost:4204');
        serverStarted = true;
      } catch (error) {
        console.info('Server not started yet', error);
        await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait for 1 second before retrying
      }
    }

    let serverStartedRunner = false;
    while (!serverStartedRunner) {
      console.info('Waiting for server to start');

      try {
        await page.goto('http://localhost:4205');
        serverStartedRunner = true;
      } catch (error) {
        console.info('Server not started yet', error);
        await new Promise((resolve) => setTimeout(resolve, 1000)); // Wait for 1 second before retrying
      }
    }
  }, 120000);

  it('Benchmark raw duckdb with memory sequence duckdb', async () => {
    await page.goto('http://localhost:4204/raw-dbm');
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

    await page.goto('http://localhost:4204/memory-dbm');

    /**
     * wait for total time to be render
     */
    await page.waitForSelector('#total_time', { timeout: 120000 });

    /**
     * Get the total time as number
     */
    totalTimeForMemoryDB = await page.$eval('#total_time', (el) =>
      Number(el.textContent)
    );

    console.info('totalTimeForMemoryDB', totalTimeForMemoryDB);

    /**
     * CI runners are noisy and can make benchmark ratios flaky.
     * Keep strict ratio checks for local/dev runs and only validate
     * successful execution on CI.
     */
    if (isCi) {
      expect(totalTimeForRawDB).toBeGreaterThan(0);
      expect(totalTimeForMemoryDB).toBeGreaterThan(0);
    } else {
      expect(totalTimeForRawDB).toBeLessThan(totalTimeForMemoryDB * 1.1);
    }
  }, 220000);

  it('Benchmark indexed dbm duckdb', async () => {
    await page.goto('http://localhost:4204/indexed-dbm');
    /**
     * wait for total time to be render
     */
    await page.waitForSelector('#total_time', { timeout: 300000 });
    /**
     * Get the total time as number
     */
    const totalTimeForIndexedDBM = await page.$eval('#total_time', (el) =>
      Number(el.textContent)
    );

    console.info('totalTimeForIndexedDBM', totalTimeForIndexedDBM);

    if (isCi) {
      expect(totalTimeForIndexedDBM).toBeGreaterThan(0);
    } else {
      expect(totalTimeForIndexedDBM).toBeLessThan(totalTimeForMemoryDB * 1.3);
    }
  }, 300000);

  it('Benchmark parallel memory dbm duckdb', async () => {
    await page.goto('http://localhost:4204/parallel-memory-dbm');
    /**
     * wait for total time to be render
     */
    await page.waitForSelector('#total_time', { timeout: 300000 });
    /**
     * Get the total time as number
     */
    const totalTimeForParallelMemoryDBM = await page.$eval(
      '#total_time',
      (el) => Number(el.textContent)
    );

    console.info('totalTimeForParallelDBM', totalTimeForParallelMemoryDBM);

    if (isCi) {
      expect(totalTimeForParallelMemoryDBM).toBeGreaterThan(0);
    } else {
      expect(totalTimeForParallelMemoryDBM).toBeLessThan(totalTimeForMemoryDB);
    }
  }, 300000);

  afterAll(async () => {
    appProcess.kill('SIGTERM');
    appProcessRunner.kill('SIGTERM');
    await browser.close();
  });
});
