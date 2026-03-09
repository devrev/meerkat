import { ChildProcess, spawn } from 'child_process';
import * as puppeteer from 'puppeteer';

async function waitForServer(
  page: puppeteer.Page,
  url: string,
  timeoutMs = 120000
) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      await page.goto(url);
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
  throw new Error(`Server at ${url} did not start within ${timeoutMs}ms`);
}

async function measureTotalTime(
  page: puppeteer.Page,
  url: string,
  timeoutMs = 300000,
  warmupRuns = 1
) {
  // Warmup runs avoid penalizing first-hit startup/caching costs.
  for (let i = 0; i < warmupRuns; i++) {
    await page.goto(url);
    await page.waitForSelector('#total_time', { timeout: timeoutMs });
  }

  await page.goto(url);
  await page.waitForSelector('#total_time', { timeout: timeoutMs });
  return page.$eval('#total_time', (el) => Number(el.textContent));
}

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
    await waitForServer(page, 'http://localhost:4204');
    await waitForServer(page, 'http://localhost:4205');
  }, 30000);

  it('Benchmark raw duckdb with memory sequence duckdb', async () => {
    const totalTimeForRawDB = await measureTotalTime(
      page,
      'http://localhost:4204/raw-dbm',
      300000,
      1
    );

    console.info('totalTimeForRawDB', totalTimeForRawDB);

    totalTimeForMemoryDB = await measureTotalTime(
      page,
      'http://localhost:4204/memory-dbm',
      300000,
      1
    );

    console.info('totalTimeForMemoryDB', totalTimeForMemoryDB);

    /**
     * The total diff between the two DBs should be less than 10%
     */
    expect(totalTimeForRawDB).toBeLessThan(totalTimeForMemoryDB * 1.1);
  }, 220000);

  it('Benchmark indexed dbm duckdb', async () => {
    const totalTimeForIndexedDBM = await measureTotalTime(
      page,
      'http://localhost:4204/indexed-dbm',
      300000,
      0
    );

    console.info('totalTimeForIndexedDBM', totalTimeForIndexedDBM);

    /**
     * The total diff between indexed dbm and memory dbm should be less than 30%
     */
    expect(totalTimeForIndexedDBM).toBeLessThan(totalTimeForMemoryDB * 1.3);
  }, 300000);

  it('Benchmark parallel memory dbm duckdb', async () => {
    const totalTimeForParallelMemoryDBM = await measureTotalTime(
      page,
      'http://localhost:4204/parallel-memory-dbm',
      300000,
      0
    );

    console.info('totalTimeForParallelDBM', totalTimeForParallelMemoryDBM);

    /**
     * The total diff between parallel memory dbm and memory dbm should be less than
     */
    expect(totalTimeForParallelMemoryDBM).toBeLessThan(totalTimeForMemoryDB);
  }, 300000);

  afterAll(async () => {
    appProcess.kill('SIGTERM');
    appProcessRunner.kill('SIGTERM');
    await browser.close();
  });
});
