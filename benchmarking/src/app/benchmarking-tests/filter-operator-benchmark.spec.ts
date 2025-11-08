import { ChildProcess, spawn } from 'child_process';
import * as puppeteer from 'puppeteer';

describe('Filter Operator Benchmarking: IN vs ANY', () => {
  let page: puppeteer.Page;
  let browser: puppeteer.Browser;
  let appProcess: ChildProcess;

  beforeAll(async () => {
    appProcess = spawn('npx', ['nx', 'serve', 'benchmarking-app'], {
      stdio: 'inherit',
    });

    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    page = await browser.newPage();

    // Wait for the server to start
    let serverStarted = false;
    while (!serverStarted) {
      console.info('Waiting for server to start');
      try {
        await page.goto('http://localhost:4204');
        serverStarted = true;
      } catch (error) {
        console.info('Server not started yet', error);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }, 60000);

  it('Should run IN vs ANY operator benchmark and report results', async () => {
    console.info('\n🔥 Starting IN vs ANY Filter Operator Benchmark...\n');

    await page.goto('http://localhost:4204/filter-benchmark');

    // Wait for total time to be rendered (benchmark complete)
    await page.waitForSelector('#total_time', { timeout: 300000 });

    // Get the total benchmark time
    const totalTime = await page.$eval('#total_time', (el) =>
      Number(el.textContent)
    );

    console.info(`\n✓ Benchmark completed in ${totalTime}ms\n`);

    // Extract all comparison results
    const comparisons = await page.evaluate(() => {
      const rows = Array.from(document.querySelectorAll('tbody tr'));
      return rows.map((row) => {
        const cells = row.querySelectorAll('td');
        return {
          test: cells[0]?.textContent || '',
          inTime: parseFloat(cells[1]?.textContent || '0'),
          anyTime: parseFloat(cells[2]?.textContent || '0'),
          improvement: cells[3]?.textContent || '',
          winner: cells[4]?.textContent || '',
        };
      });
    });

    // Log results
    console.info('📊 Comparison Results:\n');
    console.info('━'.repeat(100));
    console.info(
      '| Test                                              | IN (ms)  | ANY (ms) | Improvement | Winner |'
    );
    console.info('━'.repeat(100));

    comparisons.forEach((comp) => {
      const testName = comp.test.padEnd(50);
      const inTime = comp.inTime.toFixed(2).padStart(8);
      const anyTime = comp.anyTime.toFixed(2).padStart(8);
      const improvement = comp.improvement.padEnd(12);
      const winner = comp.winner.padStart(6);

      console.info(
        `| ${testName} | ${inTime} | ${anyTime} | ${improvement} | ${winner} |`
      );
    });
    console.info('━'.repeat(100));

    // Calculate overall statistics
    const inTotal = comparisons.reduce((sum, c) => sum + c.inTime, 0);
    const anyTotal = comparisons.reduce((sum, c) => sum + c.anyTime, 0);
    const anyWins = comparisons.filter((c) => c.winner === 'ANY').length;
    const inWins = comparisons.filter((c) => c.winner === 'IN').length;
    const ties = comparisons.filter((c) => c.winner === 'TIE').length;

    console.info(`\n📈 Overall Statistics:`);
    console.info(`   Total IN time:    ${inTotal.toFixed(2)}ms`);
    console.info(`   Total ANY time:   ${anyTotal.toFixed(2)}ms`);
    console.info(`   ANY wins:         ${anyWins}/${comparisons.length}`);
    console.info(`   IN wins:          ${inWins}/${comparisons.length}`);
    console.info(`   Ties:             ${ties}/${comparisons.length}`);

    if (anyTotal < inTotal) {
      const improvement = (((inTotal - anyTotal) / inTotal) * 100).toFixed(2);
      console.info(`\n✨ ANY operator is ${improvement}% faster overall!\n`);
    } else if (inTotal < anyTotal) {
      const improvement = (((anyTotal - inTotal) / anyTotal) * 100).toFixed(2);
      console.info(`\n✨ IN operator is ${improvement}% faster overall!\n`);
    } else {
      console.info(`\n🤝 Both operators have similar performance!\n`);
    }

    // Assertions - verify benchmark ran successfully
    expect(totalTime).toBeGreaterThan(0);
    expect(comparisons.length).toBeGreaterThan(0);

    // Optional: Assert that the benchmark actually tested both operators
    comparisons.forEach((comp) => {
      expect(comp.inTime).toBeGreaterThan(0);
      expect(comp.anyTime).toBeGreaterThan(0);
    });

    // Take a screenshot of the results
    await page.screenshot({
      path: 'filter-operator-benchmark-results.png',
      fullPage: true,
    });
    console.info(
      '📸 Screenshot saved to: filter-operator-benchmark-results.png\n'
    );
  }, 360000); // 6 minute timeout

  afterAll(async () => {
    appProcess.kill('SIGTERM');
    await browser.close();
  });
});
