import * as puppeteer from 'puppeteer';

describe('Google', () => {
  let page;
  let browser;

  beforeAll(async () => {
    browser = await puppeteer.launch({
      headless: 'new',
    });
    page = await browser.newPage();
    await page.goto('https://devrev.ai');
  }, 30000);

  it('Should display "DevRev" text on page', async () => {
    const pageContent = await page.content();
    expect(pageContent).toContain('Tech');
  }, 30000);

  afterAll(async () => {
    await browser.close();
  });
});
