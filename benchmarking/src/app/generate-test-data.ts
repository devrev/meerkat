/**
 * Generate synthetic test data for IN vs ANY operator benchmark
 * Creates a dataset similar to production data with high-cardinality string columns
 */

export interface TestDataRow {
  id: string;
  engineering_pod: string;
  type: string;
  subtype: string;
  year: string;
  state: string;
  priority: string;
  created_date: string;
  trip_miles: number;
  trip_duration: number;
  base_num: string;
  license_num: string;
}

// Realistic engineering pod IDs (similar to your example)
const ENGINEERING_PODS = [
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/6',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/10',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:product/33',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/180',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/3133',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14904',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12394',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:capability/17',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/16',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14829',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/13499',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/13396',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14723',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/181',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:capability/97',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12558',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/27',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/14539',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12706',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/3795',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11991',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11985',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11997',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11994',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/12555',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11150',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11993',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11995',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11992',
  'don:core:dvrv-in-1:devo/2sRI6Hepzz:enhancement/11996',
];

const BASE_NUMS = [
  'B03406',
  'B03256',
  'B03404',
  'B02510',
  'B02872',
  'B02764',
  'B02887',
  'B02800',
  'B02869',
  'B02682',
  'B02617',
  'B02598',
  'B02512',
  'B02765',
  'B02878',
  'B02395',
  'B02777',
  'B02835',
  'B02836',
  'B02864',
];

const LICENSE_NUMS = ['HV0002', 'HV0003', 'HV0004', 'HV0005'];
const TYPES = ['issue', 'ticket', 'task'];
const SUBTYPES = ['pse', 'bug', 'feature', 'support'];
const YEARS = ['2023', '2024', '2025'];
const STATES = ['open', 'in_progress', 'closed', 'blocked'];
const PRIORITIES = ['p0', 'p1', 'p2', 'p3'];

function randomItem<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomNumber(min: number, max: number): number {
  return Math.random() * (max - min) + min;
}

function generateDate(year: string): string {
  const month = Math.floor(Math.random() * 12) + 1;
  const day = Math.floor(Math.random() * 28) + 1;
  return `${year}-${month.toString().padStart(2, '0')}-${day
    .toString()
    .padStart(2, '0')}`;
}

export function generateTestData(rowCount: number): TestDataRow[] {
  const data: TestDataRow[] = [];

  for (let i = 0; i < rowCount; i++) {
    const year = randomItem(YEARS);
    data.push({
      id: `id_${i}`,
      engineering_pod: randomItem(ENGINEERING_PODS),
      type: randomItem(TYPES),
      subtype: randomItem(SUBTYPES),
      year,
      state: randomItem(STATES),
      priority: randomItem(PRIORITIES),
      created_date: generateDate(year),
      trip_miles: randomNumber(0.5, 50),
      trip_duration: randomNumber(5, 120),
      base_num: randomItem(BASE_NUMS),
      license_num: randomItem(LICENSE_NUMS),
    });
  }

  return data;
}

export function dataToCSV(data: TestDataRow[]): string {
  const headers = Object.keys(data[0]).join(',');
  const rows = data.map((row) =>
    Object.values(row)
      .map((val) => (typeof val === 'string' ? `"${val}"` : val))
      .join(',')
  );
  return [headers, ...rows].join('\n');
}

export function dataToJSON(data: TestDataRow[]): string {
  return JSON.stringify(data, null, 2);
}
