import {
  applyFilterParamsToBaseSQL,
  detectAllFilterParamsFromSQL,
  getFilterByMemberKey,
} from './filter-params-ast';

describe('getFilterByMemberKey', () => {
  it('should return an empty array when filters are undefined', () => {
    const result = getFilterByMemberKey(undefined, 'memberKey');
    expect(result).toEqual([]);
  });

  it('should return correct filters that match with member key', () => {
    const filters = [
      { member: 'memberKey', operator: 'equals', values: ['value1'] },
      { member: 'differentMember', operator: 'equals', values: ['value1'] },
      {
        and: [{ member: 'memberKey', operator: 'equals', values: ['value1'] }],
      },
      { or: [{ member: 'memberKey', operator: 'equals', values: ['value1'] }] },
    ];
    const expectedOutput = [
      { member: 'memberKey', operator: 'equals', values: ['value1'] },
      {
        and: [{ member: 'memberKey', operator: 'equals', values: ['value1'] }],
      },
      { or: [{ member: 'memberKey', operator: 'equals', values: ['value1'] }] },
    ];
    const result = getFilterByMemberKey(filters, 'memberKey');
    expect(result).toEqual(expectedOutput);
  });

  it('should return correct filters that match with member key with OR in different key', () => {
    const filters = [
      { member: 'memberKey', operator: 'equals', values: ['value1'] },
      { member: 'differentMember', operator: 'equals', values: ['value1'] },
      {
        and: [
          { member: 'memberKey', operator: 'equals', values: ['value1'] },
          { member: 'differentMember', operator: 'equals', values: ['value1'] },
        ],
      },
      { or: [{ member: 'memberKey', operator: 'equals', values: ['value1'] }] },
    ];
    const expectedOutput = [
      { member: 'memberKey', operator: 'equals', values: ['value1'] },
      {
        and: [{ member: 'memberKey', operator: 'equals', values: ['value1'] }],
      },
      { or: [{ member: 'memberKey', operator: 'equals', values: ['value1'] }] },
    ];
    const result = getFilterByMemberKey(filters, 'memberKey');
    expect(result).toEqual(expectedOutput);
  });

  it('should return an empty array when no filters match with member key', () => {
    const filters = [
      { member: 'differentMember', operator: 'equals', values: ['value1'] },
      {
        and: [
          { member: 'differentMember', operator: 'equals', values: ['value1'] },
        ],
      },
      {
        or: [
          { member: 'differentMember', operator: 'equals', values: ['value1'] },
        ],
      },
    ];
    const result = getFilterByMemberKey(filters, 'memberKey');
    expect(result).toEqual([
      {
        and: [],
      },
    ]);
  });
});

describe('detectAllFilterParamsFromSQL function', () => {
  it('should extract all FILTER_PARAMS from SQL string', () => {
    const sql =
      "SELECT * FROM orders WHERE ${FILTER_PARAMS.order_facts.date.filter('date')} AND ${FILTER_PARAMS.order_facts.status.filter('status')}";
    const expected = [
      {
        memberKey: 'order_facts.date',
        filterExpression: 'date',
        matchKey: "${FILTER_PARAMS.order_facts.date.filter('date')}",
      },
      {
        memberKey: 'order_facts.status',
        filterExpression: 'status',
        matchKey: "${FILTER_PARAMS.order_facts.status.filter('status')}",
      },
    ];
    expect(detectAllFilterParamsFromSQL(sql)).toEqual(expected);
  });

  it('should return an array with a single FILTER_PARAM when only one is present', () => {
    const sql =
      "SELECT * FROM orders WHERE ${FILTER_PARAMS.order_facts.date.filter('date')}";
    const expected = [
      {
        memberKey: 'order_facts.date',
        filterExpression: 'date',
        matchKey: "${FILTER_PARAMS.order_facts.date.filter('date')}",
      },
    ];
    expect(detectAllFilterParamsFromSQL(sql)).toEqual(expected);
  });

  it('should return an empty array when no FILTER_PARAMS are present', () => {
    const sql = "SELECT * FROM orders WHERE date > '2000-01-01'";
    expect(detectAllFilterParamsFromSQL(sql)).toEqual([]);
  });

  it('should handle multiple instances of the same FILTER_PARAM', () => {
    const sql =
      "SELECT * FROM orders WHERE ${FILTER_PARAMS.order_facts.date.filter('date')} AND ${FILTER_PARAMS.order_facts.date.filter('another_date')}";
    const expected = [
      {
        memberKey: 'order_facts.date',
        filterExpression: 'date',
        matchKey: "${FILTER_PARAMS.order_facts.date.filter('date')}",
      },
      {
        memberKey: 'order_facts.date',
        filterExpression: 'another_date',
        matchKey: "${FILTER_PARAMS.order_facts.date.filter('another_date')}",
      },
    ];
    expect(detectAllFilterParamsFromSQL(sql)).toEqual(expected);
  });
});

describe('applyFilterParamsToBaseSQL function', () => {
  it('should replace filter params in base SQL with corresponding SQL expressions', () => {
    const baseSQL =
      "SELECT * FROM orders WHERE ${FILTER_PARAMS.order_facts.date.filter('date')} AND ${FILTER_PARAMS.order_facts.status.filter('status')}";
    const filterParamsSQL = [
      {
        memberKey: 'order_facts.date',
        sql: "SELECT * FROM orders WHERE date > '2022-01-01'",
        matchKey: "${FILTER_PARAMS.order_facts.date.filter('date')}",
      },
      {
        memberKey: 'order_facts.status',
        sql: "SELECT * FROM orders WHERE status = 'completed'",
        matchKey: "${FILTER_PARAMS.order_facts.status.filter('status')}",
      },
    ];
    const expected =
      "SELECT * FROM orders WHERE  date > '2022-01-01' AND  status = 'completed'";
    expect(applyFilterParamsToBaseSQL(baseSQL, filterParamsSQL)).toBe(expected);
  });

  it('should return the base SQL unchanged if no filterParamsSQL are provided', () => {
    const baseSQL =
      "SELECT * FROM orders WHERE date > '2022-01-01' AND status = 'completed'";
    const filterParamsSQL = [];
    expect(applyFilterParamsToBaseSQL(baseSQL, filterParamsSQL)).toBe(baseSQL);
  });

  it('should handle cases where the base SQL does not include the matchKey', () => {
    const baseSQL =
      "SELECT * FROM orders WHERE date > '2022-01-01' AND status = 'completed'";
    const filterParamsSQL = [
      {
        memberKey: 'order_facts.date',
        sql: "SELECT * FROM orders WHERE date < '2022-01-01'",
        matchKey: "${FILTER_PARAMS.order_facts.date.filter('date')}",
      },
    ];
    expect(applyFilterParamsToBaseSQL(baseSQL, filterParamsSQL)).toBe(baseSQL);
  });

  it('Should handle the case if the filter params is not getting replaced filterParamsSQL with true', () => {
    const baseSQL =
      "SELECT * FROM orders WHERE ${FILTER_PARAMS.order_facts.date.filter('date')}";
    const filterParamsSQL = [];

    const expected = 'SELECT * FROM orders WHERE TRUE';
    expect(applyFilterParamsToBaseSQL(baseSQL, filterParamsSQL)).toBe(expected);
  });
});
