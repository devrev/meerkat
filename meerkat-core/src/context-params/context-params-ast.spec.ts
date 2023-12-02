import {
  applyContextParamsToBaseSQL,
  detectAllContextParams,
  detectApplyContextParamsToBaseSQL,
} from './context-params-ast';

describe('detectAllContextParams function', () => {
  it('should extract all CONTEXT_PARAMS from SQL string', () => {
    const sql = 'SELECT * FROM orders WHERE ${CONTEXT_PARAMS.current_user.id}';
    const expected = [
      {
        memberKey: 'current_user.id',
        matchKey: '${CONTEXT_PARAMS.current_user.id}',
      },
    ];
    expect(detectAllContextParams(sql)).toEqual(expected);
  });

  it('Should match with multiple keys in the same string', () => {
    const sql =
      'SELECT * FROM orders WHERE ${CONTEXT_PARAMS.current_user.id} AND ${CONTEXT_PARAMS.current_user.name}';
    const expected = [
      {
        memberKey: 'current_user.id',
        matchKey: '${CONTEXT_PARAMS.current_user.id}',
      },
      {
        memberKey: 'current_user.name',
        matchKey: '${CONTEXT_PARAMS.current_user.name}',
      },
    ];
    expect(detectAllContextParams(sql)).toEqual(expected);
  });

  it('should work with CONTEXT_PARAMS with deeper object keys', () => {
    const sql =
      'SELECT * FROM orders WHERE ${CONTEXT_PARAMS.current_user.id} AND ${CONTEXT_PARAMS.current_user.name} AND ${CONTEXT_PARAMS.current_user.address.city}';
    const expected = [
      {
        memberKey: 'current_user.id',
        matchKey: '${CONTEXT_PARAMS.current_user.id}',
      },
      {
        memberKey: 'current_user.name',
        matchKey: '${CONTEXT_PARAMS.current_user.name}',
      },
      {
        memberKey: 'current_user.address.city',
        matchKey: '${CONTEXT_PARAMS.current_user.address.city}',
      },
    ];
    expect(detectAllContextParams(sql)).toEqual(expected);
  });
});

describe('applyContextParamsToBaseSQL function', () => {
  it('should replace all CONTEXT_PARAMS with their values', () => {
    const baseSQL =
      'SELECT * FROM orders WHERE ${CONTEXT_PARAMS.current_user.id}';
    const contextParamsSQL = [
      {
        memberKey: 'current_user.id',
        matchKey: '${CONTEXT_PARAMS.current_user.id}',
        contextParamSQL: "'123'",
      },
    ];
    const expected = "SELECT * FROM orders WHERE '123'";
    expect(applyContextParamsToBaseSQL(baseSQL, contextParamsSQL)).toEqual(
      expected
    );
  });
  it('should replace all CONTEXT_PARAMS with their values when there are multiple CONTEXT_PARAMS', () => {
    const baseSQL =
      'SELECT * FROM orders WHERE ${CONTEXT_PARAMS.current_user.id} AND ${CONTEXT_PARAMS.current_user.name}';
    const contextParamsSQL = [
      {
        memberKey: 'current_user.id',
        matchKey: '${CONTEXT_PARAMS.current_user.id}',
        contextParamSQL: "'123'",
      },
      {
        memberKey: 'current_user.name',
        matchKey: '${CONTEXT_PARAMS.current_user.name}',
        contextParamSQL: "'John Doe'",
      },
    ];
    const expected = "SELECT * FROM orders WHERE '123' AND 'John Doe'";
    expect(applyContextParamsToBaseSQL(baseSQL, contextParamsSQL)).toEqual(
      expected
    );
  });
  it('should replace all CONTEXT_PARAMS with their values when there are multiple CONTEXT_PARAMS with deeper object keys', () => {
    const baseSQL =
      'SELECT * FROM orders WHERE ${CONTEXT_PARAMS.current_user.id} AND ${CONTEXT_PARAMS.current_user.name} AND ${CONTEXT_PARAMS.current_user.address.city}';
    const contextParamsSQL = [
      {
        memberKey: 'current_user.id',
        matchKey: '${CONTEXT_PARAMS.current_user.id}',
        contextParamSQL: "'123'",
      },
      {
        memberKey: 'current_user.name',
        matchKey: '${CONTEXT_PARAMS.current_user.name}',
        contextParamSQL: "'John Doe'",
      },
      {
        memberKey: 'current_user.address.city',
        matchKey: '${CONTEXT_PARAMS.current_user.address.city}',
        contextParamSQL: "'New York'",
      },
    ];
    const expected =
      "SELECT * FROM orders WHERE '123' AND 'John Doe' AND 'New York'";
    expect(applyContextParamsToBaseSQL(baseSQL, contextParamsSQL)).toEqual(
      expected
    );
  });
});

describe('detectApplyContextParamsToBaseSQL function', () => {
  it('should return an array of contextParamsSQL', () => {
    const baseSQL =
      'SELECT * FROM orders WHERE ${CONTEXT_PARAMS.current_user.id} AND ${CONTEXT_PARAMS.current_user.name} AND ${CONTEXT_PARAMS.current_user.address.city}';
    const contextParams = {
      'current_user.id': "'123'",
      'current_user.name': "'John Doe'",
      'current_user.address.city': "'New York'",
    };

    const expected = `SELECT * FROM orders WHERE '123' AND 'John Doe' AND 'New York'`;

    expect(detectApplyContextParamsToBaseSQL(baseSQL, contextParams)).toEqual(
      expected
    );
  });
});
