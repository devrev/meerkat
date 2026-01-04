import { TableSchema } from '../types/cube-types';
import { meerkatPlaceholderReplacer } from './meerkat-placeholder-replacer';

const defaultConfig = { useDotNotation: false };
const dotNotationConfig = { useDotNotation: true };

describe('meerkatPlaceholderReplacer (useDotNotation: false)', () => {
  let tableSchema: TableSchema;
  beforeEach(() => {
    tableSchema = {
      name: 'test',
      sql: 'test',
      measures: [
        { name: 'measure1', sql: 'COUNT(*)', type: 'number', alias: 'alias1' },
      ],
      dimensions: [],
    };
  });

  it('should not replace placeholders with tableName if placeholder pattern doesnt end in .', () => {
    const sql = 'SELECT * FROM {MEERKAT}fieldName';
    const tableName = 'customers';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, defaultConfig)
    ).toEqual('SELECT * FROM {MEERKAT}fieldName');
  });

  it('should replace multiple placeholders in the SQL query', () => {
    const sql = 'SELECT {MEERKAT}.a, {MEERKAT}.b FROM orders';
    const tableName = 'orders';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, defaultConfig)
    ).toEqual('SELECT orders__a, orders__b FROM orders');
  });

  it('should be case sensitive', () => {
    const sql = 'SELECT {meerkat}.a FROM orders';
    const tableName = 'orders';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, defaultConfig)
    ).toEqual('SELECT {meerkat}.a FROM orders');
  });

  it('should replace the correct match', () => {
    const sql = 'SELECT {MEERKAT.{MEERKAT}.a}.a FROM customers';
    const tableName = 'customers';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, defaultConfig)
    ).toEqual('SELECT {MEERKAT.customers__a}.a FROM customers');
  });

  it('should handle empty SQL queries', () => {
    const sql = '';
    const tableName = 'customers';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, defaultConfig)
    ).toEqual('');
  });

  it('should handle SQL queries without placeholders', () => {
    const sql = 'SELECT * FROM customers.';
    const tableName = 'orders';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, defaultConfig)
    ).toEqual('SELECT * FROM customers.');
  });

  it('should replace placeholders with alias if provided', () => {
    const sql = 'SELECT {MEERKAT}.measure1 FROM orders';
    const tableName = 'orders';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, defaultConfig)
    ).toEqual('SELECT "alias1" FROM orders');
  });
});

describe('meerkatPlaceholderReplacer (useDotNotation: true)', () => {
  let tableSchema: TableSchema;
  beforeEach(() => {
    tableSchema = {
      name: 'test',
      sql: 'test',
      measures: [
        { name: 'measure1', sql: 'COUNT(*)', type: 'number', alias: 'alias1' },
      ],
      dimensions: [],
    };
  });

  it('should not replace placeholders with tableName if placeholder pattern doesnt end in .', () => {
    const sql = 'SELECT * FROM {MEERKAT}fieldName';
    const tableName = 'customers';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, dotNotationConfig)
    ).toEqual('SELECT * FROM {MEERKAT}fieldName');
  });

  it('should replace multiple placeholders in the SQL query with dot notation', () => {
    const sql = 'SELECT {MEERKAT}.a, {MEERKAT}.b FROM orders';
    const tableName = 'orders';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, dotNotationConfig)
    ).toEqual('SELECT "orders.a", "orders.b" FROM orders');
  });

  it('should be case sensitive', () => {
    const sql = 'SELECT {meerkat}.a FROM orders';
    const tableName = 'orders';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, dotNotationConfig)
    ).toEqual('SELECT {meerkat}.a FROM orders');
  });

  it('should replace the correct match with dot notation', () => {
    const sql = 'SELECT {MEERKAT.{MEERKAT}.a}.a FROM customers';
    const tableName = 'customers';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, dotNotationConfig)
    ).toEqual('SELECT {MEERKAT."customers.a"}.a FROM customers');
  });

  it('should handle empty SQL queries', () => {
    const sql = '';
    const tableName = 'customers';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, dotNotationConfig)
    ).toEqual('');
  });

  it('should handle SQL queries without placeholders', () => {
    const sql = 'SELECT * FROM customers.';
    const tableName = 'orders';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, dotNotationConfig)
    ).toEqual('SELECT * FROM customers.');
  });

  it('should replace placeholders with alias if provided', () => {
    const sql = 'SELECT {MEERKAT}.measure1 FROM orders';
    const tableName = 'orders';
    expect(
      meerkatPlaceholderReplacer(sql, tableName, tableSchema, dotNotationConfig)
    ).toEqual('SELECT "alias1" FROM orders');
  });
});
