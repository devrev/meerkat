import { splitIntoDataSourceAndFields } from '../split-into-data-source-and-fields';

describe('splitIntoDataSourceAndFields', () => {
  it('should split a member with one field correctly', () => {
    const result = splitIntoDataSourceAndFields('data.source');
    expect(result).toEqual(['data', 'source']);
  });

  it('should split a member with multiple fields correctly', () => {
    const result = splitIntoDataSourceAndFields('data.source.field1.field2');
    expect(result).toEqual(['data', 'source.field1.field2']);
  });

  it('should handle a member with no fields', () => {
    const result = splitIntoDataSourceAndFields('data');
    expect(result).toEqual(['data', '']);
  });

  it('should handle an empty string', () => {
    const result = splitIntoDataSourceAndFields('');
    expect(result).toEqual(['', '']);
  });

  it('should handle a member with leading delimiter', () => {
    const result = splitIntoDataSourceAndFields('.data.source');
    expect(result).toEqual(['', 'data.source']);
  });

  it('should handle a member with trailing delimiter', () => {
    const result = splitIntoDataSourceAndFields('data.source.');
    expect(result).toEqual(['data', 'source.']);
  });

  it('should handle consecutive delimiters', () => {
    const result = splitIntoDataSourceAndFields('data..source');
    expect(result).toEqual(['data', '.source']);
  });
});
