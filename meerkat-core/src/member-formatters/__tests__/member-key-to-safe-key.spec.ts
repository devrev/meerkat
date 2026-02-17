import { memberKeyToSafeKey } from '../member-key-to-safe-key';

describe('memberKeyToSafeKey', () => {
  it('should convert a single delimiter correctly', () => {
    expect(memberKeyToSafeKey('data.source')).toBe('data__source');
  });

  it('should convert multiple delimiters correctly', () => {
    expect(memberKeyToSafeKey('data.source.field')).toBe(
      'data__source__field'
    );
  });

  it('should handle strings with no delimiters', () => {
    expect(memberKeyToSafeKey('datasource')).toBe('datasource');
  });

  it('should handle empty string', () => {
    expect(memberKeyToSafeKey('')).toBe('');
  });

  it('should handle strings with consecutive delimiters', () => {
    expect(memberKeyToSafeKey('data..source')).toBe('data____source');
  });

  it('should handle strings with leading/trailing delimiters', () => {
    expect(memberKeyToSafeKey('.data.source.')).toBe('__data__source__');
  });
});
