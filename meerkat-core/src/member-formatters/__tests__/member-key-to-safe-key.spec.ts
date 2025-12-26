import { memberKeyToSafeKey } from '../member-key-to-safe-key';

describe('memberKeyToSafeKey', () => {
  describe('with isDotDelimiterEnabled=false (default)', () => {
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

    it('should explicitly accept false flag', () => {
      expect(memberKeyToSafeKey('data.source', false)).toBe('data__source');
    });
  });

  describe('with isDotDelimiterEnabled=true', () => {
    it('should preserve a single delimiter', () => {
      expect(memberKeyToSafeKey('data.source', true)).toBe('data.source');
    });

    it('should preserve multiple delimiters', () => {
      expect(memberKeyToSafeKey('data.source.field', true)).toBe(
        'data.source.field'
      );
    });

    it('should handle strings with no delimiters', () => {
      expect(memberKeyToSafeKey('datasource', true)).toBe('datasource');
    });

    it('should handle empty string', () => {
      expect(memberKeyToSafeKey('', true)).toBe('');
    });

    it('should preserve strings with consecutive delimiters', () => {
      expect(memberKeyToSafeKey('data..source', true)).toBe('data..source');
    });

    it('should preserve strings with leading/trailing delimiters', () => {
      expect(memberKeyToSafeKey('.data.source.', true)).toBe('.data.source.');
    });
  });
});
