import { MeerkatQueryOptions } from '../../types/cube-types';
import { memberKeyToSafeKey } from '../member-key-to-safe-key';

describe('memberKeyToSafeKey', () => {
  const optionsDisabled: MeerkatQueryOptions = { isDotDelimiterEnabled: false };
  const optionsEnabled: MeerkatQueryOptions = { isDotDelimiterEnabled: true };

  describe('with isDotDelimiterEnabled=false (default)', () => {
    it('should convert a single delimiter correctly', () => {
      expect(memberKeyToSafeKey('data.source', optionsDisabled)).toBe(
        'data__source'
      );
    });

    it('should convert multiple delimiters correctly', () => {
      expect(memberKeyToSafeKey('data.source.field', optionsDisabled)).toBe(
        'data__source__field'
      );
    });

    it('should handle strings with no delimiters', () => {
      expect(memberKeyToSafeKey('datasource', optionsDisabled)).toBe(
        'datasource'
      );
    });

    it('should handle empty string', () => {
      expect(memberKeyToSafeKey('', optionsDisabled)).toBe('');
    });

    it('should handle strings with consecutive delimiters', () => {
      expect(memberKeyToSafeKey('data..source', optionsDisabled)).toBe(
        'data____source'
      );
    });

    it('should handle strings with leading/trailing delimiters', () => {
      expect(memberKeyToSafeKey('.data.source.', optionsDisabled)).toBe(
        '__data__source__'
      );
    });
  });

  describe('with isDotDelimiterEnabled=true', () => {
    it('should preserve a single delimiter', () => {
      expect(memberKeyToSafeKey('data.source', optionsEnabled)).toBe(
        'data.source'
      );
    });

    it('should preserve multiple delimiters', () => {
      expect(memberKeyToSafeKey('data.source.field', optionsEnabled)).toBe(
        'data.source.field'
      );
    });

    it('should handle strings with no delimiters', () => {
      expect(memberKeyToSafeKey('datasource', optionsEnabled)).toBe(
        'datasource'
      );
    });

    it('should handle empty string', () => {
      expect(memberKeyToSafeKey('', optionsEnabled)).toBe('');
    });

    it('should preserve strings with consecutive delimiters', () => {
      expect(memberKeyToSafeKey('data..source', optionsEnabled)).toBe(
        'data..source'
      );
    });

    it('should preserve strings with leading/trailing delimiters', () => {
      expect(memberKeyToSafeKey('.data.source.', optionsEnabled)).toBe(
        '.data.source.'
      );
    });
  });
});
