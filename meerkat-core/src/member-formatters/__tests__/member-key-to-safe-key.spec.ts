import { memberKeyToSafeKey } from '../member-key-to-safe-key';

const defaultConfig = { useDotNotation: false };

describe('memberKeyToSafeKey', () => {
  describe('with useDotNotation: false', () => {
    it('should convert a single delimiter correctly', () => {
      expect(memberKeyToSafeKey('data.source', defaultConfig)).toBe(
        'data__source'
      );
    });

    it('should convert multiple delimiters correctly', () => {
      expect(memberKeyToSafeKey('data.source.field', defaultConfig)).toBe(
        'data__source__field'
      );
    });

    it('should handle strings with no delimiters', () => {
      expect(memberKeyToSafeKey('datasource', defaultConfig)).toBe(
        'datasource'
      );
    });

    it('should handle empty string', () => {
      expect(memberKeyToSafeKey('', defaultConfig)).toBe('');
    });

    it('should handle strings with consecutive delimiters', () => {
      expect(memberKeyToSafeKey('data..source', defaultConfig)).toBe(
        'data____source'
      );
    });

    it('should handle strings with leading/trailing delimiters', () => {
      expect(memberKeyToSafeKey('.data.source.', defaultConfig)).toBe(
        '__data__source__'
      );
    });
  });

  describe('with useDotNotation: true', () => {
    it('should keep dot notation unchanged', () => {
      expect(memberKeyToSafeKey('data.source', { useDotNotation: true })).toBe(
        'data.source'
      );
    });

    it('should keep multiple dots unchanged', () => {
      expect(
        memberKeyToSafeKey('data.source.field', { useDotNotation: true })
      ).toBe('data.source.field');
    });

    it('should handle strings with no delimiters', () => {
      expect(memberKeyToSafeKey('datasource', { useDotNotation: true })).toBe(
        'datasource'
      );
    });

    it('should handle empty string', () => {
      expect(memberKeyToSafeKey('', { useDotNotation: true })).toBe('');
    });

    it('should keep consecutive delimiters unchanged', () => {
      expect(memberKeyToSafeKey('data..source', { useDotNotation: true })).toBe(
        'data..source'
      );
    });

    it('should keep leading/trailing delimiters unchanged', () => {
      expect(
        memberKeyToSafeKey('.data.source.', { useDotNotation: true })
      ).toBe('.data.source.');
    });
  });
});
