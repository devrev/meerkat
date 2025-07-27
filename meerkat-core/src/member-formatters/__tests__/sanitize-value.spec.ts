import { sanitizeStringValue } from '../sanitize-value';

describe('sanitizeStringValue', () => {
  it('should escape single quotes', () => {
    expect(sanitizeStringValue("I'm a string")).toBe("I''m a string");
  });

  it('should escape double quotes', () => {
    expect(sanitizeStringValue('I"m a string')).toBe('I"m a string');
  });
});
