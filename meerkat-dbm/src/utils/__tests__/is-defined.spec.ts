import { isDefined } from '../is-defined';

describe('isDefined', () => {
  test('should return false if value is undefined', () => {
    expect(isDefined(undefined)).toBe(false);
  });

  test('should return true if value is defined', () => {
    expect(isDefined('defined')).toBe(true);
  });
});
