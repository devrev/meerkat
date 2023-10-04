import { convertFloatToInt, getTypeInfo } from './get-type-info';
describe('getTypeInfo', () => {
  it('should correctly calculate width and scale for a floating point number', () => {
    const num = 2.2;
    const result = getTypeInfo(num);

    expect(result).toEqual({
      type: 'DECIMAL_TYPE_INFO',
      alias: '',
      width: 2,
      scale: 1,
    });
  });

  it('should correctly calculate width and scale for a whole number', () => {
    const num = 123;
    const result = getTypeInfo(num);

    expect(result).toEqual({
      type: 'DECIMAL_TYPE_INFO',
      alias: '',
      width: 3,
      scale: 0,
    });
  });

  it('should correctly calculate width and scale for a number with more than one digit after decimal', () => {
    const num = 123.456;
    const result = getTypeInfo(num);

    expect(result).toEqual({
      type: 'DECIMAL_TYPE_INFO',
      alias: '',
      width: 6,
      scale: 3,
    });
  });
});

describe('convertFloatToInt', () => {
  it('should correctly remove decimal point from a floating point number', () => {
    const num = 123.45;
    const result = convertFloatToInt(num);

    expect(result).toBe(12345);
  });

  it('should correctly convert a whole number', () => {
    const num = 123;
    const result = convertFloatToInt(num);

    expect(result).toBe(123);
  });

  it('should correctly handle a number with more than one digit after decimal', () => {
    const num = 123.456;
    const result = convertFloatToInt(num);

    expect(result).toBe(123456);
  });
});
