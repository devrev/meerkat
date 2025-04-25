import { splitIntoDataSourceAndFields } from '../member-formatters/split-into-data-source-and-fields';
export function getTypeInfo(num: number) {
  const numString = num.toString();

  let width = numString.length;
  if (numString.includes('.')) {
    width -= 1; // subtract 1 to exclude the dot from the count of digits
  }

  /**
   * If the number is a whole number, then the scale is 0
   * If the number is a floating point number, then the scale is the number of digits after the decimal
   */

  const scale = splitIntoDataSourceAndFields(numString)[1]?.length || 0;

  const typeInfo = {
    type: 'DECIMAL_TYPE_INFO',
    alias: '',
    width: width,
    scale: scale,
  };

  return typeInfo;
}

export const convertFloatToInt = (num: number) => {
  const numString = num.toString();

  //Remove dot from the number string
  const numStringWithoutDot = numString.replace('.', '');

  //Convert the number string to a number
  const numWithoutDot = parseInt(numStringWithoutDot);

  return numWithoutDot;
};
