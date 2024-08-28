import {
  convertSharedArrayBufferToUint8Array,
  convertUint8ArrayToSharedArrayBuffer,
} from '../array-convertor';

describe('Buffer Conversion Functions', () => {
  test('convertUint8ArrayToSharedArrayBuffer should correctly convert Uint8Array to SharedArrayBuffer', () => {
    // Input Uint8Array
    const inputArray = new Uint8Array([1, 2, 3, 4, 5]);

    // Convert to SharedArrayBuffer
    const sharedBuffer = convertUint8ArrayToSharedArrayBuffer(inputArray);

    // Create a view of the SharedArrayBuffer
    const sharedArrayView = new Uint8Array(sharedBuffer);

    // Assert that the SharedArrayBuffer has the same length and data as the input Uint8Array
    expect(sharedArrayView.byteLength).toBe(inputArray.length);
    expect(sharedArrayView).toEqual(inputArray);
  });

  test('convertSharedArrayBufferToUint8Array should correctly convert SharedArrayBuffer to Uint8Array', () => {
    // Input SharedArrayBuffer
    const inputArray = new Uint8Array([10, 20, 30, 40, 50]);
    const sharedBuffer = convertUint8ArrayToSharedArrayBuffer(inputArray);

    // Convert back to Uint8Array
    const resultArray = convertSharedArrayBufferToUint8Array(sharedBuffer);

    // Assert that the Uint8Array has the same length and data as the original input
    expect(resultArray.length).toBe(inputArray.length);
    expect(resultArray).toEqual(inputArray);
  });
});
