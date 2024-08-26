/**
 * Converts a Uint8Array to a SharedArrayBuffer.
 *
 * @param buffer - The Uint8Array to be converted.
 * @returns A SharedArrayBuffer containing the same data as the input Uint8Array.
 */
export const convertUint8ArrayToSharedArrayBuffer = (buffer: Uint8Array) => {
  // Create a new SharedArrayBuffer with the same byte length as the input buffer.
  const sharedBuffer = new SharedArrayBuffer(buffer.byteLength);

  // Create a view of the SharedArrayBuffer as a Uint8Array.
  const fileBufferView = new Uint8Array(sharedBuffer);

  // Copy the data from the input Uint8Array to the SharedArrayBuffer view.
  fileBufferView.set(new Uint8Array(buffer));

  // Return the SharedArrayBuffer containing the copied data.
  return sharedBuffer;
};

/**
 * Converts a SharedArrayBuffer to a Uint8Array.
 *
 * @param sharedBuffer - The SharedArrayBuffer to be converted.
 * @returns A Uint8Array containing the same data as the input SharedArrayBuffer.
 */
export const convertSharedArrayBufferToUint8Array = (
  sharedBuffer: SharedArrayBuffer
) => {
  // Create a new Uint8Array with the same length as the SharedArrayBuffer
  const newBuffer = new Uint8Array(sharedBuffer.byteLength);

  // Create a Uint8Array view of the SharedArrayBuffer
  // and copy its data to the newly created Uint8Array
  newBuffer.set(new Uint8Array(sharedBuffer));

  // Return the newly created Uint8Array containing the copied data
  return newBuffer;
};
