import { InstanceManagerType } from '../dbm';

/**
 * Converts a JSON object to a Uint8Array by writing it to a Parquet file in a DuckDB database.
 * @param instanceManager - The instance manager for DuckDB.
 * @param json - The JSON object to be converted.
 * @param tableName - The name of the table.
 * @returns A converted Uint8Array of the JSON object.
 */

export const getBufferFromJSON = async ({
  instanceManager,
  json,
  tableName,
}: {
  instanceManager: InstanceManagerType;
  json: object;
  tableName: string;
}): Promise<Uint8Array> => {
  const db = await instanceManager.getDB();
  const connection = await db.connect();

  // Register the JSON content as a file in DuckDB with the table name
  await db.registerFileText(`${tableName}.json`, JSON.stringify(json));

  // Insert JSON data into the specified table
  await connection.insertJSONFromPath(`${tableName}.json`, {
    name: tableName,
  });

  // Query to copy the table to a Parquet file
  await connection.query("COPY taxi1 TO 'taxi.parquet' (FORMAT PARQUET)';");

  // Copy Parquet file content to a Uint8Array buffer
  const buffer = await db.copyFileToBuffer('taxi.parquet');

  await connection.close();

  return buffer;
};
