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

  await db.registerFileText(`${tableName}.json`, JSON.stringify(json));

  await connection.insertJSONFromPath(`${tableName}.json`, {
    name: tableName,
  });

  await connection.query(
    `COPY ${tableName} TO '${tableName}.parquet' (FORMAT PARQUET)';`
  );

  const buffer = await db.copyFileToBuffer(`${tableName}.parquet`);

  await connection.close();

  return buffer;
};
