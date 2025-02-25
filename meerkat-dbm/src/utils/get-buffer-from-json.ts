import { InstanceManagerType } from '../dbm';
import { DBMEvent, DBMLogger } from '../logger';

/**
 * Converts a JSON object to a Uint8Array by writing it to a Parquet file in a DuckDB database.
 * Also emits an event with the time taken for the conversion.
 *
 * @param instanceManager - The instance manager for DuckDB.
 * @param json - The JSON object to be converted.
 * @param tableName - The name of the table.
 * @returns A converted Uint8Array of the JSON object.
 */

export const getBufferFromJSON = async ({
  instanceManager,
  json,
  tableName,
  logger,
  onEvent,
  metadata,
}: {
  instanceManager: InstanceManagerType;
  json: object;
  tableName: string;
  logger?: DBMLogger;
  onEvent?: (event: DBMEvent) => void;
  metadata?: object;
}): Promise<Uint8Array> => {
  const jsonFileName = `${tableName}.json`;
  const parquetFileName = `${tableName}.parquet`;

  const db = await instanceManager.getDB();

  const connection = await db.connect();

  const startConversionTime = performance.now();

  // Register the JSON content as a json file in the DuckDB.
  await db.registerFileText(jsonFileName, JSON.stringify(json));

  // Create a table with the JSON file.
  await connection.insertJSONFromPath(jsonFileName, {
    name: tableName,
  });

  // Copy the content of the table into a Parquet file.
  await connection.query(
    `COPY ${tableName} TO '${parquetFileName}' (FORMAT PARQUET);`
  );

  // Copy the content of the Parquet file into a Uint8Array buffer.
  const buffer = await db.copyFileToBuffer(parquetFileName);

  const endConversionTime = performance.now();

  const timeTaken = endConversionTime - startConversionTime;

  logger?.debug(
    'Time spent in converting JSON to buffer:',
    timeTaken,
    'ms',
    json
  );

  onEvent?.({
    event_name: 'json_to_buffer_conversion_duration',
    duration: timeTaken,
    metadata: { ...metadata, json },
  });

  // Drop the table.
  await connection.query(`DROP TABLE ${tableName};`);

  await db.registerEmptyFileBuffer(jsonFileName);

  await connection.close();

  return buffer;
};
