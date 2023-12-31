import { InstanceManagerType } from '../dbm';
import { DBMEvent, DBMLogger } from '../logger';

/**
 * Converts a JSON object to a Uint8Array by writing it to a Parquet file in a DuckDB database.
 * Also emits an event with the time taken for the conversion.
 *
 * @param instanceManager - The instance manager for DuckDB.
 * @param json - The JSON object to be converted.
 * @param tableName - The name of the table.
 * @param logger - The logger instance.
 * @param onEvent - The callback function to handle events.
 * @param metadata - Additional information for the event.
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
  const db = await instanceManager.getDB();

  const connection = await db.connect();

  const startConversionTime = Date.now();

  await db.registerFileText(`${tableName}.json`, JSON.stringify(json));

  await connection.insertJSONFromPath(`${tableName}.json`, {
    name: tableName,
  });

  await connection.query(
    `COPY ${tableName} TO '${tableName}.parquet' (FORMAT PARQUET)';`
  );

  const buffer = await db.copyFileToBuffer(`${tableName}.parquet`);

  const endConversionTime = Date.now();

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
    metadata,
  });

  await connection.close();

  return buffer;
};
