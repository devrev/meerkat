import { COLUMN_NAME_DELIMITER, MEERKAT_OUTPUT_DELIMITER } from './constants';

/**
 * Converts a member key to a safe key for use in SQL.
 * Converts dots to double underscores for SQL-safe identifiers.
 *
 * @param memberKey - The member key (e.g., "orders.customer_id")
 * @returns The safe key (e.g., "orders__customer_id")
 */
export const memberKeyToSafeKey = (memberKey: string): string => {
  return memberKey.split(COLUMN_NAME_DELIMITER).join(MEERKAT_OUTPUT_DELIMITER);
};
