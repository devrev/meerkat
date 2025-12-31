import { COLUMN_NAME_DELIMITER, MEERKAT_OUTPUT_DELIMITER } from './constants';

export interface MemberKeyToSafeKeyOptions {
  /**
   * When true, keeps the dot notation (returns memberKey as-is).
   * When false (default), converts dots to underscores for SQL-safe identifiers.
   */
  useDotNotation?: boolean;
}

/**
 * Converts a member key to a safe key for use in SQL.
 *
 * @param memberKey - The member key (e.g., "orders.customer_id")
 * @param options - Configuration options
 * @returns The safe key:
 *   - With useDotNotation=true: "orders.customer_id" (unchanged)
 *   - With useDotNotation=false (default): "orders__customer_id"
 */
export const memberKeyToSafeKey = (
  memberKey: string,
  options?: MemberKeyToSafeKeyOptions
): string => {
  if (options?.useDotNotation) {
    // Keep dot notation - the caller is responsible for quoting when needed
    return memberKey;
  }
  // Legacy behavior: convert dots to double underscores
  return memberKey.split(COLUMN_NAME_DELIMITER).join(MEERKAT_OUTPUT_DELIMITER);
};
