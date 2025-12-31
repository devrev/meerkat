export { COLUMN_NAME_DELIMITER, MEERKAT_OUTPUT_DELIMITER } from './constants';
export {
  // New flag-aware API (recommended)
  AliasConfig,
  constructAliasForAST,
  constructAliasForSQL,
  DEFAULT_ALIAS_CONFIG,
  getAliasForAST,
  getAliasForSQL,
  // Legacy API (deprecated)
  constructAlias,
  constructCompoundAlias,
  getAliasFromSchema,
} from './get-alias';
export { getNamespacedKey } from './get-namespaced-key';
export {
  memberKeyToSafeKey,
  MemberKeyToSafeKeyOptions,
} from './member-key-to-safe-key';
export { splitIntoDataSourceAndFields } from './split-into-data-source-and-fields';
