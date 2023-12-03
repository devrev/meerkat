import { ContextParams } from '../types/cube-types/table';

export const detectAllContextParams = (
  sql: string
): {
  memberKey: string;
  matchKey: string;
}[] => {
  const regex = /\$\{CONTEXT_PARAMS\.([^}]*)\}/g;
  const matches = [];
  let match;

  while ((match = regex.exec(sql)) !== null) {
    matches.push({
      memberKey: match[1],
      matchKey: match[0],
    });
  }

  return matches;
};

export const applyContextParamsToBaseSQL = (
  baseSQL: string,
  contextParamsSQL: {
    memberKey: string;
    contextParamSQL: string;
    matchKey: string;
  }[]
) => {
  let newSQL = baseSQL;
  for (const contextParamSQL of contextParamsSQL) {
    // Replace matchKey with contextParamSQL
    newSQL = newSQL.replace(
      contextParamSQL.matchKey,
      contextParamSQL.contextParamSQL
    );
  }
  return newSQL;
};

export const detectApplyContextParamsToBaseSQL = (
  baseSQL: string,
  contextParams: ContextParams
) => {
  const contextParamsSQL = [];
  const contextParamsKeys = detectAllContextParams(baseSQL);

  for (const contextParamsKey of contextParamsKeys) {
    const contextParamSQL = contextParams[contextParamsKey.memberKey];
    if (contextParamSQL) {
      contextParamsSQL.push({
        memberKey: contextParamsKey.memberKey,
        matchKey: contextParamsKey.matchKey,
        contextParamSQL: contextParamSQL,
      });
    }
  }

  return applyContextParamsToBaseSQL(baseSQL, contextParamsSQL);
};
