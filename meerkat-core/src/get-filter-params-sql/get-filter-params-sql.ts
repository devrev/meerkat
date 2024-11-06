import { astDeserializerQuery, deserializeQuery } from "../ast-deserializer/ast-deserializer";
import { getFilterParamsAST } from "../filter-params/filter-params-ast";
import { FilterType, Query, TableSchema } from "../types/cube-types";

export const getFilterParamsSQL = async ({
  query,
  tableSchema,
  filterType,
  getQueryOutput
}: {
  query: Query;
  tableSchema: TableSchema;
  filterType?: FilterType;
  getQueryOutput: (query: string) => Promise<any>;
}) => {
  const filterParamsAST = getFilterParamsAST(
    query,
    tableSchema,
    filterType
  );
  const filterParamsSQL = [];
  for (const filterParamAST of filterParamsAST) {
    if (!filterParamAST.ast) {
      continue;
    }

    const queryOutput = await getQueryOutput(astDeserializerQuery(filterParamAST.ast))

    const sql = deserializeQuery(queryOutput);

    filterParamsSQL.push({
      memberKey: filterParamAST.memberKey,
      sql: sql,
      matchKey: filterParamAST.matchKey,
    });
  }
  return filterParamsSQL;
};
