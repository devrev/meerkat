import { astDeserializerQuery, deserializeQuery } from "../ast-deserializer/ast-deserializer";
import { getFilterParamsAST } from "../filter-params/filter-params-ast";
import { QueryOptions } from "../member-formatters/get-alias";
import { FilterType, Query, TableSchema } from "../types/cube-types";

export const getFilterParamsSQL = async ({
  query,
  tableSchema,
  filterType,
  getQueryOutput,
  config
}: {
  query: Query;
  tableSchema: TableSchema;
  filterType?: FilterType;
  getQueryOutput: (query: string) => Promise<any>;
  config: QueryOptions;
}) => {
  const filterParamsAST = getFilterParamsAST(
    query,
    tableSchema,
    filterType,
    config
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
