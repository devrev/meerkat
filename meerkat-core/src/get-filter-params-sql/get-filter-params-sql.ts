import {
  batchAstDeserializerQuery,
  deserializeBatchQuery,
} from '../ast-deserializer/ast-deserializer';
import { getFilterParamsAST } from '../filter-params/filter-params-ast';
import { SelectStatement } from '../types/duckdb-serialization-types/serialization/Statement';
import { FilterType, Query, TableSchema } from '../types/cube-types';

export const getFilterParamsSQL = async ({
  query,
  tableSchema,
  filterType,
  getQueryOutput,
}: {
  query: Query;
  tableSchema: TableSchema;
  filterType: FilterType;
  getQueryOutput: (query: string) => Promise<Record<string, string>[]>;
}) => {
  const filterParamsAST = getFilterParamsAST(query, tableSchema, filterType);

  // Collect every non-null AST so the whole set can be deserialized in a
  // single DuckDB round-trip instead of one trip per filter-param placeholder.
  const pending = filterParamsAST.filter(
    (filterParamAST) => filterParamAST.ast
  );

  if (pending.length === 0) {
    return [];
  }

  const batchQuery = batchAstDeserializerQuery(
    pending.map((filterParamAST) => filterParamAST.ast as SelectStatement)
  );

  // batchQuery is non-null here because pending.length > 0.
  const queryOutput = await getQueryOutput(batchQuery as string);
  const sqls = deserializeBatchQuery(queryOutput, pending.length);

  return pending.map((filterParamAST, index) => ({
    memberKey: filterParamAST.memberKey,
    sql: sqls[index],
    matchKey: filterParamAST.matchKey,
  }));
};
