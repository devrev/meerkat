import {
  AggregateHandling,
  ExpressionClass,
  ExpressionType,
  QueryNodeType,
  SelectStatement,
  TableReferenceType,
} from '@devrev/duckdb-serialization-types';

export const BASE_TABLE_NAME = 'REPLACE_BASE_TABLE';

export const getBaseAST = (): SelectStatement => {
  return {
    node: {
      type: QueryNodeType.SELECT_NODE,
      modifiers: [],
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      //@ts-ignore
      cte_map: { map: [] },
      select_list: [
        {
          class: ExpressionClass.STAR,
          type: ExpressionType.STAR,
          alias: '',
          relation_name: '',
          exclude_list: [],
          replace_list: [],
          columns: false,
          expr: null,
        },
      ],
      from_table: {
        type: TableReferenceType.BASE_TABLE,
        alias: '',
        sample: null,
        schema_name: '',
        table_name: BASE_TABLE_NAME,
        column_name_alias: [],
        catalog_name: '',
      },
      where_clause: undefined,
      group_expressions: [],
      group_sets: [],
      aggregate_handling: AggregateHandling.STANDARD_HANDLING,
      having: null,
      sample: null,
      qualify: null,
    },
  };
};
