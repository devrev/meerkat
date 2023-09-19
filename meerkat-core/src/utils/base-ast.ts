import {
  AggregateHandling,
  ExpressionClass,
  ExpressionType,
  QueryNodeType,
  SelectStatement,
  TableReferenceType,
} from '@devrev/duckdb-serialization-types';

export const getBaseAST = (): SelectStatement => {
  return {
    node: {
      type: QueryNodeType.SELECT_NODE,
      modifiers: [],
      cte_map: { map: {} },
      select_list: [
        {
          class: ExpressionClass.STAR,
          type: ExpressionType.STAR,
          alias: '',
          relation_name: '',
          columns: false,
        },
      ],
      from_table: {
        type: TableReferenceType.BASE_TABLE,
        alias: '',
        sample: null,
        schema_name: '',
        table_name: 'REPLACE_BASE_TABLE',
        column_name_alias: [],
        catalog_name: '',
      },
      where_clause: undefined,
      group_expressions: [],
      group_sets: new Set(),
      aggregate_handling: AggregateHandling.STANDARD_HANDLING,
      having: null,
      sample: null,
      qualify: null,
    },
  };
};
