import { ExpressionClass, ExpressionType } from './Expression';
import { AggregateHandling, QueryNodeType } from './QueryNode';
import { ResultModifierType } from './ResultModifier';
import { SelectStatement } from './Statement';
import { JoinRefType, JoinType, TableReferenceType } from './TableRef';

const dummy: SelectStatement = {
  node: {
    type: QueryNodeType.SELECT_NODE,
    modifiers: [],
    cte_map: { map: {} },
    select_list: [
      {
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        column_names: ['rev_org', 'id'],
      },
      {
        class: ExpressionClass.FUNCTION,
        type: ExpressionType.FUNCTION,
        alias: '',
        function_name: 'count_star',
        schema: '',
        children: [],
        filter: null,
        order_bys: { type: ResultModifierType.ORDER_MODIFIER, orders: [] },
        distinct: false,
        is_operator: false,
        export_state: false,
        catalog: '',
      },
    ],
    from_table: {
      type: TableReferenceType.JOIN,
      alias: '',
      sample: null,
      left: {
        type: TableReferenceType.BASE_TABLE,
        alias: '',
        sample: null,
        schema_name: '',
        table_name: 'rev_org',
        column_name_alias: [],
        catalog_name: '',
      },
      right: {
        type: TableReferenceType.BASE_TABLE,
        alias: '',
        sample: null,
        schema_name: '',
        table_name: 'tickets',
        column_name_alias: [],
        catalog_name: '',
      },
      condition: {
        class: ExpressionClass.COMPARISON,
        type: ExpressionType.COMPARE_EQUAL,
        alias: '',
        left: {
          class: ExpressionClass.COLUMN_REF,
          type: ExpressionType.COLUMN_REF,
          alias: '',
          column_names: ['rev_org', 'id'],
        },
        right: {
          class: ExpressionClass.COLUMN_REF,
          type: ExpressionType.COLUMN_REF,
          alias: '',
          column_names: ['tickets', 'rev_oid'],
        },
      },
      join_type: JoinType.INNER,
      ref_type: JoinRefType.REGULAR,
      using_columns: [],
    },
    where_clause: {
      class: ExpressionClass.COMPARISON,
      type: ExpressionType.COMPARE_EQUAL,
      alias: '',
      left: {
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        column_names: ['rev_org', 'tier'],
      },
      right: {
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        column_names: ['tier_1'],
      },
    },
    group_expressions: [
      {
        class: ExpressionClass.COLUMN_REF,
        type: ExpressionType.COLUMN_REF,
        alias: '',
        column_names: ['rev_org', 'id'],
      },
    ],
    group_sets: new Set(),
    aggregate_handling: AggregateHandling.STANDARD_HANDLING,
    having: null,
    sample: null,
    qualify: null,
  },
};
