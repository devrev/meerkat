import { CommonTableExpressionMap, SampleOptions } from './Nodes';
import { ParsedExpression } from './ParsedExpression';
import { ResultModifier } from './ResultModifier';
import { TableRef } from './TableRef';
export declare enum QueryNodeType {
    SELECT_NODE = "SELECT_NODE",
    SET_OPERATION_NODE = "SET_OPERATION_NODE",
    BOUND_SUBQUERY_NODE = "BOUND_SUBQUERY_NODE",
    RECURSIVE_CTE_NODE = "RECURSIVE_CTE_NODE",
    CTE_NODE = "CTE_NODE"
}
export interface BaseQueryNode {
    type: QueryNodeType;
    modifiers: ResultModifier[];
    cte_map: CommonTableExpressionMap;
}
export type QueryNode = SelectNode | SetOperationNode | RecursiveCTENode | CTENode;
export declare enum AggregateHandling {
    STANDARD_HANDLING = "STANDARD_HANDLING",
    NO_AGGREGATES_ALLOWED = "NO_AGGREGATES_ALLOWED",
    FORCE_AGGREGATES = "FORCE_AGGREGATES"
}
export interface SelectNode extends BaseQueryNode {
    type: QueryNodeType.SELECT_NODE;
    select_list: ParsedExpression[];
    from_table?: TableRef;
    where_clause?: ParsedExpression;
    group_expressions: ParsedExpression[];
    group_sets: Set<number>;
    aggregate_handling: AggregateHandling;
    having: ParsedExpression | null;
    sample: SampleOptions | null;
    qualify: ParsedExpression | null;
}
export declare enum SetOperationType {
    NONE = "NONE",
    UNION = "UNION",
    EXCEPT = "EXCEPT",
    INTERSECT = "INTERSECT",
    UNION_BY_NAME = "UNION_BY_NAME"
}
export interface SetOperationNode extends BaseQueryNode {
    type: QueryNodeType.SET_OPERATION_NODE;
    setop_type: SetOperationType;
    left: QueryNode;
    right: QueryNode;
}
export interface RecursiveCTENode extends BaseQueryNode {
    type: QueryNodeType.RECURSIVE_CTE_NODE;
    cte_name: string;
    union_all: boolean;
    left: QueryNode;
    right: QueryNode;
    aliases: string[];
}
export interface CTENode extends BaseQueryNode {
    type: QueryNodeType.CTE_NODE;
    cte_name: string;
    query: QueryNode;
    child: QueryNode;
    aliases: string[];
}
