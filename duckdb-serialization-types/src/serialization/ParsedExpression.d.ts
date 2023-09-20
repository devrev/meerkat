import { ExpressionClass, ExpressionType } from './Expression';
import { CacheCheck, Value } from './Misc';
import { LogicalType, OrderByNode } from './Nodes';
import { OrderModifier } from './ResultModifier';
import { SelectStatement } from './Statement';
export interface BaseParsedExpression {
    class: ExpressionClass;
    type: ExpressionType;
    alias: string;
}
export type ParsedExpression = BetweenExpression | CaseExpression | CastExpression | CollateExpression | ColumnRefExpression | ComparisonExpression | ConjunctionExpression | ConstantExpression | DefaultExpression | FunctionExpression | LambdaExpression | OperatorExpression | ParameterExpression | PositionalReferenceExpression | StarExpression | SubqueryExpression | WindowExpression;
export interface BetweenExpression extends BaseParsedExpression {
    input: ParsedExpression;
    lower: ParsedExpression;
    upper: ParsedExpression;
}
export interface CaseExpression extends BaseParsedExpression {
    case_checks: CacheCheck[];
    else_expr: BaseParsedExpression;
}
export interface CastExpression extends BaseParsedExpression {
    child: ParsedExpression;
    cast_type: LogicalType;
    try_cast: boolean;
}
export interface CollateExpression extends BaseParsedExpression {
    child: ParsedExpression;
    collation: string;
}
export interface ColumnRefExpression extends BaseParsedExpression {
    column_names: string[];
}
export interface ComparisonExpression extends BaseParsedExpression {
    left: ParsedExpression;
    right: ParsedExpression;
}
export interface ConjunctionExpression extends BaseParsedExpression {
    children: ParsedExpression[];
}
export interface ConstantExpression extends BaseParsedExpression {
    value: Value;
}
export interface DefaultExpression extends BaseParsedExpression {
}
export interface FunctionExpression extends BaseParsedExpression {
    function_name: string;
    schema: string;
    children: ParsedExpression[];
    filter: ParsedExpression | null;
    order_bys: OrderModifier;
    distinct: boolean;
    is_operator: boolean;
    export_state: boolean;
    catalog: string;
}
export interface LambdaExpression extends BaseParsedExpression {
    lhs: ParsedExpression;
    expr: ParsedExpression;
}
export interface OperatorExpression extends BaseParsedExpression {
    children: ParsedExpression[];
}
export interface ParameterExpression extends BaseParsedExpression {
    identifier: string;
}
export interface PositionalReferenceExpression extends BaseParsedExpression {
    index: number;
}
export interface StarExpression extends BaseParsedExpression {
    relation_name: string;
    exclude_list: Set<string>;
    replace_list: Set<ParsedExpression>;
    columns: boolean;
    expr?: ParsedExpression;
}
export declare enum SubqueryType {
    INVALID = "INVALID",
    SCALAR = "SCALAR",
    EXISTS = "EXISTS",
    NOT_EXISTS = "NOT_EXISTS",
    ANY = "ANY"
}
export interface SubqueryExpression extends BaseParsedExpression {
    subquery_type: SubqueryType;
    subquery: SelectStatement;
    child?: ParsedExpression;
    comparison_type: ExpressionType;
}
export declare enum WindowBoundary {
    INVALID = "INVALID",
    UNBOUNDED_PRECEDING = "UNBOUNDED_PRECEDING",
    UNBOUNDED_FOLLOWING = "UNBOUNDED_FOLLOWING",
    CURRENT_ROW_RANGE = "CURRENT_ROW_RANGE",
    CURRENT_ROW_ROWS = "CURRENT_ROW_ROWS",
    EXPR_PRECEDING_ROWS = "EXPR_PRECEDING_ROWS",
    EXPR_FOLLOWING_ROWS = "EXPR_FOLLOWING_ROWS",
    EXPR_PRECEDING_RANGE = "EXPR_PRECEDING_RANGE",
    EXPR_FOLLOWING_RANGE = "EXPR_FOLLOWING_RANGE"
}
export interface WindowExpression extends BaseParsedExpression {
    function_name: string;
    schema: string;
    catalog: string;
    children: ParsedExpression[];
    partitions: ParsedExpression[];
    orders: OrderByNode[];
    start: WindowBoundary;
    end: WindowBoundary;
    start_expr?: ParsedExpression;
    end_expr?: ParsedExpression;
    offset_expr?: ParsedExpression;
    default_expr?: ParsedExpression;
    ignore_nulls: boolean;
    filter_expr?: ParsedExpression;
}
