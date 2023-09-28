import { ParsedExpression } from './ParsedExpression';
import { BaseQueryNode } from './QueryNode';

export enum MacroType {
  VOID_MACRO = 'VOID_MACRO',
  TABLE_MACRO = 'TABLE_MACRO',
  SCALAR_MACRO = 'SCALAR_MACRO',
}

export interface MacroFunction {
  type: MacroType;
  parameters: ParsedExpression[];
  default_parameters: { [key: string]: ParsedExpression };
}

export interface ScalarMacroFunction extends MacroFunction {
  expression: ParsedExpression;
}

export interface TableMacroFunction extends MacroFunction {
  query_node: BaseQueryNode;
}
