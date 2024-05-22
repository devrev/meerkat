import { Dimension, Measure } from '../../types/cube-types/table';
import { CubeToParseExpressionTransform } from "../factory";


import {
    ExpressionClass,
    ExpressionType
} from '../../types/duckdb-serialization-types/serialization/Expression';
import { valueBuilder } from "../base-condition-builder/base-condition-builder";


const inDuckDbCondition = (
    columnName: string,
    values: string[],
    memberInfo: Measure | Dimension
) => {
    const sqlTreeValues = values.map((value) => {
        return {
            class: ExpressionClass.CONSTANT,
            type: ExpressionType.VALUE_CONSTANT,
            alias: "",
            value: valueBuilder(value, memberInfo)
        }
    })
    const columnRef = {
        class: "COLUMN_REF",
        type: "COLUMN_REF",
        alias: "",
        column_names: columnName.split('.')
    }
    console.log('memberInfo', memberInfo)
    switch (memberInfo.type) {
        case 'number_array':
        case 'string_array': {
            return {
                "class": ExpressionClass.FUNCTION,
                "type": ExpressionType.FUNCTION,
                "alias": "",
                "function_name": "&&",
                "schema": "",
                "children": [
                    columnRef,
                    {
                        class: ExpressionClass.OPERATOR,
                        type: ExpressionType.ARRAY_CONSTRUCTOR,
                        alias: "",
                        children: sqlTreeValues,
                    }
                ],
                "filter": null,
                "order_bys": {
                    "type": "ORDER_MODIFIER",
                    "orders": []
                },
                "distinct": false,
                "is_operator": true,
                "export_state": false,
                "catalog": ""
            }
        }
        default: {
            return {
                class: ExpressionClass.OPERATOR,
                type: ExpressionType.COMPARE_IN,
                alias: "",
                children: [
                    columnRef,
                    ...sqlTreeValues
                ]
            }
        }
    }
   
}


export const inTransform: CubeToParseExpressionTransform = (query) => {
  const { member, values, memberInfo } = query;
  if (!values || values.length === 0) {
    throw new Error('Equals filter must have at least one value');
  }

  /**
   * If there is only one value, we can create a simple equals condition
   */
  return inDuckDbCondition(member, values, memberInfo);
}