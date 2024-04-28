import { ExpressionClass, ExpressionType } from '../../types/duckdb-serialization-types/serialization/Expression';
import { CubeToParseExpressionTransform } from "../factory";

export const notSetTransform: CubeToParseExpressionTransform = (query)  => {
    const { member } = query;
    return {
        class: ExpressionClass.OPERATOR,
        type: ExpressionType.OPERATOR_IS_NULL,
        alias: "",
        children: [
            {
                class: "COLUMN_REF",
                type: "COLUMN_REF",
                alias: "",
                column_names: member.split('.')
            }
        ]
    }
}