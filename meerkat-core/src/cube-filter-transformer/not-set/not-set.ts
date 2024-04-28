import { CubeToParseExpressionTransform } from "../factory";

// @ts-ignore
export const notSetTransform: CubeToParseExpressionTransform = (query)  => {
    const { member } = query;
    return {
        class: "OPERATOR",
        type: "OPERATOR_IS_NULL",
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