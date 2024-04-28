import { CubeToParseExpressionTransform } from "../factory";

// @ts-ignore
export const setTransform: CubeToParseExpressionTransform = (query)  => {
    const { member } = query;
    return {
        class: "OPERATOR",
        type: "OPERATOR_IS_NOT_NULL",
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