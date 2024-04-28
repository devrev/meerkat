import { notSetTransform } from "./not-set";

describe("notSetTransform", () => {
    it("should return the correct expression for a given query", () => {
        const query = {
            member: "table.column"
        };

        const expectedExpression = {
            class: "OPERATOR",
            type: "OPERATOR_IS_NULL",
            alias: "",
            children: [
                {
                    class: "COLUMN_REF",
                    type: "COLUMN_REF",
                    alias: "",
                    column_names: ["table", "column"]
                }
            ]
        };

        const result = notSetTransform(query);

        expect(result).toEqual(expectedExpression);
    });
    it("should return the correct expression for a __ delimited query", () => {
        const query = {
            member: "table__column"
        };

        const expectedExpression = {
            class: "OPERATOR",
            type: "OPERATOR_IS_NULL",
            alias: "",
            children: [
                {
                    class: "COLUMN_REF",
                    type: "COLUMN_REF",
                    alias: "",
                    column_names: ["table__column"]
                }
            ]
        };

        const result = notSetTransform(query);

        expect(result).toEqual(expectedExpression);
    });
});
