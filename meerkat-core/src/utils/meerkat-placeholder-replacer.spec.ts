import { meerkatPlaceholderReplacer } from './meerkat-placeholder-replacer';

describe("meerkatPlaceholderReplacer", () => {
    it("should not replace placeholders with tableName if placeholder pattern doesnt end in .", () => {
        const sql = "SELECT * FROM {tableName1}fieldName";
        const tableName = "customers";
        expect(meerkatPlaceholderReplacer(sql, tableName)).toEqual("SELECT * FROM {tableName1}fieldName");
    });

    it("should replace multiple placeholders in the SQL query", () => {
        const sql = "SELECT {tableName1}.a, {tableName2}.b FROM orders";
        const tableName = "orders";
        expect(meerkatPlaceholderReplacer(sql, tableName)).toEqual('SELECT orders__a, orders__b FROM orders');
    });

    it("should replace the outer most match", () => {
        const sql = "SELECT {tableName1.{tableName1}.}.a FROM customers";
        const tableName = "customers";
        expect(meerkatPlaceholderReplacer(sql, tableName)).toEqual("SELECT customers__}.a FROM customers");
    });

    it("should handle empty SQL queries", () => {
        const sql = "";
        const tableName = "customers";
        expect(meerkatPlaceholderReplacer(sql, tableName)).toEqual('');
    });

    it("should handle SQL queries without placeholders", () => {
        const sql = "SELECT * FROM customers.";
        const tableName = "orders";
        expect(meerkatPlaceholderReplacer(sql, tableName)).toEqual('SELECT * FROM customers.');
    });
});
