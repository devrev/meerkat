import { meerkatPlaceholderReplacer } from './meerkat-placeholder-replacer';

describe("meerkatPlaceholderReplacer", () => {
    it("should not replace placeholders with tableName if placeholder pattern doesnt end in .", () => {
        const sql = "SELECT * FROM {MEERKAT}fieldName";
        const tableName = "customers";
        expect(meerkatPlaceholderReplacer(sql, tableName)).toEqual("SELECT * FROM {MEERKAT}fieldName");
    });

    it("should replace multiple placeholders in the SQL query", () => {
        const sql = "SELECT {MEERKAT}.a, {MEERKAT}.b FROM orders";
        const tableName = "orders";
        expect(meerkatPlaceholderReplacer(sql, tableName)).toEqual('SELECT orders__a, orders__b FROM orders');
    });

    it("should be case sensitive", () => {
        const sql = "SELECT {meerkat}.a FROM orders";
        const tableName = "orders";
        expect(meerkatPlaceholderReplacer(sql, tableName)).toEqual('SELECT {meerkat}.a FROM orders');
    });

    it("should replace the correct match", () => {
        const sql = "SELECT {MEERKAT.{MEERKAT}.}.a FROM customers";
        const tableName = "customers";
        expect(meerkatPlaceholderReplacer(sql, tableName)).toEqual("SELECT {MEERKAT.customers__}.a FROM customers");
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
