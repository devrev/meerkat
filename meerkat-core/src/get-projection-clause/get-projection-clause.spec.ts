import { TableSchema } from "../types/cube-types";
import { getDimensionProjection, getProjectionClause } from './get-projection-clause';


const TABLE_SCHEMA: TableSchema = {
    dimensions: [{ name: 'a', sql: 'others', type: 'number' }, { name: 'c', sql: 'any', type: 'string' }],
    measures: [],
    name: 'test',
    sql: 'SELECT * from test'
    // Define your table schema here
};
describe("get-projection-clause", () => {
    describe("getDimensionProjection", () => {
        it("should return the member projection when the key exists in the table schema", () => {
            const key = "test.a";
    
            const result = getDimensionProjection({ key, tableSchema: TABLE_SCHEMA });
            expect(result).toEqual({ aliasKey: "test__a", foundMember: {"name": "a", "sql": "others", "type": "number"}, sql: "others AS test__a"});
        });
    
        it("should return the object with undefined values when the key doesn't exist in the table schema", () => {
            const key = "test.a";
            const tableSchema: TableSchema = {
                ...TABLE_SCHEMA,
                dimensions: [{ name: 'b', sql: 'others', type: 'number' }],
            };
    
            const result = getDimensionProjection({ key, tableSchema });
            expect(result).toEqual({ aliasKey: undefined, foundMember: undefined, sql: undefined });
        });
    })

    describe("getProjectionClause", () => {
        it('should return the projection clause when the members are present in the table schema', () => {
            const members = ['test.a', 'test.c'];
            const tableSchema = TABLE_SCHEMA;
            const aliasedColumnSet = new Set<string>();
            const result = getProjectionClause([], members, tableSchema, aliasedColumnSet);
            expect(result).toEqual('others AS test__a, any AS test__c');
        })
        it('should skip aliased items present in already seen', () => {
            const members = ['test.a', 'test.c'];

            const tableSchema = TABLE_SCHEMA;
            const aliasedColumnSet = new Set<string>(['test.c']);
            const result = getProjectionClause([], members, tableSchema, aliasedColumnSet);
            expect(result).toEqual('others AS test__a, ');
        })
    })

});
