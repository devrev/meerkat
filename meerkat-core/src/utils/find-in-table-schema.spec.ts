import { TableSchema } from "../types/cube-types";
import { findInSchema } from "./find-in-table-schema";

const measure1 = {
    name: 'column1',
    sql: `table.column1`,
    type: 'number',
}


const dimension1 = {
    name: 'column2',
    sql: `table.column2`,
    type: 'number',
}

const mockTableSchema: TableSchema = {
    name: 'test',
    measures: [measure1],
    dimensions: [dimension1],
    sql: 'SELECT * from test'
};


describe("findInSchema", () => {
    it("should return the matching measure", () => {
        const measureWithoutTable = measure1.name;

        const result = findInSchema(measureWithoutTable, mockTableSchema);

        expect(result).toEqual(measure1);
    });

    it("should return the matching dimension", () => {
        const measureWithoutTable = dimension1.name;

        const result = findInSchema(measureWithoutTable, mockTableSchema);

        expect(result).toEqual(dimension1);
    });

    it("should return the dimension when both measure and dimension match", () => {
        const instertedDimension = {
            ...measure1,
            sql: 'xyz'
        }
        const measureWithoutTable = instertedDimension.name;
        const result = findInSchema(measureWithoutTable, {  ...mockTableSchema, dimensions: [...mockTableSchema.dimensions, instertedDimension]  });

        expect(result).toEqual(instertedDimension);
    });
});
