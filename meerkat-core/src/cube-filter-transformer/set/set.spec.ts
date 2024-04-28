import { setTransform } from './set';

describe('setTransform', () => {
    it('should return the correct expression object', () => {
        const query = {
            member: 'table.column'
        };

        const expected = {
            class: 'OPERATOR',
            type: 'OPERATOR_IS_NOT_NULL',
            alias: '',
            children: [
                {
                    class: 'COLUMN_REF',
                    type: 'COLUMN_REF',
                    alias: '',
                    column_names: ['table', 'column']
                }
            ]
        };

        const result = setTransform(query);

        expect(result).toEqual(expected);
    });

    it('should handle __ delimited query', () => {
        const query = {
            member: 'table__column'
        };

        const expected = {
            class: 'OPERATOR',
            type: 'OPERATOR_IS_NOT_NULL',
            alias: '',
            children: [
                {
                    class: 'COLUMN_REF',
                    type: 'COLUMN_REF',
                    alias: '',
                    column_names: ['table__column']
                }
            ]
        };

        const result = setTransform(query);

        expect(result).toEqual(expected);
    });
});
