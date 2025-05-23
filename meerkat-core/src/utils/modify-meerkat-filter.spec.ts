import { QueryFilter } from '../types/cube-types';
import { modifyLeafMeerkatFilter } from './modify-meerkat-filter';

const ORIGINAL_FILTERS = [{
    and: [
        {
            member: `table.column1`,
            operator: 'equals',
            memberInfo: {
                name: 'column1',
                sql: 'table.country',
                type: 'string',
            },
        },
        {
            or: [
            {
                member: `table.column3`,
                operator: 'equals',
                memberInfo: {
                    name: 'column1',
                    sql: 'table.country',
                    type: 'string',
                },
            },
            {
                member: `table.column2`,
                operator: 'equals',
                memberInfo: {
                    name: 'column2',
                    sql: 'table.country',
                    type: 'string',
                },
            },
            ],
        },
    ],
}];


const EXPECTED_FILTERS = [{
    and: [
        {
            member: `table.column1`,
            operator: 'xyz',
            memberInfo: {
                name: 'column1',
                sql: 'table.country',
                type: 'string',
            },
        },
        {
            or: [
            {
                member: `table.column3`,
                operator: 'equals',
                memberInfo: {
                    name: 'column1',
                    sql: 'table.country',
                    type: 'string',
                },
            },
            {
                member: `table.column2`,
                operator: 'abc',
                memberInfo: {
                    name: 'column2',
                    sql: 'table.country',
                    type: 'string',
                },
            },
            ],
        },
    ],
}];
describe('modifyLeafMeerkatFilter', () => {
    it('should modify the leaf meerkat filter using the provided callback', () => {


        const modifiedFilters = modifyLeafMeerkatFilter(ORIGINAL_FILTERS, (filter: QueryFilter) => {
            if (filter.member === 'table.column1') {
                filter.operator = 'xyz';
            }
            if (filter.member === 'table.column2') {
                filter.operator = 'abc';
            }
            return filter;
        });

        expect(modifiedFilters).toEqual(EXPECTED_FILTERS);
    });
});
