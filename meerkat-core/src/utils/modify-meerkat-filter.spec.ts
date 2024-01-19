import { QueryFilter } from '../types/cube-types';
import { modifyLeafMeerkatFilter } from './modify-meerkat-filter';

const ORIGINAL_FILTERS = [{
    and: [
        {
            member: `table__column1`,
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
                member: `table__column3`,
                operator: 'equals',
                memberInfo: {
                    name: 'column1',
                    sql: 'table.country',
                    type: 'string',
                },
            },
            {
                member: `table__column2`,
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
            member: `table__column1`,
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
                member: `table__column3`,
                operator: 'equals',
                memberInfo: {
                    name: 'column1',
                    sql: 'table.country',
                    type: 'string',
                },
            },
            {
                member: `table__column2`,
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
            console.log('filter', filter)
            if (filter.member === 'table__column1') {
                filter.operator = 'xyz';
            }
            if (filter.member === 'table__column2') {
                filter.operator = 'abc';
            }
            console.log('filter 2', filter)
            return filter;
        });

        expect(modifiedFilters).toEqual(EXPECTED_FILTERS);
    });
});
