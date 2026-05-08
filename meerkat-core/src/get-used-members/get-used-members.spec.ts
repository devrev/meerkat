import { Query } from '../types/cube-types';
import { getUsedMembers } from './get-used-members';

describe('getUsedMembers', () => {
  it('collects measures and dimensions', () => {
    const query: Query = {
      measures: ['orders.total', 'orders.count'],
      dimensions: ['orders.status', 'customers.id'],
    };

    expect(getUsedMembers(query)).toEqual(
      new Set([
        'orders.total',
        'orders.count',
        'orders.status',
        'customers.id',
      ])
    );
  });

  it('collects order keys', () => {
    const query: Query = {
      measures: [],
      order: { 'orders.created_at': 'desc', 'orders.status': 'asc' },
    };

    expect(getUsedMembers(query)).toEqual(
      new Set(['orders.created_at', 'orders.status'])
    );
  });

  it('collects members from flat filters', () => {
    const query: Query = {
      measures: [],
      filters: [
        { member: 'orders.status', operator: 'equals', values: ['open'] },
      ],
    };

    expect(getUsedMembers(query)).toEqual(new Set(['orders.status']));
  });

  it('collects members from nested and/or filters', () => {
    const query: Query = {
      measures: [],
      filters: [
        {
          and: [
            {
              member: 'orders.status',
              operator: 'equals',
              values: ['open'],
            },
            {
              or: [
                {
                  member: 'customers.id',
                  operator: 'equals',
                  values: ['42'],
                },
                {
                  and: [
                    {
                      member: 'orders.amount',
                      operator: 'gt',
                      values: [100],
                    },
                  ],
                },
              ],
            },
          ],
        },
      ],
    };

    expect(getUsedMembers(query)).toEqual(
      new Set(['orders.status', 'customers.id', 'orders.amount'])
    );
  });

  it('ignores joinPaths entries because they reference table names', () => {
    const query: Query = {
      measures: [],
      joinPaths: [
        [
          {
            left: 'orders',
            right: 'customers',
            on: 'customer_id',
          },
        ],
      ],
    };

    expect(getUsedMembers(query)).toEqual(new Set());
  });

  it('unions measures, dimensions, order, and filter refs', () => {
    const query: Query = {
      measures: ['orders.total'],
      dimensions: ['orders.status'],
      order: { 'orders.created_at': 'desc' },
      filters: [
        {
          and: [
            {
              member: 'customers.id',
              operator: 'equals',
              values: ['42'],
            },
          ],
        },
      ],
    };

    expect(getUsedMembers(query)).toEqual(
      new Set([
        'orders.total',
        'orders.status',
        'orders.created_at',
        'customers.id',
      ])
    );
  });

  it('returns an empty set for an empty query', () => {
    expect(getUsedMembers({ measures: [] })).toEqual(new Set());
  });
});
