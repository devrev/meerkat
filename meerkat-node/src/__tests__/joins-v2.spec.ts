import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';
import {
  AUTHOR_SCHEMA,
  BOOK_SCHEMA,
  CHILD_ITEMS_SCHEMA,
  CREATE_AUTHORS_TABLE,
  CREATE_BOOKS_TABLE,
  CREATE_CHILD_ITEMS_TABLE,
  CREATE_CUTOMERS_TABLE,
  CREATE_ORDERS_TABLE,
  CREATE_PARENT_ITEMS_TABLE,
  CREATE_PRODUCTS_TABLE,
  CUSTOMER_SCHEMA,
  INPUT_AUTHORS_DATA,
  INPUT_BOOKS_DATA,
  INPUT_CHILD_ITEMS_DATA,
  INPUT_CUSTOMERS_DATA,
  INPUT_ORDERS_DATA,
  INPUT_PARENT_ITEMS_DATA,
  INPUT_PRODUCTS_DATA,
  ORDER_SCHEMA,
  PARENT_ITEMS_SCHEMA,
  PRODUCT_SCHEMA,
} from './joins-fixtures';

// Structured-join (v2) variants of the tests in joins.spec.ts. The v2
// path takes its edges from `query.joinPathsV2` — {from, to} descriptors
// — and emits an UNNEST equi-join for array-from columns instead of a
// CONTAINS predicate. Table schemas don't need to carry `joins[]` on the
// v2 path; we reuse the existing schemas unchanged.

const JOIN_TEST_TABLES = [
  'customers',
  'orders',
  'authors',
  'books',
  'products',
  'parent_items',
  'child_items',
];

const dropJoinTestTables = async (): Promise<void> => {
  for (const t of JOIN_TEST_TABLES) {
    await duckdbExec(`DROP TABLE IF EXISTS ${t};`);
  }
};

describe('Joins Tests (v2)', () => {
  beforeAll(async () => {
    await dropJoinTestTables();
    await duckdbExec(CREATE_CUTOMERS_TABLE);
    await duckdbExec(CREATE_ORDERS_TABLE);
    await duckdbExec(CREATE_AUTHORS_TABLE);
    await duckdbExec(CREATE_BOOKS_TABLE);
    await duckdbExec(CREATE_PRODUCTS_TABLE);
    await duckdbExec(CREATE_PARENT_ITEMS_TABLE);
    await duckdbExec(CREATE_CHILD_ITEMS_TABLE);
    await duckdbExec(INPUT_CUSTOMERS_DATA);
    await duckdbExec(INPUT_ORDERS_DATA);
    await duckdbExec(INPUT_AUTHORS_DATA);
    await duckdbExec(INPUT_BOOKS_DATA);
    await duckdbExec(INPUT_PRODUCTS_DATA);
    await duckdbExec(INPUT_PARENT_ITEMS_DATA);
    await duckdbExec(INPUT_CHILD_ITEMS_DATA);
  });

  afterAll(dropJoinTestTables);

  it('Loops in Graph', async () => {
    const query = {
      measures: ['books.total_book_count'],
      filters: [],
      dimensions: ['authors.author_name'],
    };
    await expect(
      cubeQueryToSQL({
        query: query,
        tableSchemas: [BOOK_SCHEMA, AUTHOR_SCHEMA],
      })
    ).rejects.toThrow(
      'Invalid path, multiple data sources are present without a join path.'
    );
  });

  it('Loops in the join paths', async () => {
    // v2 detects loops at graph-construction time: a duplicate
    // edge on the same (from.table, from.column) pair throws
    // "An invalid path was detected." when the second edge
    // (books -> authors on id) tries to register.
    const query = {
      measures: ['books.total_book_count'],
      filters: [],
      joinPathsV2: [
        [
          {
            from: { table: 'authors', column: 'author_id' },
            to: { table: 'books', column: 'id' },
          },
          {
            from: { table: 'books', column: 'id' },
            to: { table: 'authors', column: 'author_id' },
          },
        ],
      ],
      dimensions: ['authors.author_name'],
    };
    await expect(
      cubeQueryToSQL({
        query,
        tableSchemas: [BOOK_SCHEMA, AUTHOR_SCHEMA],
      })
    ).rejects.toThrow(`A loop was detected in the joins.`);
  });

  // v1 has a "Single node in the path" test that exercises a
  // SingleNode ({ left: 'orders', on: 'order_id' }) path entry.
  // That shape has no v2 analog — StructuredJoin requires both
  // `from` and `to`. When consumers want to select from a single
  // table without any join, they simply omit joinPathsV2 (or pass
  // an empty array), which falls back to v1's single-node path.
  it.skip('Single node in the path (no v2 equivalent)', () => {
    // StructuredJoin requires both `from` and `to` — a single-node
    // path has no v2 representation. Consumers that need to query a
    // single table omit `joinPathsV2` entirely and fall back to v1.
  });

  it('Discrete Islands on data graph', async () => {
    const query = {
      measures: ['books.total_book_count', 'authors.total_author_count'],
      joinPathsV2: [
        [
          {
            from: { table: 'authors', column: 'author_id' },
            to: { table: 'books', column: 'id' },
          },
        ],
        [
          {
            from: { table: 'customers', column: 'customer_id' },
            to: { table: 'orders', column: 'id' },
          },
        ],
      ],
      filters: [],
      dimensions: ['customers.customer_id', 'orders.customer_id'],
    };
    await expect(
      cubeQueryToSQL({
        query,
        tableSchemas: [BOOK_SCHEMA, CUSTOMER_SCHEMA, ORDER_SCHEMA, AUTHOR_SCHEMA],
      })
    ).rejects.toThrow(
      'Invalid path, starting node is not the same for all paths.'
    );
  });

  it('Three tables join - Direct', async () => {
    const query = {
      measures: ['orders.total_order_amount'],
      joinPathsV2: [
        [
          {
            from: { table: 'customers', column: 'customer_id' },
            to: { table: 'orders', column: 'customer_id' },
          },
          {
            from: { table: 'orders', column: 'product_id' },
            to: { table: 'products', column: 'product_id' },
          },
        ],
      ],
      filters: [
        {
          and: [
            {
              member: 'orders.order_amount',
              operator: 'gt',
              values: ['40'],
            },
            {
              member: 'customers.customer_name',
              operator: 'contains',
              values: ['Doe'],
            },
          ],
        },
      ],
      dimensions: [
        'products.product_id',
        'orders.product_id',
        'customers.customer_id',
      ],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [ORDER_SCHEMA, CUSTOMER_SCHEMA, PRODUCT_SCHEMA],
    });
    const output = await duckdbExec(sql);
    const parsedOutput = JSON.parse(JSON.stringify(output));
    expect(parsedOutput).toHaveLength(3);
  });

  it('Three tables join - Indirect', async () => {
    const query = {
      measures: ['orders.total_order_amount'],
      joinPathsV2: [
        [
          {
            from: { table: 'customers', column: 'customer_id' },
            to: { table: 'orders', column: 'customer_id' },
          },
        ],
        [
          {
            from: { table: 'customers', column: 'customer_id' },
            to: { table: 'products', column: 'product_id' },
          },
        ],
      ],
      filters: [
        {
          and: [
            {
              member: 'orders.order_amount',
              operator: 'gt',
              values: ['79'],
            },
            {
              member: 'customers.customer_name',
              operator: 'contains',
              values: ['Doe'],
            },
          ],
        },
      ],
      dimensions: [
        'products.product_id',
        'orders.product_id',
        'customers.customer_id',
      ],
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [ORDER_SCHEMA, CUSTOMER_SCHEMA, PRODUCT_SCHEMA],
    });
    const output = await duckdbExec(sql);
    const parsedOutput = JSON.parse(JSON.stringify(output));
    expect(parsedOutput).toHaveLength(1);
    expect(parsedOutput[0].customers__customer_id).toBe('1');
    expect(parsedOutput[0].products__product_id).toBe('1');
    expect(parsedOutput[0].orders__product_id).toBe('2');
    expect(parsedOutput[0].orders__total_order_amount).toBe(80);
  });

  it('Joins with Different Paths', async () => {
    const query1 = {
      measures: ['orders.total_order_amount'],
      joinPathsV2: [
        [
          {
            from: { table: 'customers', column: 'customer_id' },
            to: { table: 'orders', column: 'customer_id' },
          },
        ],
      ],
      filters: [
        {
          and: [
            {
              member: 'orders.order_amount',
              operator: 'gt',
              values: ['40'],
            },
            {
              member: 'customers.customer_name',
              operator: 'contains',
              values: ['Doe'],
            },
          ],
        },
      ],
      dimensions: [
        'orders.product_id',
        'customers.customer_id',
        'customers.order_id',
      ],
      order: {
        'orders.total_order_amount': 'desc',
        'customers.customer_id': 'asc',
      },
    };

    const sql = await cubeQueryToSQL({
      query: query1,
      tableSchemas: [ORDER_SCHEMA, CUSTOMER_SCHEMA],
    });
    const output = await duckdbExec(sql);
    const parsedOutput = JSON.parse(JSON.stringify(output));
    expect(parsedOutput).toHaveLength(3);
    expect(parsedOutput[0].customers__customer_id).toBe('1');
    expect(parsedOutput[1].customers__customer_id).toBe('2');
    expect(parsedOutput[2].customers__customer_id).toBe('1');

    const query2 = {
      measures: ['orders.total_order_amount'],
      joinPathsV2: [
        [
          {
            from: { table: 'customers', column: 'order_id' },
            to: { table: 'orders', column: 'order_id' },
          },
        ],
      ],
      filters: [
        {
          and: [
            {
              member: 'orders.order_amount',
              operator: 'gt',
              values: ['40'],
            },
            {
              member: 'customers.customer_name',
              operator: 'contains',
              values: ['Doe'],
            },
          ],
        },
      ],
      dimensions: [
        'orders.product_id',
        'customers.customer_id',
        'customers.order_id',
      ],
    };

    const sql2 = await cubeQueryToSQL({
      query: query2,
      tableSchemas: [ORDER_SCHEMA, CUSTOMER_SCHEMA],
    });
    const output2 = await duckdbExec(sql2);
    const parsedOutput2 = JSON.parse(JSON.stringify(output2));
    expect(parsedOutput2).toHaveLength(1);
    expect(parsedOutput2[0].customers__customer_id).toBe('2');
    expect(parsedOutput2[0].customers__order_id).toBe('2');
  });

  it('Success Join with filters', async () => {
    const query = {
      measures: ['orders.total_order_amount'],
      joinPathsV2: [
        [
          {
            from: { table: 'customers', column: 'customer_id' },
            to: { table: 'orders', column: 'customer_id' },
          },
        ],
      ],
      filters: [
        {
          and: [
            {
              member: 'orders.order_amount',
              operator: 'gt',
              values: ['40'],
            },
            {
              member: 'customers.customer_name',
              operator: 'contains',
              values: ['Doe'],
            },
          ],
        },
      ],
      dimensions: ['orders.customer_id', 'customers.customer_name'],
      order: {
        'orders.total_order_amount': 'desc',
      },
      limit: 2,
    };
    const sql = await cubeQueryToSQL({
      query,
      tableSchemas: [ORDER_SCHEMA, CUSTOMER_SCHEMA],
    });
    const output = await duckdbExec(sql);
    const parsedOutput = JSON.parse(JSON.stringify(output));
    expect(parsedOutput[0].orders__total_order_amount).toBeGreaterThan(
      parsedOutput[1].orders__total_order_amount
    );
    expect(parsedOutput[0].customers__customer_name).toContain('Doe');
    expect(parsedOutput[1].customers__customer_name).toContain('Doe');
    expect(parsedOutput[0].orders__customer_id).toBe('1');
    expect(parsedOutput[1].orders__customer_id).toBe('2');
  });

  describe('Array Join Tests (UNNEST equi-join)', () => {
    it('Basic array join', async () => {
      const query = {
        measures: ['child_items.total_child_value'],
        joinPathsV2: [
          [
            {
              from: { table: 'parent_items', column: 'child_ids' },
              to: { table: 'child_items', column: 'child_id' },
            },
          ],
        ],
        filters: [],
        dimensions: ['parent_items.parent_name'],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [PARENT_ITEMS_SCHEMA, CHILD_ITEMS_SCHEMA],
      });

      // v2 must emit UNNEST, never CONTAINS.
      expect(sql).toMatch(/UNNEST\(child_ids\)/);
      expect(sql).not.toMatch(/CONTAINS/i);

      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      expect(parsedOutput).toHaveLength(3);
      const parent1 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p.parent_items__parent_name === 'Parent 1'
      );
      const parent2 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p.parent_items__parent_name === 'Parent 2'
      );
      const parent3 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p.parent_items__parent_name === 'Parent 3'
      );
      expect(parent1.child_items__total_child_value).toBe(60);
      expect(parent2.child_items__total_child_value).toBe(60);
      expect(parent3.child_items__total_child_value).toBe(50);
    });

    it('Array join with filters', async () => {
      const query = {
        measures: ['child_items.child_count'],
        joinPathsV2: [
          [
            {
              from: { table: 'parent_items', column: 'child_ids' },
              to: { table: 'child_items', column: 'child_id' },
            },
          ],
        ],
        filters: [
          {
            member: 'child_items.child_value',
            operator: 'gte',
            values: ['30'],
          },
        ],
        dimensions: ['parent_items.parent_name', 'child_items.child_name'],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [PARENT_ITEMS_SCHEMA, CHILD_ITEMS_SCHEMA],
      });
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      expect(parsedOutput).toHaveLength(3);
      const childNames = parsedOutput.map(
        (p: Record<string, unknown>) => p['child_items__child_name']
      );
      expect(childNames).toContain('Child 3');
      expect(childNames).toContain('Child 4');
      expect(childNames).toContain('Child 5');
    });

    it('Array join with ordering', async () => {
      const query = {
        measures: ['child_items.total_child_value'],
        joinPathsV2: [
          [
            {
              from: { table: 'parent_items', column: 'child_ids' },
              to: { table: 'child_items', column: 'child_id' },
            },
          ],
        ],
        filters: [],
        dimensions: ['parent_items.parent_name'],
        order: {
          'child_items.total_child_value': 'asc',
        },
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [PARENT_ITEMS_SCHEMA, CHILD_ITEMS_SCHEMA],
      });
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      expect(parsedOutput).toHaveLength(3);
      expect(parsedOutput[0].child_items__total_child_value).toBe(50);
      expect(parsedOutput[0].parent_items__parent_name).toBe('Parent 3');
    });

    it('Array join - one-to-many: single parent with multiple children', async () => {
      const query = {
        measures: ['child_items.child_count'],
        joinPathsV2: [
          [
            {
              from: { table: 'parent_items', column: 'child_ids' },
              to: { table: 'child_items', column: 'child_id' },
            },
          ],
        ],
        filters: [
          {
            member: 'parent_items.parent_name',
            operator: 'equals',
            values: ['Parent 1'],
          },
        ],
        dimensions: ['parent_items.parent_name', 'child_items.child_name'],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [PARENT_ITEMS_SCHEMA, CHILD_ITEMS_SCHEMA],
      });
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      expect(parsedOutput).toHaveLength(3);
      const childNames = parsedOutput.map(
        (p: Record<string, unknown>) => p['child_items__child_name']
      );
      expect(childNames).toContain('Child 1');
      expect(childNames).toContain('Child 2');
      expect(childNames).toContain('Child 3');
      parsedOutput.forEach((row: Record<string, unknown>) => {
        expect(row['parent_items__parent_name']).toBe('Parent 1');
      });
    });

    it('Array join - many-to-one: shared child appears with multiple parents', async () => {
      const query = {
        measures: ['child_items.child_count'],
        joinPathsV2: [
          [
            {
              from: { table: 'parent_items', column: 'child_ids' },
              to: { table: 'child_items', column: 'child_id' },
            },
          ],
        ],
        filters: [
          {
            member: 'child_items.child_name',
            operator: 'equals',
            values: ['Child 2'],
          },
        ],
        dimensions: ['parent_items.parent_name', 'child_items.child_name'],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [PARENT_ITEMS_SCHEMA, CHILD_ITEMS_SCHEMA],
      });
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      expect(parsedOutput).toHaveLength(2);
      const parentNames = parsedOutput.map(
        (p: Record<string, unknown>) => p['parent_items__parent_name']
      );
      expect(parentNames).toContain('Parent 1');
      expect(parentNames).toContain('Parent 2');
      parsedOutput.forEach((row: Record<string, unknown>) => {
        expect(row['child_items__child_name']).toBe('Child 2');
      });
    });

    it('Array join - many-to-one: count parents per child', async () => {
      const query = {
        measures: ['parent_items.parent_count'],
        joinPathsV2: [
          [
            {
              from: { table: 'parent_items', column: 'child_ids' },
              to: { table: 'child_items', column: 'child_id' },
            },
          ],
        ],
        filters: [],
        dimensions: ['child_items.child_name'],
        order: {
          'parent_items.parent_count': 'desc',
        },
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [PARENT_ITEMS_SCHEMA, CHILD_ITEMS_SCHEMA],
      });
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      expect(parsedOutput).toHaveLength(5);
      const child2 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['child_items__child_name'] === 'Child 2'
      );
      expect(child2.parent_items__parent_count).toBe(2);
      const child5 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['child_items__child_name'] === 'Child 5'
      );
      expect(child5.parent_items__parent_count).toBe(1);
    });

    it('Array join - many-to-many: full relationship mapping', async () => {
      const query = {
        measures: [],
        joinPathsV2: [
          [
            {
              from: { table: 'parent_items', column: 'child_ids' },
              to: { table: 'child_items', column: 'child_id' },
            },
          ],
        ],
        filters: [],
        dimensions: ['parent_items.parent_name', 'child_items.child_name'],
        order: {
          'parent_items.parent_name': 'asc',
          'child_items.child_name': 'asc',
        },
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [PARENT_ITEMS_SCHEMA, CHILD_ITEMS_SCHEMA],
      });
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      expect(parsedOutput).toHaveLength(6);

      const pairs = parsedOutput.map(
        (p: Record<string, unknown>) =>
          `${p['parent_items__parent_name']}-${p['child_items__child_name']}`
      );
      expect(pairs).toContain('Parent 1-Child 1');
      expect(pairs).toContain('Parent 1-Child 2');
      expect(pairs).toContain('Parent 1-Child 3');
      expect(pairs).toContain('Parent 2-Child 2');
      expect(pairs).toContain('Parent 2-Child 4');
      expect(pairs).toContain('Parent 3-Child 5');
    });

    it('Array join - many-to-many: aggregation across relationships', async () => {
      const query = {
        measures: ['child_items.total_child_value', 'child_items.child_count'],
        joinPathsV2: [
          [
            {
              from: { table: 'parent_items', column: 'child_ids' },
              to: { table: 'child_items', column: 'child_id' },
            },
          ],
        ],
        filters: [],
        dimensions: ['parent_items.parent_name'],
        order: {
          'parent_items.parent_name': 'asc',
        },
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [PARENT_ITEMS_SCHEMA, CHILD_ITEMS_SCHEMA],
      });
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      expect(parsedOutput).toHaveLength(3);

      const parent1 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['parent_items__parent_name'] === 'Parent 1'
      );
      expect(parent1.child_items__total_child_value).toBe(60);
      expect(parent1.child_items__child_count).toBe(3);

      const parent2 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['parent_items__parent_name'] === 'Parent 2'
      );
      expect(parent2.child_items__total_child_value).toBe(60);
      expect(parent2.child_items__child_count).toBe(2);

      const parent3 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['parent_items__parent_name'] === 'Parent 3'
      );
      expect(parent3.child_items__total_child_value).toBe(50);
      expect(parent3.child_items__child_count).toBe(1);
    });
  });
});
