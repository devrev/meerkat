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

describe('Joins Tests', () => {
  beforeAll(async () => {
    // Ensure a clean slate; the DuckDB singleton is shared across spec
    // files so a previous suite may have left these tables behind.
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
      const query = {
        measures: ['books.total_book_count'],
        filters: [],
        joinPaths: [
          [
            {
              left: 'authors',
              right: 'books',
              on: 'author_id',
            },
            {
              left: 'books',
              right: 'authors',
              on: 'id',
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

    it('Discrete Islands on data graph', async () => {
      BOOK_SCHEMA.joins = [];
      const query = {
        measures: ['books.total_book_count', 'authors.total_author_count'],
        joinPaths: [
          [
            {
              left: 'authors',
              right: 'books',
              on: 'author_id',
            },
          ],
          [
            {
              left: 'customers',
              right: 'orders',
              on: 'customer_id',
            },
          ],
        ],

        filters: [],
        dimensions: ['customers.customer_id', 'orders.customer_id'],
      };
      await expect(
        cubeQueryToSQL({
          query,
          tableSchemas: [
            BOOK_SCHEMA,
            CUSTOMER_SCHEMA,
            ORDER_SCHEMA,
            AUTHOR_SCHEMA,
          ],
        })
      ).rejects.toThrow(
        'Invalid path, starting node is not the same for all paths.'
      );
    });

    it('Single node in the path', async () => {
      const query = {
        measures: [],
        filters: [],
        dimensions: ['orders.order_id'],
        joinPaths: [
          [
            {
              left: 'orders',
              on: 'order_id',
            },
          ],
        ],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [AUTHOR_SCHEMA, ORDER_SCHEMA],
      });
      console.info(`SQL for Simple Cube Query: `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(sql).toEqual(
        'SELECT  orders__order_id FROM (SELECT orders.order_id AS orders__order_id, * FROM (select * from orders) AS orders) AS MEERKAT_GENERATED_TABLE'
      );
      expect(parsedOutput).toHaveLength(11);
  });

  it('Three tables join - Direct', async () => {
    const DEMO_SCHEMA = structuredClone(ORDER_SCHEMA);

      DEMO_SCHEMA.joins = [
        {
          sql: 'products.product_id = orders.product_id',
        },
      ];

      const query = {
        measures: ['orders.total_order_amount'],
        joinPaths: [
          [
            {
              left: 'customers',
              right: 'orders',
              on: 'customer_id',
            },
            {
              left: 'orders',
              right: 'products',
              on: 'product_id',
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
        tableSchemas: [DEMO_SCHEMA, CUSTOMER_SCHEMA, PRODUCT_SCHEMA],
      });
      console.info(`SQL for Simple Cube Query: `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(parsedOutput).toHaveLength(3);
  });

  it('Three tables join - Indirect', async () => {
      const DEMO_SCHEMA = structuredClone(CUSTOMER_SCHEMA);

      DEMO_SCHEMA.joins.push({
        sql: 'products.product_id = customers.customer_id',
      });

      const query = {
        measures: ['orders.total_order_amount'],
        joinPaths: [
          [
            {
              left: 'customers',
              right: 'orders',
              on: 'customer_id',
            },
          ],
          [
            {
              left: 'customers',
              right: 'products',
              on: 'customer_id',
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
        tableSchemas: [ORDER_SCHEMA, DEMO_SCHEMA, PRODUCT_SCHEMA],
      });
      console.info(`SQL for Simple Cube Query: `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(parsedOutput).toHaveLength(1);
      expect(parsedOutput[0].customers__customer_id).toBe('1');
      expect(parsedOutput[0].products__product_id).toBe('1');
      expect(parsedOutput[0].orders__product_id).toBe('2');
      expect(parsedOutput[0].orders__total_order_amount).toBe(80);
  });

  it('Joins with Different Paths', async () => {
      const query1 = {
        measures: ['orders.total_order_amount'],
        joinPaths: [
          [
            {
              left: 'customers',
              right: 'orders',
              on: 'customer_id',
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
      console.info(`SQL for Simple Cube Query: `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(parsedOutput).toHaveLength(3);
      expect(parsedOutput[0].customers__customer_id).toBe('1');
      expect(parsedOutput[1].customers__customer_id).toBe('2');
      expect(parsedOutput[2].customers__customer_id).toBe('1');
      expect(parsedOutput[0].customers__order_id).toBe('3');
      expect(parsedOutput[1].customers__order_id).toBe('2');
      expect(parsedOutput[2].customers__order_id).toBe('3');

      const query2 = {
        measures: ['orders.total_order_amount'],
        joinPaths: [
          [
            {
              left: 'customers',
              right: 'orders',
              on: 'order_id',
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
        joinPaths: [
          [
            {
              left: 'customers',
              right: 'orders',
              on: 'customer_id',
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
      console.info(`SQL for Simple Cube Query: `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(parsedOutput[0].orders__total_order_amount).toBeGreaterThan(
        parsedOutput[1].orders__total_order_amount
      );
      expect(parsedOutput[0].customers__customer_name).toContain('Doe');
      expect(parsedOutput[1].customers__customer_name).toContain('Doe');
      expect(parsedOutput[0].orders__customer_id).toBe('1');
      expect(parsedOutput[1].orders__customer_id).toBe('2');
  });

  describe('CONTAINS Join Tests', () => {
    it('Basic CONTAINS join', async () => {
      const query = {
        measures: ['child_items.total_child_value'],
        joinPaths: [
          [
            {
              left: 'parent_items',
              right: 'child_items',
              on: 'child_ids',
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
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      // Parent 1 has children c1, c2, c3 with values 10, 20, 30 = 60
      // Parent 2 has children c2, c4 with values 20, 40 = 60
      // Parent 3 has child c5 with value 50
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

    it('CONTAINS join with filters', async () => {
      const query = {
        measures: ['child_items.child_count'],
        joinPaths: [
          [
            {
              left: 'parent_items',
              right: 'child_items',
              on: 'child_ids',
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

      // Only children with value >= 30 should be included
      // Parent 1: c3 (30)
      // Parent 2: c4 (40)
      // Parent 3: c5 (50)
      expect(parsedOutput).toHaveLength(3);
      const childNames = parsedOutput.map(
        (p: Record<string, unknown>) => p['child_items__child_name']
      );
      expect(childNames).toContain('Child 3');
      expect(childNames).toContain('Child 4');
      expect(childNames).toContain('Child 5');
    });

    it('CONTAINS join with ordering', async () => {
      const query = {
        measures: ['child_items.total_child_value'],
        joinPaths: [
          [
            {
              left: 'parent_items',
              right: 'child_items',
              on: 'child_ids',
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
      // Should be ordered by total_child_value ascending: 50, 60, 60
      expect(parsedOutput[0].child_items__total_child_value).toBe(50);
      expect(parsedOutput[0].parent_items__parent_name).toBe('Parent 3');
    });

    it('CONTAINS join - one-to-many: single parent with multiple children', async () => {
      // Test that one parent (Parent 1) correctly joins to its multiple children (c1, c2, c3)
      const query = {
        measures: ['child_items.child_count'],
        joinPaths: [
          [
            {
              left: 'parent_items',
              right: 'child_items',
              on: 'child_ids',
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

      // Parent 1 has children c1, c2, c3 - should return 3 rows
      expect(parsedOutput).toHaveLength(3);
      const childNames = parsedOutput.map(
        (p: Record<string, unknown>) => p['child_items__child_name']
      );
      expect(childNames).toContain('Child 1');
      expect(childNames).toContain('Child 2');
      expect(childNames).toContain('Child 3');
      // All rows should be for Parent 1
      parsedOutput.forEach((row: Record<string, unknown>) => {
        expect(row['parent_items__parent_name']).toBe('Parent 1');
      });
    });

    it('CONTAINS join - many-to-one: shared child appears with multiple parents', async () => {
      // Test that a shared child (c2) appears in results for both Parent 1 and Parent 2
      const query = {
        measures: ['child_items.child_count'],
        joinPaths: [
          [
            {
              left: 'parent_items',
              right: 'child_items',
              on: 'child_ids',
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

      // Child 2 (c2) is in both Parent 1 and Parent 2's arrays - should return 2 rows
      expect(parsedOutput).toHaveLength(2);
      const parentNames = parsedOutput.map(
        (p: Record<string, unknown>) => p['parent_items__parent_name']
      );
      expect(parentNames).toContain('Parent 1');
      expect(parentNames).toContain('Parent 2');
      // All rows should be for Child 2
      parsedOutput.forEach((row: Record<string, unknown>) => {
        expect(row['child_items__child_name']).toBe('Child 2');
      });
    });

    it('CONTAINS join - many-to-one: count parents per child', async () => {
      // Count how many parents each child belongs to
      const query = {
        measures: ['parent_items.parent_count'],
        joinPaths: [
          [
            {
              left: 'parent_items',
              right: 'child_items',
              on: 'child_ids',
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
      // Child 2 is shared by Parent 1 and Parent 2, so it should have count 2
      const child2 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['child_items__child_name'] === 'Child 2'
      );
      expect(child2.parent_items__parent_count).toBe(2);
      // Other children belong to only 1 parent
      const child5 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['child_items__child_name'] === 'Child 5'
      );
      expect(child5.parent_items__parent_count).toBe(1);
    });

    it('CONTAINS join - many-to-many: full relationship mapping', async () => {
      // Test the full many-to-many relationship - all parent-child combinations
      // Data setup:
      //   Parent 1 -> [c1, c2, c3] (3 children)
      //   Parent 2 -> [c2, c4]     (2 children, c2 shared with Parent 1)
      //   Parent 3 -> [c5]         (1 child)
      // Total relationships: 6
      const query = {
        measures: [],
        joinPaths: [
          [
            {
              left: 'parent_items',
              right: 'child_items',
              on: 'child_ids',
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

      // Should have 6 total relationships
      expect(parsedOutput).toHaveLength(6);

      // Verify all expected parent-child pairs exist
      const pairs = parsedOutput.map(
        (p: Record<string, unknown>) =>
          `${p['parent_items__parent_name']}-${p['child_items__child_name']}`
      );
      // Parent 1's children
      expect(pairs).toContain('Parent 1-Child 1');
      expect(pairs).toContain('Parent 1-Child 2');
      expect(pairs).toContain('Parent 1-Child 3');
      // Parent 2's children
      expect(pairs).toContain('Parent 2-Child 2'); // c2 is shared
      expect(pairs).toContain('Parent 2-Child 4');
      // Parent 3's children
      expect(pairs).toContain('Parent 3-Child 5');
    });

    it('CONTAINS join - many-to-many: aggregation across relationships', async () => {
      // Aggregate child values per parent in a many-to-many scenario
      // This verifies that shared children (c2) contribute to multiple parents
      const query = {
        measures: ['child_items.total_child_value', 'child_items.child_count'],
        joinPaths: [
          [
            {
              left: 'parent_items',
              right: 'child_items',
              on: 'child_ids',
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

      // Parent 1: c1(10) + c2(20) + c3(30) = 60, count = 3
      const parent1 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['parent_items__parent_name'] === 'Parent 1'
      );
      expect(parent1.child_items__total_child_value).toBe(60);
      expect(parent1.child_items__child_count).toBe(3);

      // Parent 2: c2(20) + c4(40) = 60, count = 2
      // Note: c2 is shared but still contributes its full value to Parent 2
      const parent2 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['parent_items__parent_name'] === 'Parent 2'
      );
      expect(parent2.child_items__total_child_value).toBe(60);
      expect(parent2.child_items__child_count).toBe(2);

      // Parent 3: c5(50) = 50, count = 1
      const parent3 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['parent_items__parent_name'] === 'Parent 3'
      );
      expect(parent3.child_items__total_child_value).toBe(50);
      expect(parent3.child_items__child_count).toBe(1);
    });
  });
});
