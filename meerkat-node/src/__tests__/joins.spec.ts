import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

// Tables for CONTAINS join tests
export const CREATE_PARENT_ITEMS_TABLE = `
CREATE TABLE parent_items (
    parent_id VARCHAR,
    parent_name VARCHAR,
    child_ids VARCHAR[]
);
`;

export const INPUT_PARENT_ITEMS_DATA = `
INSERT INTO parent_items VALUES
('p1', 'Parent 1', ['c1', 'c2', 'c3']),
('p2', 'Parent 2', ['c2', 'c4']),
('p3', 'Parent 3', ['c5']);
`;

export const PARENT_ITEMS_SCHEMA = {
  name: 'parent_items',
  sql: 'select * from parent_items',
  measures: [
    {
      name: 'parent_count',
      sql: 'CAST(COUNT(parent_items.parent_id) AS FLOAT)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'parent_id',
      sql: 'parent_items.parent_id',
      type: 'string',
    },
    {
      name: 'parent_name',
      sql: 'parent_items.parent_name',
      type: 'string',
    },
    {
      name: 'child_ids',
      sql: 'parent_items.child_ids',
      type: 'string_array',
    },
  ],
  joins: [
    {
      sql: 'CONTAINS(parent_items.child_ids, child_items.child_id)',
    },
  ],
};

export const CREATE_CHILD_ITEMS_TABLE = `
CREATE TABLE child_items (
    child_id VARCHAR,
    child_name VARCHAR,
    child_value FLOAT
);
`;

export const INPUT_CHILD_ITEMS_DATA = `
INSERT INTO child_items VALUES
('c1', 'Child 1', 10),
('c2', 'Child 2', 20),
('c3', 'Child 3', 30),
('c4', 'Child 4', 40),
('c5', 'Child 5', 50);
`;

export const CHILD_ITEMS_SCHEMA = {
  name: 'child_items',
  sql: 'select * from child_items',
  measures: [
    {
      name: 'child_count',
      sql: 'CAST(COUNT(child_items.child_id) AS FLOAT)',
      type: 'number',
    },
    {
      name: 'total_child_value',
      sql: 'SUM(child_items.child_value)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'child_id',
      sql: 'child_items.child_id',
      type: 'string',
    },
    {
      name: 'child_name',
      sql: 'child_items.child_name',
      type: 'string',
    },
    {
      name: 'child_value',
      sql: 'child_items.child_value',
      type: 'number',
    },
  ],
  joins: [],
};

export const CREATE_CUTOMERS_TABLE = `
CREATE TABLE customers (
    customer_id VARCHAR,
    order_id VARCHAR,
    customer_name VARCHAR,
    customer_email VARCHAR,
    customer_phone VARCHAR
);
`;

export const INPUT_CUSTOMERS_DATA = `
INSERT INTO customers VALUES
('1', '3', 'John Doe', 'johndoe@gmail.com', '1234567890'),
('2', '2', 'Jane Doe', 'janedoe@gmail.com', '9876543210'),
('3', '1', 'John Smith', 'johnsmith@gmail.com', '1234567892');
`;

export const CUSTOMER_SCHEMA = {
  name: 'customers',
  sql: 'select * from customers',
  measures: [
    {
      name: 'total_customer_count',
      sql: 'COUNT({MEERKAT}.customer_id)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'customer_id',
      sql: 'customers.customer_id',
      type: 'number',
    },
    {
      name: 'order_id',
      sql: 'customers.order_id',
      type: 'number',
    },
    {
      name: 'customer_name',
      sql: 'customers.customer_name',
      type: 'string',
    },
    {
      name: 'customer_email',
      sql: 'customers.customer_email',
      type: 'string',
    },
    {
      name: 'customer_phone',
      sql: 'customers.customer_phone',
      type: 'string',
    },
  ],
  joins: [
    {
      sql: 'orders.customer_id = customers.customer_id',
    },
    {
      sql: 'orders.order_id = customers.order_id',
    },
  ],
};

export const CREATE_PRODUCTS_TABLE = `
CREATE TABLE products (
    product_id VARCHAR,
    product_name VARCHAR,
    product_category VARCHAR
);
`;

export const INPUT_PRODUCTS_DATA = `
INSERT INTO products VALUES
('1', 'Product 1', 'Category 1'),
('2', 'Product 2', 'Category 1'),
('3', 'Product 3', 'Category 2');
`;

export const PRODUCT_SCHEMA = {
  name: 'products',
  sql: 'select * from products',
  measures: [
    {
      name: 'total_product_count',
      sql: 'COUNT({MEERKAT}.product_id)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'product_id',
      sql: 'products.product_id',
      type: 'number',
    },
    {
      name: 'product_name',
      sql: 'products.product_name',
      type: 'string',
    },
    {
      name: 'product_category',
      sql: 'products.product_category',
      type: 'string',
    },
  ],
};

export const CREATE_ORDERS_TABLE = `
CREATE TABLE orders (
    order_id INTEGER,
    customer_id VARCHAR,
    product_id VARCHAR,
    order_date DATE,
    order_amount FLOAT
);
`;

export const INPUT_ORDERS_DATA = `
INSERT INTO orders VALUES
(1, '1', '1', '2022-01-01', 50),
(2, '1', '2', '2022-01-02', 80),
(3, '2', '3', '2022-02-01', 25),
(4, '2', '1', '2022-03-01', 75),
(5, '3', '1', '2022-03-02', 100),
(6, '4', '2', '2022-04-01', 45),
(7, '4', '3', '2022-05-01', 90),
(8, '5', '1', '2022-05-02', 65),
(9, '5', '2', '2022-05-05', 85),
(10, '6', '3', '2022-06-01', 120),
(11, '6aa6', '3', '2024-06-01', 0);
`;

export const ORDER_SCHEMA = {
  name: 'orders',
  sql: 'select * from orders',
  measures: [
    {
      name: 'order_amount',
      sql: 'orders.order_amount',
      type: 'number',
    },
    {
      name: 'total_order_amount',
      sql: 'SUM(orders.order_amount)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'order_amount',
      sql: 'orders.order_amount',
      type: 'number',
    },
    {
      name: 'order_date',
      sql: 'orders.order_date',
      type: 'time',
    },
    {
      name: 'order_id',
      sql: 'orders.order_id',
      type: 'number',
    },
    {
      name: 'customer_id',
      sql: 'orders.customer_id',
      type: 'string',
    },
    {
      name: 'product_id',
      sql: 'orders.product_id',
      type: 'string',
    },
    {
      name: 'order_month',
      sql: `DATE_TRUNC('month', orders.order_date)`,
      type: 'string',
    },
  ],
};

export const CREATE_AUTHORS_TABLE = `
CREATE TABLE authors (
    author_id INTEGER,
    author_name VARCHAR
);
`;

export const INPUT_AUTHORS_DATA = `
INSERT INTO authors VALUES
(1, 'John Doe'),
(2, 'Jane Doe'),
(3, 'John Smith');
`;

export const AUTHOR_SCHEMA = {
  name: 'authors',
  sql: 'select * from authors',
  measures: [
    {
      name: 'total_author_count',
      sql: 'COUNT(author_id)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'author_id',
      sql: 'author_id',
      type: 'number',
    },
    {
      name: 'author_name',
      sql: 'author_name',
      type: 'string',
    },
  ],

  joins: [
    {
      sql: 'books.author_id = authors.author_id',
    },
  ],
};

export const CREATE_BOOKS_TABLE = `
CREATE TABLE books (
    book_id INTEGER,
    book_name VARCHAR,
    author_id INTEGER
);
`;

export const INPUT_BOOKS_DATA = `
INSERT INTO books VALUES
(1, 'Book 1', 1),
(2, 'Book 2', 1),
(3, 'Book 3', 2),
(4, 'Book 4', 2),
(5, 'Book 5', 3),
(6, 'Book 6', 3);
`;

export const BOOK_SCHEMA = {
  name: 'books',
  sql: 'select * from books',
  measures: [
    {
      name: 'total_book_count',
      sql: 'COUNT(book_id)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'book_id',
      sql: 'book_id',
      type: 'number',
    },
    {
      name: 'book_name',
      sql: 'book_name',
      type: 'string',
    },
    {
      name: 'author_id',
      sql: 'author_id',
      type: 'number',
    },
  ],
  joins: [
    {
      sql: 'books.author_id = authors.author_id',
    },
  ],
};

describe('Joins Tests', () => {
  beforeAll(async () => {
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

  describe('useDotNotation: false (default)', () => {
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
          options: { useDotNotation: false },
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
          options: { useDotNotation: false },
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
          options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
  });

  describe('useDotNotation: true', () => {
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
          options: { useDotNotation: true },
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
          options: { useDotNotation: true },
        })
      ).rejects.toThrow(`A loop was detected in the joins.`);
    });

    it('Discrete Islands on data graph', async () => {
      const BOOK_SCHEMA_COPY = structuredClone(BOOK_SCHEMA);
      BOOK_SCHEMA_COPY.joins = [];
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
            BOOK_SCHEMA_COPY,
            CUSTOMER_SCHEMA,
            ORDER_SCHEMA,
            AUTHOR_SCHEMA,
          ],
          options: { useDotNotation: true },
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
        options: { useDotNotation: true },
      });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(sql).toEqual(
        'SELECT  "orders.order_id" FROM (SELECT orders.order_id AS "orders.order_id", * FROM (select * from orders) AS orders) AS MEERKAT_GENERATED_TABLE'
      );
      expect(parsedOutput).toHaveLength(11);
    });

    it('Three tables join - Direct with dot notation', async () => {
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
        options: { useDotNotation: true },
      });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(parsedOutput).toHaveLength(3);
      // Verify dot notation keys - use array notation to check literal keys with dots
      expect(parsedOutput[0]).toHaveProperty(['orders.total_order_amount']);
      expect(parsedOutput[0]).toHaveProperty(['products.product_id']);
      expect(parsedOutput[0]).toHaveProperty(['customers.customer_id']);
    });

    it('Three tables join - Indirect with dot notation', async () => {
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
        options: { useDotNotation: true },
      });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(parsedOutput).toHaveLength(1);
      expect(parsedOutput[0]['customers.customer_id']).toBe('1');
      expect(parsedOutput[0]['products.product_id']).toBe('1');
      expect(parsedOutput[0]['orders.product_id']).toBe('2');
      expect(parsedOutput[0]['orders.total_order_amount']).toBe(80);
    });

    it('Joins with Different Paths with dot notation', async () => {
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
        options: { useDotNotation: true },
      });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(parsedOutput).toHaveLength(3);
      expect(parsedOutput[0]['customers.customer_id']).toBe('1');
      expect(parsedOutput[1]['customers.customer_id']).toBe('2');
      expect(parsedOutput[2]['customers.customer_id']).toBe('1');
      expect(parsedOutput[0]['customers.order_id']).toBe('3');
      expect(parsedOutput[1]['customers.order_id']).toBe('2');
      expect(parsedOutput[2]['customers.order_id']).toBe('3');

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
        options: { useDotNotation: true },
      });
      const output2 = await duckdbExec(sql2);
      const parsedOutput2 = JSON.parse(JSON.stringify(output2));
      expect(parsedOutput2).toHaveLength(1);
      expect(parsedOutput2[0]['customers.customer_id']).toBe('2');
      expect(parsedOutput2[0]['customers.order_id']).toBe('2');
    });

    it('Success Join with filters and dot notation', async () => {
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
        options: { useDotNotation: true },
      });
      console.info(`SQL for Simple Cube Query (dot notation): `, sql);
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));
      console.info('parsedOutput', parsedOutput);
      expect(parsedOutput[0]['orders.total_order_amount']).toBeGreaterThan(
        parsedOutput[1]['orders.total_order_amount']
      );
      expect(parsedOutput[0]['customers.customer_name']).toContain('Doe');
      expect(parsedOutput[1]['customers.customer_name']).toContain('Doe');
      expect(parsedOutput[0]['orders.customer_id']).toBe('1');
      expect(parsedOutput[1]['orders.customer_id']).toBe('2');
    });
  });

  describe('CONTAINS Join Tests', () => {
    it('Basic CONTAINS join - useDotNotation: false', async () => {
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
        options: { useDotNotation: false },
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

    it('Basic CONTAINS join - useDotNotation: true', async () => {
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
        options: { useDotNotation: true },
      });
      const output = await duckdbExec(sql);
      const parsedOutput = JSON.parse(JSON.stringify(output));

      expect(parsedOutput).toHaveLength(3);
      const parent1 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['parent_items.parent_name'] === 'Parent 1'
      );
      const parent2 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['parent_items.parent_name'] === 'Parent 2'
      );
      const parent3 = parsedOutput.find(
        (p: Record<string, unknown>) =>
          p['parent_items.parent_name'] === 'Parent 3'
      );
      expect(parent1['child_items.total_child_value']).toBe(60);
      expect(parent2['child_items.total_child_value']).toBe(60);
      expect(parent3['child_items.total_child_value']).toBe(50);
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
        options: { useDotNotation: false },
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
