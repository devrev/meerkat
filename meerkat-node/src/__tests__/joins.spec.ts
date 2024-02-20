import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

export const CREATE_CUTOMERS_TABLE = `
CREATE TABLE customers (
    customer_id VARCHAR,
    customer_name VARCHAR,
    customer_email VARCHAR,
    customer_phone VARCHAR
);
`;

export const INPUT_CUSTOMERS_DATA = `
INSERT INTO customers VALUES
('1', 'John Doe', 'johndoe@gmail.com', '1234567890'),
('2', 'Jane Doe', 'janedoe@gmail.com', '9876543210'),
('3', 'John Smith', 'johnsmith@gmail.com', '1234567892'); 
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
    await duckdbExec(INPUT_CUSTOMERS_DATA);
    await duckdbExec(INPUT_ORDERS_DATA);
    await duckdbExec(INPUT_AUTHORS_DATA);
    await duckdbExec(INPUT_BOOKS_DATA);
    await duckdbExec(INPUT_PRODUCTS_DATA);
  });

  it('Loops in Graph', async () => {
    const query = {
      measures: ['books.total_book_count'],
      filters: [],
      dimensions: ['authors.author_name'],
    };
    await expect(
      cubeQueryToSQL(query, [BOOK_SCHEMA, AUTHOR_SCHEMA])
    ).rejects.toThrow('A loop was detected in the joins.');
  });

  it('Discrete Islands on data graph', async () => {
    const query = {
      measures: ['books.total_book_count'],
      filters: [],
      dimensions: ['customers.customer_id', 'orders.customer_id'],
    };
    await expect(
      cubeQueryToSQL(query, [BOOK_SCHEMA, CUSTOMER_SCHEMA, ORDER_SCHEMA])
    ).rejects.toThrow('Multiple starting nodes found in the graph.');
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
    const sql = await cubeQueryToSQL(query, [
      DEMO_SCHEMA,
      CUSTOMER_SCHEMA,
      PRODUCT_SCHEMA,
    ]);
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
    const sql = await cubeQueryToSQL(query, [
      ORDER_SCHEMA,
      DEMO_SCHEMA,
      PRODUCT_SCHEMA,
    ]);
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

  it('Success Join with filters', async () => {
    const query = {
      measures: ['orders.total_order_amount'],
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
    const sql = await cubeQueryToSQL(query, [ORDER_SCHEMA, CUSTOMER_SCHEMA]);
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
