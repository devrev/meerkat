// Shared fixtures — schemas, CREATE TABLE statements, and seed data
// used by both joins.spec.ts (legacy) and joins-v2.spec.ts (v2).

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

