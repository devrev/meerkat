export const CREATE_TEST_TABLE = `
CREATE TABLE orders (
    order_id INTEGER,
    customer_id VARCHAR,
    product_id VARCHAR,
    order_date DATE,
    order_amount FLOAT
);
`;

export const INPUT_DATA_QUERY = `
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

export const TABLE_SCHEMA = {
  cube: 'select * from orders',
  measures: [
    {
      sql: 'orders.order_amount',
      type: 'number',
    },
  ],
  dimensions: [
    {
      sql: 'orders.order_date',
      type: 'time',
    },
    {
      sql: 'orders.order_id',
      type: 'number',
    },
    {
      sql: 'orders.customer_id',
      type: 'string',
    },
    {
      sql: 'orders.product_id',
      type: 'string',
    },
  ],
};

export const TEST_DATA = [
  {
    testName: 'Equals',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE (customer_id = '1')`,
    cubeInput: {
      measures: [],
      filters: [
        {
          member: 'customer_id',
          operator: 'equals',
          values: ['1'],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 1,
        customer_id: '1',
        product_id: '1',
        order_date: '2022-01-01',
        order_amount: 50.0,
      },
      {
        order_id: 2,
        customer_id: '1',
        product_id: '2',
        order_date: '2022-01-02',
        order_amount: 80.0,
      },
    ],
  },
  {
    testName: 'NotEquals',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE (customer_id != '1')`,
    cubeInput: {
      measures: [],
      filters: [
        {
          member: 'customer_id',
          operator: 'notEquals',
          values: ['1'],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 3,
        customer_id: '2',
        product_id: '3',
        order_date: '2022-02-01',
        order_amount: 25.0,
      },
      {
        order_id: 4,
        customer_id: '2',
        product_id: '1',
        order_date: '2022-03-01',
        order_amount: 75.0,
      },
      {
        order_id: 5,
        customer_id: '3',
        product_id: '1',
        order_date: '2022-03-02',
        order_amount: 100.0,
      },
      {
        order_id: 6,
        customer_id: '4',
        product_id: '2',
        order_date: '2022-04-01',
        order_amount: 45.0,
      },
      {
        order_id: 7,
        customer_id: '4',
        product_id: '3',
        order_date: '2022-05-01',
        order_amount: 90.0,
      },
      {
        order_id: 8,
        customer_id: '5',
        product_id: '1',
        order_date: '2022-05-02',
        order_amount: 65.0,
      },
      {
        order_id: 9,
        customer_id: '5',
        product_id: '2',
        order_date: '2022-05-05',
        order_amount: 85.0,
      },
      {
        order_id: 10,
        customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        order_amount: 120.0,
      },
      {
        order_id: 11,
        customer_id: '6aa6',
        product_id: '3',
        order_date: '2024-06-01',
        order_amount: 0.0,
      },
    ],
  },
  {
    testName: 'Contains',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE (customer_id ~~* '%aa%')`,
    cubeInput: {
      measures: [],
      filters: [
        {
          member: 'customer_id',
          operator: 'contains',
          values: ['aa'],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 11,
        customer_id: '6aa6',
        product_id: '3',
        order_date: '2024-06-01',
        order_amount: 0.0,
      },
    ],
  },
  {
    testName: 'NotContains',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE ((customer_id !~~ '%1%') AND (customer_id !~~ '%2%') AND (customer_id !~~ '%3%') AND (customer_id !~~ '%4%') AND (customer_id !~~ '%5%') AND (customer_id !~~ '%aa%'))`,
    cubeInput: {
      measures: [],
      filters: [
        {
          and: [
            {
              member: 'customer_id',
              operator: 'notContains',
              values: ['1'],
            },
            {
              member: 'customer_id',
              operator: 'notContains',
              values: ['2'],
            },
            {
              member: 'customer_id',
              operator: 'notContains',
              values: ['3'],
            },
            {
              member: 'customer_id',
              operator: 'notContains',
              values: ['4'],
            },
            {
              member: 'customer_id',
              operator: 'notContains',
              values: ['5'],
            },
            {
              member: 'customer_id',
              operator: 'notContains',
              values: ['aa'],
            },
          ],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 10,
        customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        order_amount: 120,
      },
    ],
  },
  {
    testName: 'GreaterThan',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE (order_amount > 50)`,
    cubeInput: {
      measures: [],
      filters: [
        {
          member: 'order_amount',
          operator: 'gt',
          values: ['50'],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 2,
        customer_id: '1',
        product_id: '2',
        order_date: '2022-01-02',
        order_amount: 80.0,
      },
      {
        order_id: 4,
        customer_id: '2',
        product_id: '1',
        order_date: '2022-03-01',
        order_amount: 75.0,
      },
      {
        order_id: 5,
        customer_id: '3',
        product_id: '1',
        order_date: '2022-03-02',
        order_amount: 100.0,
      },
      {
        order_id: 7,
        customer_id: '4',
        product_id: '3',
        order_date: '2022-05-01',
        order_amount: 90.0,
      },
      {
        order_id: 8,
        customer_id: '5',
        product_id: '1',
        order_date: '2022-05-02',
        order_amount: 65.0,
      },
      {
        order_id: 9,
        customer_id: '5',
        product_id: '2',
        order_date: '2022-05-05',
        order_amount: 85.0,
      },
      {
        order_id: 10,
        customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        order_amount: 120.0,
      },
    ],
  },
  {
    testName: 'LessThan',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE (order_amount < 50)`,
    cubeInput: {
      measures: [],
      filters: [
        {
          member: 'order_amount',
          operator: 'lt',
          values: ['50'],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 3,
        customer_id: '2',
        product_id: '3',
        order_date: '2022-02-01',
        order_amount: 25.0,
      },
      {
        order_id: 6,
        customer_id: '4',
        product_id: '2',
        order_date: '2022-04-01',
        order_amount: 45.0,
      },
      {
        order_id: 11,
        customer_id: '6aa6',
        product_id: '3',
        order_date: '2024-06-01',
        order_amount: 0.0,
      },
    ],
  },
  {
    testName: 'InDateRange',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE ((order_date >= '2022-02-01') AND (order_date <= '2022-03-31'))`,
    cubeInput: {
      measures: [],
      filters: [
        {
          member: 'order_date',
          operator: 'inDateRange',
          values: ['2022-02-01', '2022-03-31'],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 3,
        customer_id: '2',
        product_id: '3',
        order_date: '2022-02-01',
        order_amount: 25.0,
      },
      {
        order_id: 4,
        customer_id: '2',
        product_id: '1',
        order_date: '2022-03-01',
        order_amount: 75.0,
      },
      {
        order_id: 5,
        customer_id: '3',
        product_id: '1',
        order_date: '2022-03-02',
        order_amount: 100.0,
      },
    ],
  },
  {
    testName: 'NotInDateRange',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE ((order_date < '2022-02-01') OR (order_date > '2022-03-31'))`,
    cubeInput: {
      measures: [],
      filters: [
        {
          member: 'order_date',
          operator: 'notInDateRange',
          values: ['2022-02-01', '2022-03-31'],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 1,
        customer_id: '1',
        product_id: '1',
        order_date: '2022-01-01',
        order_amount: 50.0,
      },
      {
        order_id: 2,
        customer_id: '1',
        product_id: '2',
        order_date: '2022-01-02',
        order_amount: 80.0,
      },
      {
        order_id: 6,
        customer_id: '4',
        product_id: '2',
        order_date: '2022-04-01',
        order_amount: 45.0,
      },
      {
        order_id: 7,
        customer_id: '4',
        product_id: '3',
        order_date: '2022-05-01',
        order_amount: 90.0,
      },
      {
        order_id: 8,
        customer_id: '5',
        product_id: '1',
        order_date: '2022-05-02',
        order_amount: 65.0,
      },
      {
        order_id: 9,
        customer_id: '5',
        product_id: '2',
        order_date: '2022-05-05',
        order_amount: 85.0,
      },
      {
        order_id: 10,
        customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        order_amount: 120.0,
      },
      {
        order_id: 11,
        customer_id: '6aa6',
        product_id: '3',
        order_date: '2024-06-01',
        order_amount: 0.0,
      },
    ],
  },
  {
    testName: 'Or',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE ((order_amount > 80) OR ((order_date >= '2022-02-01') AND (order_date <= '2022-03-01')))`,
    cubeInput: {
      measures: [],
      filters: [
        {
          or: [
            {
              member: 'order_amount',
              operator: 'gt',
              values: ['80'],
            },
            {
              member: 'order_date',
              operator: 'inDateRange',
              values: ['2022-02-01', '2022-03-01'],
            },
          ],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 3,
        customer_id: '2',
        product_id: '3',
        order_date: '2022-02-01',
        order_amount: 25.0,
      },
      {
        order_id: 4,
        customer_id: '2',
        product_id: '1',
        order_date: '2022-03-01',
        order_amount: 75.0,
      },
      {
        order_id: 5,
        customer_id: '3',
        product_id: '1',
        order_date: '2022-03-02',
        order_amount: 100.0,
      },
      {
        order_id: 7,
        customer_id: '4',
        product_id: '3',
        order_date: '2022-05-01',
        order_amount: 90.0,
      },
      {
        order_id: 9,
        customer_id: '5',
        product_id: '2',
        order_date: '2022-05-05',
        order_amount: 85.0,
      },
      {
        order_id: 10,
        customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        order_amount: 120.0,
      },
    ],
  },
  {
    testName: 'And',
    expectedSQL: `SELECT * FROM (select * from orders) WHERE ((order_amount > 50) AND ((order_date >= '2022-02-01') AND (order_date <= '2022-06-01')))`,
    cubeInput: {
      measures: [],
      filters: [
        {
          and: [
            {
              member: 'order_amount',
              operator: 'gt',
              values: ['50'],
            },
            {
              member: 'order_date',
              operator: 'inDateRange',
              values: ['2022-02-01', '2022-06-01'],
            },
          ],
        },
      ],
      dimensions: [],
    },
    expectedOutput: [
      {
        order_id: 4,
        customer_id: '2',
        product_id: '1',
        order_date: '2022-03-01',
        order_amount: 75.0,
      },
      {
        order_id: 5,
        customer_id: '3',
        product_id: '1',
        order_date: '2022-03-02',
        order_amount: 100.0,
      },
      {
        order_id: 7,
        customer_id: '4',
        product_id: '3',
        order_date: '2022-05-01',
        order_amount: 90.0,
      },
      {
        order_id: 8,
        customer_id: '5',
        product_id: '1',
        order_date: '2022-05-02',
        order_amount: 65,
      },
      {
        order_id: 9,
        customer_id: '5',
        product_id: '2',
        order_date: '2022-05-05',
        order_amount: 85.0,
      },
      {
        order_id: 10,
        customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        order_amount: 120.0,
      },
    ],
  },
];
