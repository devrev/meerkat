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
  name: 'orders',
  sql: 'select * from orders',
  measures: [
    {
      name: 'order_amount',
      sql: 'order_amount',
      type: 'number',
    },
    {
      name: 'total_order_amount',
      sql: 'SUM(order_amount)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'order_date',
      sql: 'order_date',
      type: 'time',
    },
    {
      name: 'order_id',
      sql: 'order_id',
      type: 'number',
    },
    {
      name: 'customer_id',
      sql: 'customer_id',
      type: 'string',
    },
    {
      name: 'product_id',
      sql: 'product_id',
      type: 'string',
    },
    {
      name: 'order_month',
      sql: `DATE_TRUNC('month', order_date)`,
      type: 'string',
    },
  ],
};

export const TEST_DATA = [
  {
    testName: 'GroupBySQLInnerQuery',
    expectedSQL: `SELECT SUM(order_amount) AS orders__total_order_amount ,   DATE_TRUNC('month', order_date) AS orders__order_month FROM (select * from orders) AS orders GROUP BY orders__order_month LIMIT 1`,
    cubeInput: {
      measures: ['orders.total_order_amount'],
      filters: [],
      dimensions: ['orders.order_month'],
      limit: 1,
    },
    expectedOutput: [
      {
        orders__order_month: '2022-01-01T00:00:00.000Z',
        orders__total_order_amount: 130,
      },
    ],
  },
  {
    testName: 'GroupBy',
    expectedSQL: `SELECT SUM(order_amount) AS orders__total_order_amount ,   customer_id AS orders__customer_id FROM (select * from orders) AS orders GROUP BY orders__customer_id`,
    cubeInput: {
      measures: ['orders.total_order_amount'],
      filters: [],
      dimensions: ['orders.customer_id'],
    },
    expectedOutput: [
      {
        orders__customer_id: '1',
        orders__total_order_amount: 130,
      },
      {
        orders__customer_id: '2',
        orders__total_order_amount: 100,
      },
      {
        orders__customer_id: '3',
        orders__total_order_amount: 100,
      },
      {
        orders__customer_id: '4',
        orders__total_order_amount: 135,
      },
      {
        orders__customer_id: '5',
        orders__total_order_amount: 150,
      },
      {
        orders__customer_id: '6',
        orders__total_order_amount: 120,
      },
      {
        orders__customer_id: '6aa6',
        orders__total_order_amount: 0,
      },
    ],
  },
  {
    testName: 'Equals',
    expectedSQL: `SELECT orders.* FROM (SELECT *, orders.customer_id AS orders__customer_id  FROM (select * from orders) AS orders) AS orders WHERE (orders__customer_id = '1')`,
    cubeInput: {
      measures: ['*'],
      filters: [
        {
          member: 'orders__customer_id',
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
        orders__customer_id: '1',
        product_id: '1',
        order_date: '2022-01-01',
        order_amount: 50.0,
      },
      {
        order_id: 2,
        customer_id: '1',
        orders__customer_id: '1',
        product_id: '2',
        order_date: '2022-01-02',
        order_amount: 80.0,
      },
    ],
  },
  {
    testName: 'NotEquals',
    expectedSQL: `SELECT orders.* FROM (SELECT *, orders.customer_id AS orders__customer_id  FROM (select * from orders) AS orders) AS orders WHERE (orders__customer_id != '1')`,
    cubeInput: {
      measures: ['*'],
      filters: [
        {
          member: 'orders__customer_id',
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
        orders__customer_id: '2',
        product_id: '3',
        order_date: '2022-02-01',
        order_amount: 25.0,
      },
      {
        order_id: 4,
        customer_id: '2',
        orders__customer_id: '2',
        product_id: '1',
        order_date: '2022-03-01',
        order_amount: 75.0,
      },
      {
        order_id: 5,
        customer_id: '3',
        orders__customer_id: '3',
        product_id: '1',
        order_date: '2022-03-02',
        order_amount: 100.0,
      },
      {
        order_id: 6,
        customer_id: '4',
        orders__customer_id: '4',
        product_id: '2',
        order_date: '2022-04-01',
        order_amount: 45.0,
      },
      {
        order_id: 7,
        customer_id: '4',
        orders__customer_id: '4',
        product_id: '3',
        order_date: '2022-05-01',
        order_amount: 90.0,
      },
      {
        order_id: 8,
        customer_id: '5',
        orders__customer_id: '5',
        product_id: '1',
        order_date: '2022-05-02',
        order_amount: 65.0,
      },
      {
        order_id: 9,
        customer_id: '5',
        orders__customer_id: '5',
        product_id: '2',
        order_date: '2022-05-05',
        order_amount: 85.0,
      },
      {
        order_id: 10,
        customer_id: '6',
        orders__customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        order_amount: 120.0,
      },
      {
        order_id: 11,
        customer_id: '6aa6',
        orders__customer_id: '6aa6',
        product_id: '3',
        order_date: '2024-06-01',
        order_amount: 0.0,
      },
    ],
  },
  {
    testName: 'Contains',
    expectedSQL: `SELECT orders.* FROM (SELECT *, orders.customer_id AS orders__customer_id  FROM (select * from orders) AS orders) AS orders WHERE (orders__customer_id ~~* '%aa%')`,
    cubeInput: {
      measures: ['*'],
      filters: [
        {
          member: 'orders__customer_id',
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
        orders__customer_id: '6aa6',
        product_id: '3',
        order_date: '2024-06-01',
        order_amount: 0.0,
      },
    ],
  },
  {
    testName: 'NotContains',
    expectedSQL: `SELECT orders.* FROM (SELECT *, orders.customer_id AS orders__customer_id  FROM (select * from orders) AS orders) AS orders WHERE ((orders__customer_id !~~ '%1%') AND (orders__customer_id !~~ '%2%') AND (orders__customer_id !~~ '%3%') AND (orders__customer_id !~~ '%4%') AND (orders__customer_id !~~ '%5%') AND (orders__customer_id !~~ '%aa%'))`,
    cubeInput: {
      measures: ['*'],
      filters: [
        {
          and: [
            {
              member: 'orders__customer_id',
              operator: 'notContains',
              values: ['1'],
            },
            {
              member: 'orders__customer_id',
              operator: 'notContains',
              values: ['2'],
            },
            {
              member: 'orders__customer_id',
              operator: 'notContains',
              values: ['3'],
            },
            {
              member: 'orders__customer_id',
              operator: 'notContains',
              values: ['4'],
            },
            {
              member: 'orders__customer_id',
              operator: 'notContains',
              values: ['5'],
            },
            {
              member: 'orders__customer_id',
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
        orders__customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        order_amount: 120,
      },
    ],
  },
  {
    testName: 'GreaterThan',
    expectedSQL: `SELECT orders.* FROM (SELECT *, orders.order_amount AS orders__order_amount  FROM (select * from orders) AS orders) AS orders WHERE (orders__order_amount > 50)`,
    cubeInput: {
      measures: ['*'],
      filters: [
        {
          member: 'orders__order_amount',
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
        orders__order_amount: 80.0,
      },
      {
        order_id: 4,
        customer_id: '2',
        product_id: '1',
        order_date: '2022-03-01',
        order_amount: 75.0,
        orders__order_amount: 75.0,
      },
      {
        order_id: 5,
        customer_id: '3',
        product_id: '1',
        order_date: '2022-03-02',
        order_amount: 100.0,
        orders__order_amount: 100.0,
      },
      {
        order_id: 7,
        customer_id: '4',
        product_id: '3',
        order_date: '2022-05-01',
        order_amount: 90.0,
        orders__order_amount: 90.0,
      },
      {
        order_id: 8,
        customer_id: '5',
        product_id: '1',
        order_date: '2022-05-02',
        order_amount: 65.0,
        orders__order_amount: 65.0,
      },
      {
        order_id: 9,
        customer_id: '5',
        product_id: '2',
        order_date: '2022-05-05',
        order_amount: 85.0,
        orders__order_amount: 85.0,
      },
      {
        order_id: 10,
        customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        order_amount: 120.0,
        orders__order_amount: 120.0,
      },
    ],
  },
  {
    testName: 'LessThan',
    expectedSQL: `SELECT orders.* FROM (SELECT *, orders.order_amount AS orders__order_amount  FROM (select * from orders) AS orders) AS orders WHERE (orders__order_amount < 50)`,
    cubeInput: {
      measures: ['*'],
      filters: [
        {
          member: 'orders__order_amount',
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
        orders__order_amount: 25.0,
      },
      {
        order_id: 6,
        customer_id: '4',
        product_id: '2',
        order_date: '2022-04-01',
        order_amount: 45.0,
        orders__order_amount: 45.0,
      },
      {
        order_id: 11,
        customer_id: '6aa6',
        product_id: '3',
        order_date: '2024-06-01',
        order_amount: 0.0,
        orders__order_amount: 0.0,
      },
    ],
  },
  {
    testName: 'InDateRange',
    expectedSQL: `SELECT orders.* FROM (SELECT *, orders.order_date AS orders__order_date  FROM (select * from orders) AS orders) AS orders WHERE ((orders__order_date >= '2022-02-01') AND (orders__order_date <= '2022-03-31'))`,
    cubeInput: {
      measures: ['*'],
      filters: [
        {
          member: 'orders__order_date',
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
        orders__order_date: '2022-02-01',
        order_amount: 25.0,
      },
      {
        order_id: 4,
        customer_id: '2',
        product_id: '1',
        order_date: '2022-03-01',
        orders__order_date:'2022-03-01',
        order_amount: 75.0,
      },
      {
        order_id: 5,
        customer_id: '3',
        product_id: '1',
        order_date: '2022-03-02',
        orders__order_date: '2022-03-02',
        order_amount: 100.0,
      },
    ],
  },
  {
    testName: 'NotInDateRange',
    expectedSQL: `SELECT orders.* FROM (SELECT *, orders.order_date AS orders__order_date  FROM (select * from orders) AS orders) AS orders WHERE ((orders__order_date < '2022-02-01') OR (orders__order_date > '2022-03-31'))`,
    cubeInput: {
      measures: ['*'],
      filters: [
        {
          member: 'orders__order_date',
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
        orders__order_date: '2022-01-01',
      },
      {
        order_id: 2,
        customer_id: '1',
        product_id: '2',
        order_date: '2022-01-02',
        order_amount: 80.0,
        orders__order_date: '2022-01-02',
      },
      {
        order_id: 6,
        customer_id: '4',
        product_id: '2',
        order_date: '2022-04-01',
        orders__order_date: '2022-04-01',
        order_amount: 45.0,
      },
      {
        order_id: 7,
        customer_id: '4',
        product_id: '3',
        order_date: '2022-05-01',
        orders__order_date: '2022-05-01',
        order_amount: 90.0,
      },
      {
        order_id: 8,
        customer_id: '5',
        product_id: '1',
        order_date: '2022-05-02',
        orders__order_date: '2022-05-02',
        order_amount: 65.0,
      },
      {
        order_id: 9,
        customer_id: '5',
        product_id: '2',
        order_date: '2022-05-05',
        orders__order_date: '2022-05-05',
        order_amount: 85.0,
      },
      {
        order_id: 10,
        customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        orders__order_date: '2022-06-01',
        order_amount: 120.0,
      },
      {
        order_id: 11,
        customer_id: '6aa6',
        product_id: '3',
        order_date: '2024-06-01',
        orders__order_date: '2024-06-01',
        order_amount: 0.0,
      },
    ],
  },
  // {
  //   testName: 'Or',
  //   expectedSQL: `SELECT orders.* FROM (select * from orders) AS orders WHERE ((orders.order_amount > 80) OR ((orders.order_date >= '2022-02-01') AND (orders.order_date <= '2022-03-01')))`,
  //   cubeInput: {
  //     measures: ['*'],
  //     filters: [
  //       {
  //         or: [
  //           {
  //             member: 'orders.order_amount',
  //             operator: 'gt',
  //             values: ['80'],
  //           },
  //           {
  //             member: 'orders.order_date',
  //             operator: 'inDateRange',
  //             values: ['2022-02-01', '2022-03-01'],
  //           },
  //         ],
  //       },
  //     ],
  //     dimensions: [],
  //   },
  //   expectedOutput: [
  //     {
  //       order_id: 3,
  //       customer_id: '2',
  //       product_id: '3',
  //       order_date: '2022-02-01',
  //       order_amount: 25.0,
  //     },
  //     {
  //       order_id: 4,
  //       customer_id: '2',
  //       product_id: '1',
  //       order_date: '2022-03-01',
  //       order_amount: 75.0,
  //     },
  //     {
  //       order_id: 5,
  //       customer_id: '3',
  //       product_id: '1',
  //       order_date: '2022-03-02',
  //       order_amount: 100.0,
  //     },
  //     {
  //       order_id: 7,
  //       customer_id: '4',
  //       product_id: '3',
  //       order_date: '2022-05-01',
  //       order_amount: 90.0,
  //     },
  //     {
  //       order_id: 9,
  //       customer_id: '5',
  //       product_id: '2',
  //       order_date: '2022-05-05',
  //       order_amount: 85.0,
  //     },
  //     {
  //       order_id: 10,
  //       customer_id: '6',
  //       product_id: '3',
  //       order_date: '2022-06-01',
  //       order_amount: 120.0,
  //     },
  //   ],
  // },
  {
    testName: 'And',
    expectedSQL: `SELECT orders.* FROM (SELECT *, orders.order_amount AS orders__order_amount , orders.order_date AS orders__order_date  FROM (select * from orders) AS orders) AS orders WHERE ((orders__order_amount > 50) AND ((orders__order_date >= '2022-02-01') AND (orders__order_date <= '2022-06-01')))`,
    cubeInput: {
      measures: ['*'],
      filters: [
        {
          and: [
            {
              member: 'orders__order_amount',
              operator: 'gt',
              values: ['50'],
            },
            {
              member: 'orders__order_date',
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
        orders__order_amount: 75.0,
        orders__order_date: '2022-03-01',
      },
      {
        order_id: 5,
        customer_id: '3',
        product_id: '1',
        order_date: '2022-03-02',
        orders__order_date: '2022-03-02',
        order_amount: 100.0,
        orders__order_amount: 100.0,
      },
      {
        order_id: 7,
        customer_id: '4',
        product_id: '3',
        order_date: '2022-05-01',
        orders__order_date: '2022-05-01',
        order_amount: 90.0,
        orders__order_amount: 90.0,
      },
      {
        order_id: 8,
        customer_id: '5',
        product_id: '1',
        order_date: '2022-05-02',
        orders__order_date: '2022-05-02',
        order_amount: 65,
        orders__order_amount: 65,
      },
      {
        order_id: 9,
        customer_id: '5',
        product_id: '2',
        order_date: '2022-05-05',
        orders__order_date: '2022-05-05',
        order_amount: 85.0,
        orders__order_amount: 85.0,
      },
      {
        order_id: 10,
        customer_id: '6',
        product_id: '3',
        order_date: '2022-06-01',
        orders__order_date: '2022-06-01',
        order_amount: 120.0,
        orders__order_amount: 120.0,
      },
    ],
  },
];
