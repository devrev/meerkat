import { getUsedTableSchema } from '../get-used-table-schema/get-used-table-schema';
import { TableSchema } from '../types/cube-types';
import {
  checkLoopInJoinPath,
  createDirectedGraph,
  generateSqlQuery,
  getCombinedTableSchema,
} from './joins';

describe('Table schema functions', () => {
  it('should create a directed graph from the table schema', () => {
    const sqlQueryMap = {
      table1: 'select * from table1',
      table2: 'select * from table2',
      table3: 'select * from table3',
    };
    const tableSchema = [
      {
        name: 'table1',
        sql: 'select * from table1',
        joins: [{ sql: 'table1.id = table2.id' }],
      },
      {
        name: 'table2',
        sql: 'select * from table2',
        joins: [{ sql: 'table2.id = table3.id' }],
      },
      { name: 'table3', sql: 'select * from table3', joins: [] },
    ];
    const directedGraph = createDirectedGraph(tableSchema, sqlQueryMap);

    expect(directedGraph).toEqual({
      table1: { table2: { id: 'table1.id = table2.id' } },
      table2: { table3: { id: 'table2.id = table3.id' } },
    });
  });

  it('should ignore a directed graph edge from the table schema if not present in query map', () => {
    const sqlQueryMap = {
      table1: 'select * from table1',
      table2: 'select * from table2',
    };
    const tableSchema = [
      {
        name: 'table1',
        sql: 'select * from table1',
        joins: [
          { sql: 'table1.id = table2.id' },
          { sql: 'table1.id = table4.id' },
        ],
      },
      {
        name: 'table2',
        sql: 'select * from table2',
        joins: [{ sql: 'table2.id = table3.id' }],
      },
      { name: 'table3', sql: 'select * from table3', joins: [] },
    ];
    const directedGraph = createDirectedGraph(tableSchema, sqlQueryMap);

    expect(directedGraph).toEqual({
      table1: { table2: { id: 'table1.id = table2.id' } },
    });
  });

  it('should correctly generate a SQL query from the provided join path, table schema SQL map, and directed graph', () => {
    const joinPaths = [
      [
        { left: 'table1', right: 'table2', on: 'id' },
        { left: 'table2', right: 'table3', on: 'id' },
      ],
    ];
    const directedGraph = {
      table1: { table2: { id: 'table1.id = table2.id' } },
      table2: { table3: { id: 'table2.id = table3.id' } },
    };
    const tableSchemaSqlMap = {
      table1: 'select * from table1',
      table2: 'select * from table2',
      table3: 'select * from table3',
    };
    const sqlQuery = generateSqlQuery(
      joinPaths,
      tableSchemaSqlMap,
      directedGraph
    );

    expect(sqlQuery).toBe(
      'select * from table1 LEFT JOIN (select * from table2) AS table2  ON table1.id = table2.id LEFT JOIN (select * from table3) AS table3  ON table2.id = table3.id'
    );
  });

  describe('checkLoopInJoinPath', () => {
    it('should return false if there is no loop in the join path', () => {
      const joinPath = [
        [
          { left: 'table1', right: 'table2', on: 'id' },
          { left: 'table2', right: 'table3', on: 'id' },
        ],
      ];
      expect(checkLoopInJoinPath(joinPath)).toBe(false);
    });
    it('should return true if there is a loop in the join path', () => {
      const joinPath = [
        [
          { left: 'table1', right: 'table2', on: 'id' },
          { left: 'table2', right: 'table3', on: 'id' },
          { left: 'table3', right: 'table1', on: 'id' },
        ],
      ];
      expect(checkLoopInJoinPath(joinPath)).toBe(true);
    });
    it('should return false for single node', () => {
      const joinPath = [[{ left: 'table1' }, { left: 'table1' }]];
      expect(checkLoopInJoinPath(joinPath)).toBe(false);
    });
  });
  describe('getCombinedTableSchema', () => {
    it('should return single table schema when only one table is provided', async () => {
      const tableSchema = [
        {
          name: 'orders',
          sql: 'select * from orders',
          measures: [
            { name: 'total_amount', sql: 'SUM(amount)' },
            { name: 'order_count', sql: 'COUNT(*)' },
          ],
          dimensions: [
            { name: 'order_date', sql: 'created_at' },
            { name: 'status', sql: 'order_status' },
          ],
          joins: [],
        },
      ];
      const cubeQuery = {
        joinPaths: [],
      };
      const result = await getCombinedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual(tableSchema[0]);
    });

    it('should combine multiple table schemas correctly', async () => {
      const tableSchema = [
        {
          name: 'orders',
          sql: 'select * from orders',
          measures: [
            { name: 'total_amount', sql: 'SUM(amount)' },
            { name: 'order_count', sql: 'COUNT(*)' },
            { name: 'avg_order_value', sql: 'AVG(amount)' },
          ],
          dimensions: [
            { name: 'order_date', sql: 'created_at' },
            { name: 'status', sql: 'order_status' },
            { name: 'payment_method', sql: 'payment_type' },
          ],
          joins: [{ sql: 'orders.customer_id = customers.id' }],
        },
        {
          name: 'customers',
          sql: 'select * from customers',
          measures: [
            { name: 'customer_count', sql: 'COUNT(DISTINCT id)' },
            { name: 'total_customers', sql: 'COUNT(*)' },
            { name: 'avg_customer_age', sql: 'AVG(age)' },
          ],
          dimensions: [
            { name: 'join_date', sql: 'created_at' },
            { name: 'customer_type', sql: 'type' },
            { name: 'region', sql: 'region' },
          ],
          joins: [],
        },
      ];
      const cubeQuery = {
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
      const result = await getCombinedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual({
        name: 'MEERKAT_GENERATED_TABLE',
        sql: 'select * from orders LEFT JOIN (select * from customers) AS customers  ON orders.customer_id = customers.id',
        measures: [
          { name: 'total_amount', sql: 'SUM(amount)' },
          { name: 'order_count', sql: 'COUNT(*)' },
          { name: 'avg_order_value', sql: 'AVG(amount)' },
          { name: 'customer_count', sql: 'COUNT(DISTINCT id)' },
          { name: 'total_customers', sql: 'COUNT(*)' },
          { name: 'avg_customer_age', sql: 'AVG(age)' },
        ],
        dimensions: [
          { name: 'order_date', sql: 'created_at' },
          { name: 'status', sql: 'order_status' },
          { name: 'payment_method', sql: 'payment_type' },
          { name: 'join_date', sql: 'created_at' },
          { name: 'customer_type', sql: 'type' },
          { name: 'region', sql: 'region' },
        ],
        joins: [],
      });
    });

    it('should throw error when loop is detected in join paths', async () => {
      const tableSchema = [
        {
          name: 'orders',
          sql: 'select * from orders',
          measures: [{ name: 'total_amount', sql: 'SUM(amount)' }],
          dimensions: [{ name: 'order_id', sql: 'id' }],
          joins: [{ sql: 'orders.customer_id = customers.id' }],
        },
        {
          name: 'customers',
          sql: 'select * from customers',
          measures: [{ name: 'customer_count', sql: 'COUNT(*)' }],
          dimensions: [{ name: 'customer_id', sql: 'id' }],
          joins: [{ sql: 'customers.order_id = orders.id' }],
        },
      ];
      const cubeQuery = {
        joinPaths: [
          [
            { left: 'orders', right: 'customers', on: 'id' },
            { left: 'customers', right: 'orders', on: 'id' },
          ],
        ],
      };
      await expect(
        getCombinedTableSchema(tableSchema, cubeQuery)
      ).rejects.toThrow(/A loop was detected in the joins/);
    });

    it('should handle empty measures and dimensions', async () => {
      const tableSchema = [
        {
          name: 'table1',
          sql: 'select * from table1',
          measures: [],
          dimensions: [],
          joins: [{ sql: 'table1.id = table2.id' }],
        },
        {
          name: 'table2',
          sql: 'select * from table2',
          measures: [],
          dimensions: [],
          joins: [],
        },
      ];
      const cubeQuery = {
        joinPaths: [[{ left: 'table1', right: 'table2', on: 'id' }]],
      };
      const result = await getCombinedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual({
        name: 'MEERKAT_GENERATED_TABLE',
        sql: 'select * from table1 LEFT JOIN (select * from table2) AS table2  ON table1.id = table2.id',
        measures: [],
        dimensions: [],
        joins: [],
      });
    });

    it('should filter table schema based on filters, measures, and dimensions', () => {
      const tableSchema: TableSchema[] = [
        {
          name: 'products',
          sql: 'SELECT * FROM products',
          measures: [
            {
              name: 'total_sales',
              sql: 'SUM(sales)',
              type: 'string',
            },
            {
              name: 'inventory_count',
              sql: 'SUM(stock)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'category',
              sql: 'category',
              type: 'string',
            },
            {
              name: 'brand',
              sql: 'brand_name',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'sales',
          sql: 'SELECT * FROM sales',
          measures: [
            {
              name: 'revenue',
              sql: 'SUM(amount)',
              type: 'string',
            },
            {
              name: 'transaction_count',
              sql: 'COUNT(*)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'sale_date',
              sql: 'created_at',
              type: 'string',
            },
            {
              name: 'store_location',
              sql: 'location',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'inventory',
          sql: 'SELECT * FROM inventory',
          measures: [
            {
              name: 'stock_value',
              sql: 'SUM(value)',
              type: 'string',
            },
            {
              name: 'reorder_count',
              sql: 'COUNT(reorder_flag)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'warehouse',
              sql: 'warehouse_id',
              type: 'string',
            },
            {
              name: 'stock_status',
              sql: 'status',
              type: 'string',
            },
          ],
          joins: [],
        },
      ];
      const cubeQuery: Query = {
        measures: ['products.total_sales'],
        dimensions: ['sales.store_location'],
        filters: [
          {
            member: 'inventory.stock_status',
            operator: 'equals',
            values: ['LOW'],
          },
        ],
      };
      const result = getUsedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual(tableSchema);
    });

    it('should filter table schema based on filters, measures, dimensions, order, and joinPaths', () => {
      const tableSchema: TableSchema[] = [
        {
          name: 'users',
          sql: 'SELECT * FROM users',
          measures: [
            {
              name: 'user_count',
              sql: 'COUNT(DISTINCT id)',
              type: 'string',
            },
            {
              name: 'active_users',
              sql: 'SUM(is_active)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'age_group',
              sql: 'age_bracket',
              type: 'string',
            },
            {
              name: 'user_type',
              sql: 'type',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'sessions',
          sql: 'SELECT * FROM sessions',
          measures: [
            {
              name: 'session_duration',
              sql: 'AVG(duration)',
              type: 'string',
            },
            {
              name: 'session_count',
              sql: 'COUNT(*)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'device_type',
              sql: 'device',
              type: 'string',
            },
            {
              name: 'platform',
              sql: 'os',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'events',
          sql: 'SELECT * FROM events',
          measures: [
            {
              name: 'event_count',
              sql: 'COUNT(*)',
              type: 'string',
            },
            {
              name: 'unique_events',
              sql: 'COUNT(DISTINCT event_type)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'event_category',
              sql: 'category',
              type: 'string',
            },
            {
              name: 'event_source',
              sql: 'source',
              type: 'string',
            },
          ],
          joins: [],
        },
        {
          name: 'conversions',
          sql: 'SELECT * FROM conversions',
          measures: [
            {
              name: 'conversion_rate',
              sql: 'AVG(is_converted)',
              type: 'string',
            },
            {
              name: 'total_value',
              sql: 'SUM(value)',
              type: 'string',
            },
          ],
          dimensions: [
            {
              name: 'conversion_type',
              sql: 'type',
              type: 'string',
            },
            {
              name: 'funnel_stage',
              sql: 'stage',
              type: 'string',
            },
          ],
          joins: [],
        },
      ];
      const cubeQuery: Query = {
        measures: ['users.user_count'],
        dimensions: ['sessions.device_type'],
        filters: [
          {
            member: 'events.event_category',
            operator: 'equals',
            values: ['purchase'],
          },
        ],
        order: {
          'conversions.conversion_rate': 'asc',
        },
        joinPaths: [
          [
            {
              left: 'users',
              right: 'sessions',
              on: 'user_id',
            },
          ],
        ],
      };
      const result = getUsedTableSchema(tableSchema, cubeQuery);
      expect(result).toEqual(tableSchema);
    });
  });
});
