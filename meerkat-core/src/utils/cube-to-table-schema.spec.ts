import { convertCubeStringToTableSchema } from './cube-to-table-schema';
describe('cube-to-table-schema', () => {
  it('Should convert to table schema', async () => {
    const files = [
      'cube(`users`, { sql_table: `public.users`, joins: {}, dimensions: { id: { sql: `id`, type: `number`, primary_key: true }, gender: { sql: `gender`, type: `string` }, city: { sql: `city`, type: `string` }, state: { sql: `state`, type: `string` }, first_name: { sql: `first_name`, type: `string` }, company: { sql: `company`, type: `string` }, last_name: { sql: `last_name`, type: `string` }, created_at: { sql: `created_at`, type: `time` } }, measures: { count: { type: `count` } }, pre_aggregations: {} });',
      'cube(`line_items`, { sql_table: `public.line_items`, joins: { orders: { sql: `${CUBE}.order_id = ${orders}.id`, relationship: `many_to_one` }, products: { sql: `${CUBE}.product_id = ${products}.id`, relationship: `many_to_one` } }, dimensions: { id: { sql: `id`, type: `number`, primary_key: true }, created_at: { sql: `created_at`, type: `time` } }, measures: { count: { type: `count` }, price: { sql: `price`, type: `sum` }, quantity: { sql: `quantity`, type: `sum` } }, pre_aggregations: {} });',
    ];
    const outputTableSchemas = [
      {
        name: 'users',
        sql: 'public.users',
        measures: [{ name: 'count', sql: 'COUNT(*)', type: 'number' }],
        dimensions: [
          { name: 'id', sql: 'id', type: 'number' },
          { name: 'gender', sql: 'gender', type: 'string' },
          { name: 'city', sql: 'city', type: 'string' },
          { name: 'state', sql: 'state', type: 'string' },
          { name: 'first_name', sql: 'first_name', type: 'string' },
          { name: 'company', sql: 'company', type: 'string' },
          { name: 'last_name', sql: 'last_name', type: 'string' },
          { name: 'created_at', sql: 'created_at', type: 'time' },
        ],
      },
      {
        name: 'line_items',
        sql: 'public.line_items',
        measures: [
          { name: 'count', sql: 'COUNT(*)', type: 'number' },
          { name: 'price', sql: 'SUM(price)', type: 'number' },
          { name: 'quantity', sql: 'SUM(quantity)', type: 'number' },
        ],
        dimensions: [
          { name: 'id', sql: 'id', type: 'number' },
          { name: 'created_at', sql: 'created_at', type: 'time' },
        ],
        joins: [
          { sql: '{MEERKAT}.order_id = orders.id' },
          { sql: '{MEERKAT}.product_id = products.id' },
        ],
      },
    ];
    for (let i = 0; i < files.length; i++) {
      const tableSchema = convertCubeStringToTableSchema(files[i]);
      expect(tableSchema).toEqual(outputTableSchemas[i]);
    }
  });
});
