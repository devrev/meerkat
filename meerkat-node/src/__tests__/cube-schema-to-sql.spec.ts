import { convertCubeStringToTableSchema } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

export const CREATE_PRODUCTS_TABLE = `
CREATE TABLE products (
    id INTEGER,
    name VARCHAR,
    description VARCHAR,
    created_at TIMESTAMP,
    supplier_id INTEGER
);
`;
export const CREATE_SUPPLIERS_TABLE = `
CREATE TABLE suppliers (
    id INTEGER,
    address VARCHAR,
    email VARCHAR,
    company VARCHAR,
    created_at TIMESTAMP
);
`;

export const INPUT_PRODUCTS_DATA = `
INSERT INTO products VALUES
(1, 'Pencil', 'It is a pencil', '2022-01-01', 1),
(2, 'Pen', 'It is a pen', '2022-01-02', 1),
(3, 'Eraser', 'It is an eraser', '2022-02-01', 2); 
`;

export const INPUT_SUPPLIERS_DATA = `
INSERT INTO suppliers VALUES
(1, '123 Main St', 'john@gmail.com', 'john doe', '2022-01-01'),
(2, '456 Main St', 'steve@gmail.com', 'steve smith', '2022-01-02'); 
`;

describe('Cube Schema to SQL', () => {
  beforeAll(async () => {
    await duckdbExec(CREATE_PRODUCTS_TABLE);
    await duckdbExec(CREATE_SUPPLIERS_TABLE);
    await duckdbExec(INPUT_PRODUCTS_DATA);
    await duckdbExec(INPUT_SUPPLIERS_DATA);
  });

  it('Should be able to execute sql query from cube string', async () => {
      const productsFile =
        "cube('products', { sql_table: 'select * from products', joins: { suppliers: { sql: `products.supplier_id = ${suppliers}.id`, relationship: 'many_to_one' } }, dimensions: { id: { sql: 'id', type: 'number', primary_key: true }, name: { sql: 'name', type: 'string' }, description: { sql: 'description', type: 'string' }, created_at: { sql: 'products.created_at', type: 'time' } }, measures: { count: { type: 'count' } }, pre_aggregations: {} });";
      const suppliersFile =
        "cube('suppliers', { sql_table: 'select * from suppliers', joins: {}, dimensions: { id: { sql: 'id', type: 'number', primary_key: true }, address: { sql: 'address', type: 'string' }, email: { sql: 'email', type: 'string' }, company: { sql: 'company', type: 'string' }, created_at: { sql: 'suppliers.created_at', type: 'time' } }, measures: { count: { type: 'count' } }, pre_aggregations: {} });";
      const productsSchema = convertCubeStringToTableSchema(productsFile);
      const suppliersSchema = convertCubeStringToTableSchema(suppliersFile);
      const query = {
        measures: ['products.count'],
        filters: [],
        dimensions: ['suppliers.created_at', 'products.created_at'],
        joinPaths: [
          [
            {
              left: 'products',
              on: 'supplier_id',
              right: 'suppliers',
            },
          ],
        ],
      };
      const sql = await cubeQueryToSQL({
        query,
        tableSchemas: [productsSchema, suppliersSchema],
      });
      console.info(`SQL for Simple Cube Query: `, sql);
      const output = await duckdbExec(sql);
      console.info('parsedOutput', output);
      expect(sql).toEqual(
        'SELECT COUNT(*) AS products__count ,   suppliers__created_at,  products__created_at FROM (SELECT products.created_at AS products__created_at, * FROM (select * from products) AS products LEFT JOIN (SELECT suppliers.created_at AS suppliers__created_at, * FROM (select * from suppliers) AS suppliers) AS suppliers  ON products.supplier_id = suppliers.id) AS MEERKAT_GENERATED_TABLE GROUP BY suppliers__created_at, products__created_at'
      );
      expect(output).toHaveLength(3);
  });
});
