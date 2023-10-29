import { duckdbExec } from '../duckdb-exec';

describe('filter-param-tests', () => {
  beforeAll(async () => {
    await duckdbExec(`CREATE TABLE orders(
        id INTEGER PRIMARY KEY,
        date DATE,
        status VARCHAR,
        amount DECIMAL
    );`);
    await duckdbExec(`INSERT INTO orders(id, date, status, amount)
      VALUES (1, DATE '2022-01-21', 'completed', 99.99),
             (2, DATE '2022-02-10', 'pending', 200.50),
             (3, DATE '2022-01-25', 'completed', 150.00),
             (4, DATE '2022-03-01', 'cancelled', 40.00),
             (5, DATE '2022-01-28', 'completed', 80.75),
             (6, DATE '2022-02-15', 'pending', 120.00),
             (7, DATE '2022-04-01', 'completed', 210.00);`);
  });
});
