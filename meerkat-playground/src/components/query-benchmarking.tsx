import { cubeQueryToSQL } from '@devrev/meerkat-browser';
import { useState } from 'react';
import { useDBM } from '../hooks/dbm-context';
import { useClassicEffect } from '../hooks/use-classic-effect';

export const TABLE_SCHEMA: any = {
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

export const QueryBenchmarking = () => {
  const [output, setOuput] = useState<any>();
  const { instanceManager, dbm } = useDBM();

  useClassicEffect(() => {
    (async () => {
      const db = await instanceManager.getDB();
      const connection: any = await db.connect();
      try {
        const query = await cubeQueryToSQL(
          connection,
          {
            measures: ['orders.total_order_amount'],
            filters: [],
            dimensions: ['orders.order_month'],
            limit: 1,
          },
          [TABLE_SCHEMA]
        );
        setOuput(query);
        console.info('Query: ', query);
      } catch (err) {
        console.info('Error: ', err);
        console.error(err);
      }
    })();
  }, []);

  return <div>Query: {output}</div>;
};
