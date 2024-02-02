import { Query } from '@devrev/meerkat-core';
import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const CREATE_TEST_TABLE = `CREATE TABLE orders (
    order_id INTEGER,
    customer_id VARCHAR,
    product_id VARCHAR,
    order_date DATE,
    order_amount FLOAT
);`

const INPUT_DATA_QUERY = `INSERT INTO orders VALUES
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
`

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

describe('cube-to-sql', () => {
    beforeAll(async () => {
        //Create test table
        await duckdbExec(CREATE_TEST_TABLE);
        //Insert test data
        await duckdbExec(INPUT_DATA_QUERY);
        //Get SQL from cube query
    });

    it.skip('Should apply having/where correctly key in measure and filter', async () => {
        const query: Query = {
            measures: ['orders.total_order_amount'],
            filters: [{
                member: 'orders.total_order_amount',
                operator: 'equals',
                values: ['100']
            },
            {
                member: 'orders.customer_id',
                operator: 'equals',
                values: ['2']
            }],
            dimensions: ['orders.customer_id'],
            order: {
                'orders.total_order_amount': 'desc',
            },
            limit: 2,
        };
        const sql = await cubeQueryToSQL(query, TABLE_SCHEMA);
        console.info(`SQL for Simple Cube Query: `, sql);
        expect(sql).toBe(`SELECT SUM(order_amount) AS orders__total_order_amount ,   orders__customer_id FROM (SELECT *, customer_id AS orders__customer_id, customer_id AS orders__customer_id FROM (select * from orders) AS orders) AS orders WHERE (orders__customer_id = '2') GROUP BY orders__customer_id HAVING (orders__total_order_amount = 100) ORDER BY orders__total_order_amount DESC LIMIT 2`);
        const output = await duckdbExec(sql);
        expect(output).toEqual([{
            "orders__customer_id": "2",
            "orders__total_order_amount": 100,
        }]);
    });

    it.skip('Should apply having/where correctly key in filter and not in measure', async () => {
        const query: Query = {
            measures: ['orders.total_order_amount', ],
            filters: [{
                    member: 'orders.order_amount',
                    operator: 'equals',
                    values: ['100']
                }
            ],
            dimensions: ['orders.customer_id'],
            order: {
                'orders.total_order_amount': 'desc',
            },
            limit: 2,
        };
        const sql = await cubeQueryToSQL(query, TABLE_SCHEMA);
        console.info(`SQL for Simple Cube Query: `, sql);
        expect(sql).toBe(`SELECT SUM(order_amount) AS orders__total_order_amount ,   orders__customer_id FROM (SELECT *, orders.order_amount AS orders__order_amount, customer_id AS orders__customer_id FROM (select * from orders) AS orders) AS orders WHERE (orders__order_amount = 100) GROUP BY orders__customer_id ORDER BY orders__total_order_amount DESC LIMIT 2`);
        const output = await duckdbExec(sql);
        expect(output).toEqual([{
            "orders__customer_id": "3",
            "orders__total_order_amount": 100,
        }]);
    });


    // it('Should apply having/where correctly key in filter and not in measure', async () => {
    //     const query: Query = {
    //         "dimensions": [
    //             "support_insights_ticket_metrics_summary.ticket_prioritized"
    //         ],
    //         "limit": Infinity,
    //         "measures": [
    //             "support_insights_ticket_metrics_summary.count_star"
    //         ],
    //         "timeDimensions": [],
    //         "type": "sql",
    //         "filters": [
    //             {
    //                 "and": [
    //                     {
    //                         "or": [
    //                             {
    //                                 "member": "support_insights_ticket_metrics_summary.ticket_prioritized",
    //                                 "operator": "equals",
    //                                 "values": [
    //                                     "no"
    //                                 ]
    //                             },
    //                             {
    //                                 "member": "support_insights_ticket_metrics_summary.ticket_prioritized",
    //                                 "operator": "equals",
    //                                 "values": [
    //                                     "yes"
    //                                 ]
    //                             }
    //                         ]
    //                     }
    //                 ]
    //             }
    //         ]
    //     };

    //     const TABLE_SCHEMA = {
    //         "dimensions": [
    //             {
    //                 "name": "other_dimension",
    //                 "sql": "'dashboard_others'",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "id",
    //                 "sql": "id",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "severity_name",
    //                 "sql": "severity_name",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "stage_id",
    //                 "sql": "stage_id",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "account_id",
    //                 "sql": "account_id",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "created_date",
    //                 "sql": "created_date",
    //                 "type": "time"
    //             },
    //             {
    //                 "name": "record_date",
    //                 "sql": "record_date",
    //                 "type": "time"
    //             },
    //             {
    //                 "name": "sla_stage",
    //                 "sql": "sla_stage",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "is_conversation_linked",
    //                 "sql": "is_conversation_linked",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "primary_part_id",
    //                 "sql": "primary_part_id",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "source_channel",
    //                 "sql": "source_channel",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "tag_ids",
    //                 "sql": "tag_ids",
    //                 "type": "string_array"
    //             },
    //             {
    //                 "name": "group_id",
    //                 "sql": "group_id",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "record_hour",
    //                 "sql": "record_hour",
    //                 "type": "time"
    //             },
    //             {
    //                 "name": "ticket_prioritized",
    //                 "sql": "CASE WHEN primary_part_id LIKE '%enhancement%' OR is_issue_linked = 'yes' THEN 'yes' ELSE 'no' END",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "primary_part_id",
    //                 "sql": "primary_part_id",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "sla_stage",
    //                 "sql": "sla_stage",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "rev_oid",
    //                 "sql": "rev_oid",
    //                 "type": "string"
    //             },
    //             {
    //                 "name": "owned_by_ids",
    //                 "sql": "owned_by_ids",
    //                 "type": "string_array"
    //             },
    //             {
    //                 "name": "created_by_id",
    //                 "sql": "created_by_id",
    //                 "type": "string"
    //             }
    //         ],
    //         "measures": [
    //             {
    //                 "name": "total_first_resp_breaches",
    //                 "sql": "SUM(COALESCE(total_first_resp_breaches, 0))",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "median_first_resp_time_arr",
    //                 "sql": "MEDIAN(first_resp_time_arr)",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "median_next_resp_time_arr",
    //                 "sql": "MEDIAN(next_resp_time_arr)",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "total_second_resp_breaches",
    //                 "sql": "SUM(COALESCE(total_second_resp_breaches, 0))",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "total_resolution_breaches",
    //                 "sql": "SUM(COALESCE(total_resolution_breaches, 0))",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "count_sla_stage",
    //                 "sql": "COUNT(sla_stage)",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "count_star",
    //                 "sql": "COUNT(DISTINCT id)",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "sum_total_responses",
    //                 "sql": "SUM(total_responses)",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "sum_total_survey_dispatched",
    //                 "sql": "SUM(total_survey_dispatched)",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "average_floor",
    //                 "sql": "FLOOR(SUM(sum_rating) OVER (PARTITION BY id) / SUM(total_responses) OVER (PARTITION BY id))",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "count_over_id_partition",
    //                 "sql": "COUNT(*) OVER (PARTITION BY id)",
    //                 "type": "number"
    //             },
    //             {
    //                 "name": "total_ids_count",
    //                 "sql": "COUNT(DISTINCT id)",
    //                 "type": "number"
    //             }
    //         ],
    //         "name": "support_insights_ticket_metrics_summary",
    //         "sql": "SELECT *, DATE_TRUNC('day', record_hour) AS record_date FROM support_insights_ticket_metrics_summary where state!='closed'"
    //     }
    //     const sql = await cubeQueryToSQL(query, TABLE_SCHEMA);
    //     console.log({sql})
    // });
});


