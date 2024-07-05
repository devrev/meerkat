import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

//account_id,account_name,revu_id,revu_name,dev_oid,COUNT_id,cumulative_COUNT_id,created_date

const ARTICLES_SCHEMA = {
  name: 'articles',
  sql: 'select * from articles',
  measures: [
    {
      name: 'cumulative_COUNT_id',
      sql: 'AVG(cumulative_COUNT_id)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'account_id',
      sql: 'articles.account_id',
      type: 'string',
    },
    {
      name: 'account_name',
      sql: 'account_name',
      type: 'string',
    },
    {
      name: 'revu_id',
      sql: 'revu_id',
      type: 'string',
    },
    {
      name: 'revu_name',
      sql: 'revu_name',
      type: 'string',
    },
    {
      name: 'dev_oid',
      sql: 'dev_oid',
      type: 'string',
    },
    {
      name: 'COUNT_id',
      sql: 'COUNT_id',
      type: 'number',
    },
    {
      name: 'created_date',
      sql: 'created_date',
      type: 'time',
    },
  ],
  joins: [
    {
      sql: 'issues.account_id = articles.account_id',
    },
    {
      sql: 'issues.revu_id = articles.revu_id',
    },
  ],
};

const ISSUES_SCHEMA = {
  name: 'issues',
  sql: 'select * from issues',
  measures: [
    {
      name: 'cumulative_COUNT_id',
      sql: 'AVG(cumulative_COUNT_id)',
      type: 'number',
    },
  ],
  dimensions: [
    {
      name: 'account_id',
      sql: 'issues.account_id',
      type: 'string',
    },
    {
      name: 'account_name',
      sql: 'account_name',
      type: 'string',
    },
    {
      name: 'revu_id',
      sql: 'revu_id',
      type: 'string',
    },
    {
      name: 'revu_name',
      sql: 'revu_name',
      type: 'string',
    },
    {
      name: 'dev_oid',
      sql: 'dev_oid',
      type: 'string',
    },
    {
      name: 'COUNT_id',
      sql: 'COUNT_id',
      type: 'number',
    },
    {
      name: 'created_date',
      sql: 'created_date',
      type: 'time',
    },
  ],
  joins: [
    {
      sql: 'articles.account_id = issues.account_id',
    },
    {
      sql: 'articles.revu_id = issues.revu_id',
    },
  ],
};

const DATASET_EXAMPLE =  {
    "access_level": "internal",
    "columns": [
        {
            "name": "org_ref",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "rev_oid",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "user_ref",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "name",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "hash",
            "sql": {
                "type": "UBIGINT"
            }
        },
        {
            "name": "timestamp_nsecs",
            "sql": {
                "type": "TIMESTAMP"
            }
        },
        {
            "name": "value",
            "sql": {
                "type": "DOUBLE"
            }
        },
        {
            "name": "dev_oid",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "issue_id",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "issue_url",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "log_level",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "log_link",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "log_message",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "message",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "service",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "source",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "stacktrace",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "trace_id",
            "sql": {
                "type": "VARCHAR"
            }
        },
        {
            "name": "type",
            "sql": {
                "type": "VARCHAR"
            }
        }
    ],
    "created_by": {
        "type": "sys_user",
        "display_handle": "DevRev Bot",
        "display_id": "SYSU-1",
        "display_name": "DevRev Bot",
        "email": "system@generated.ai",
        "full_name": "DevRev Bot",
        "id": "don:identity:dvrv-us-1:devo/0:sysu/1",
        "id_v1": "don:DEV-0:sys_user:SYSU-1",
        "state": "active",
        "thumbnail": "https://api.devrev.ai/internal/display-picture/~bot~.png"
    },
    "created_date": "2024-07-03T19:48:24.468Z",
    "dataset_id": "test_anu_dd_alerts_ingestion",
    "dataset_name": "test_anu_dd_alerts_ingestion",
    "description": "Metrics dataset for test_anu_dd_alerts_ingestion",
    "display_id": "DATASET-27f2e501-575d-3d85-9e48-463d7dba594d",
    "id": "don:data:dvrv-us-1:devo/0:dataset/27f2e501-575d-3d85-9e48-463d7dba594d",
    "id_v1": "don:DEV-0:dataset:dataset-27f2e501-575d-3d85-9e48-463d7dba594d",
    "modified_by": {
        "type": "sys_user",
        "display_handle": "DevRev Bot",
        "display_id": "SYSU-1",
        "display_name": "DevRev Bot",
        "email": "system@generated.ai",
        "full_name": "DevRev Bot",
        "id": "don:identity:dvrv-us-1:devo/0:sysu/1",
        "id_v1": "don:DEV-0:sys_user:SYSU-1",
        "state": "active",
        "thumbnail": "https://api.devrev.ai/internal/display-picture/~bot~.png"
    },
    "modified_date": "2024-07-03T19:48:24.468Z",
    "partition_columns": [
        {
            "devrev_field_type": "timestamp",
            "name": "timestamp_nsecs",
            "sql": {
                "type": "TIMESTAMP"
            },
            "timestamp_granularity": "day"
        }
    ],
    "title": "test_anu_dd_alerts_ingestion",
    "version": 1
},

/**
 * 
 * Input 
 *  tables
 *  filters
 *  Group By 
 * 
 * Your task 
 * Construct SCHEMAS - Generate the Schema for the tables from dataset 
 * Construct QUERY - Filters, Measures count, Group by, Join paths
 */

describe('UG Stuff', () => {
  beforeAll(async () => {
    await duckdbExec(`
        CREATE TABLE issues as (
            SELECT * FROM read_csv_auto('${__dirname}/issues.csv')
        )
    `);
    await duckdbExec(`
        CREATE TABLE articles as (
            SELECT * FROM read_csv_auto('${__dirname}/articles.csv')
        )
    `);
  });

  afterAll(async () => {
    await duckdbExec(`DROP TABLE issues`);
    await duckdbExec(`DROP TABLE articles`);
  });

  it('Loops in Graph', async () => {
    const query = {
      measures: ['articles.cumulative_COUNT_id'],
      joinPaths: [
        [
          {
            left: 'articles',
            right: 'issues',
            on: 'account_id',
          },
        ],
      ],
      filters: [
        {
          and: [
            {
              and: [
                {
                  member: 'articles.cumulative_COUNT_id',
                  operator: 'lt',
                  values: ['20'],
                },
                {
                  member: 'articles.cumulative_COUNT_id',
                  operator: 'gt',
                  values: ['10'],
                },
              ],
            },
            {
              member: 'issues.cumulative_COUNT_id',
              operator: 'lt',
              values: ['20'],
            },
          ],
        },
      ],
      dimensions: ['articles.account_id'],
    };
    const sql = await cubeQueryToSQL(query, [ARTICLES_SCHEMA, ISSUES_SCHEMA]);
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    console.info('Output: ', output);
  });
});
