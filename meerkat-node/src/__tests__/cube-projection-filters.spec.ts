import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const SCHEMA = {
  dimensions: [
    {
      name: 'other_dimension',
      sql: "'dashboard_others'",
      type: 'string',
    },
    {
      name: 'primary_part_id',
      sql: 'primary_part_id',
      type: 'string',
    },
    {
      name: 'id',
      sql: 'id',
      type: 'string',
    },
    {
      name: 'ticket_prioritized',
      sql: "CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END",
      type: 'string',
    },
  ],
  measures: [
    {
      name: 'count_star',
      sql: 'COUNT(DISTINCT id)',
      type: 'number',
    },
  ],
  name: 'person',
  sql: 'SELECT * FROM person',
};

const QUERY = {
  dimensions: ['person.other_dimension'],
  measures: ['person.count_star'],
  timeDimensions: [],
  type: 'sql',
  filters: [
    {
      and: [
        {
          member: 'person.ticket_prioritized',
          operator: 'notEquals',
          values: ['no'],
        },
      ],
    },
  ],
};

describe('cube-to-sql', () => {
  beforeAll(async () => {
    //Create test table
    await duckdbExec(`
    CREATE TABLE PERSON (
        id VARCHAR,
        primary_part_id VARCHAR,
    );
    `);
    //Insert test data
    await duckdbExec(`
    INSERT INTO person (id, primary_part_id) 
    VALUES 
    ('1', 'enhancement1'),
    ('2', 'product1'),
    ('3', 'feature'),
    ('4', 'enhancement2');
`);
  });

  it('Should construct the SQL query and apply filter on projections', async () => {
    const sql = await cubeQueryToSQL(QUERY, [SCHEMA]);
    const expectedSQL = `SELECT COUNT(DISTINCT id) AS person__count_star ,   person__other_dimension FROM (SELECT *, CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END AS person__ticket_prioritized, 'dashboard_others' AS person__other_dimension FROM (SELECT * FROM person) AS person) AS person WHERE ((person__ticket_prioritized != 'no')) GROUP BY person__other_dimension`;
    expect(sql).toBe(expectedSQL);
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    const expectQueryResult = [
      { person__count_star: BigInt(2), person__other_dimension: 'dashboard_others' },
    ];
    expect(output).toEqual(expectQueryResult);
  });

  it('Should be able to handle multi level filters', async () => {
    const sql = await cubeQueryToSQL(
      {
        ...QUERY,
        filters: [
          {
            and: [
              {
                or: [
                  {
                    member: 'person.id',
                    operator: 'notEquals',
                    values: ['1'],
                  },
                  {
                    member: 'person.ticket_prioritized',
                    operator: 'notEquals',
                    values: ['no'],
                  },
                ],
              },
            ],
          },
        ],
      },
      [SCHEMA]
    );
    const expectedSQL = `SELECT COUNT(DISTINCT id) AS person__count_star ,   person__other_dimension FROM (SELECT *, id AS person__id, CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END AS person__ticket_prioritized, 'dashboard_others' AS person__other_dimension FROM (SELECT * FROM person) AS person) AS person WHERE (((person__id != '1') OR (person__ticket_prioritized != 'no'))) GROUP BY person__other_dimension`;
    expect(sql).toBe(expectedSQL);
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    const expectQueryResult = [
      { person__count_star: BigInt(4), person__other_dimension: 'dashboard_others' },
    ];
    expect(output).toEqual(expectQueryResult);
  });

  it('Should be able to handle same projection on multiple levels in filter', async () => {
    const sql = await cubeQueryToSQL(
      {
        ...QUERY,
        filters: [
          {
            and: [
              {
                member: 'person.id',
                operator: 'notEquals',
                values: ['2'],
              },
              {
                or: [
                  {
                    member: 'person.id',
                    operator: 'notEquals',
                    values: ['1'],
                  },
                  {
                    member: 'person.ticket_prioritized',
                    operator: 'notEquals',
                    values: ['no'],
                  },
                ],
              },
            ],
          },
        ],
      },
      [SCHEMA]
    );
    const expectedSQL = `SELECT COUNT(DISTINCT id) AS person__count_star ,   person__other_dimension FROM (SELECT *, id AS person__id, CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END AS person__ticket_prioritized, 'dashboard_others' AS person__other_dimension FROM (SELECT * FROM person) AS person) AS person WHERE ((person__id != '2') AND ((person__id != '1') OR (person__ticket_prioritized != 'no'))) GROUP BY person__other_dimension`;
    expect(sql).toBe(expectedSQL);
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    const expectQueryResult = [
      { person__count_star: BigInt(3), person__other_dimension: 'dashboard_others' },
    ];
    expect(output).toEqual(expectQueryResult);
  });

  it('Should not create duplicate projections', async () => {
    const TABLE_SCHEMA = [
      {
        name: "dim_issue",
        sql: "select * from system.dim_issue",
        measures: [
          {
            "name": "id_count___function__count",
            "function": {
              "type": "count"
            },
            "sql": "count(dim_issue.id)",
            "type": "string"
          }
        ],
        dimensions: [{
          "modifier": {
              "shouldUnnestGroupBy": true
          },
          "name": "tags_json",
          "sql": "CAST(json_extract_string(dim_issue.tags_json, '$[*].tag_id') AS VARCHAR[])",
          "type": "string_array"
        }, {
          "name": "work_type",
          "sql": "'issue'",
          "type": "string"
        }, {
          "name": "stage_json",
          "sql": "json_extract_string(dim_issue.stage_json, '$.stage_id')",
          "type": "string"
        }, {
          "modifier": {
              "shouldUnnestGroupBy": true
          },
          "name": "owned_by_ids",
          "sql": "dim_issue.owned_by_ids",
          "type": "string_array"
        }]
      }
    ];

    const QUERY = {
      "dimensions": [
            "dim_issue.tags_json"
        ],
        "measures": [
            "dim_issue.id_count___function__count"
        ],
        "type": "sql",
        "filters": [
          {
            "member": "dim_issue.tags_json",
            "operator": "notIn",
            "values": [
                "don:core:dvrv-us-1:devo/787:tag/67241"
            ]
          }
      ]
    }
    const sql = await cubeQueryToSQL(QUERY, TABLE_SCHEMA);
    const expectedSQL = `SELECT count(dim_issue__id) AS dim_issue__id_count___function__count ,   dim_issue__tags_json FROM (SELECT *, array[unnest(CAST(json_extract_string(dim_issue.tags_json, '$[*].tag_id') AS VARCHAR[]))] AS dim_issue__tags_json, dim_issue.id AS dim_issue__id FROM (select * from system.dim_issue) AS dim_issue) AS dim_issue WHERE (NOT (dim_issue__tags_json && (ARRAY['don:core:dvrv-us-1:devo/787:tag/67241']))) GROUP BY dim_issue__tags_json`;
    expect(sql).toBe(expectedSQL);
  });
});
