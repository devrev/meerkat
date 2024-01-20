import { cubeQueryToSQL } from '../cube-to-sql/cube-to-sql';
import { duckdbExec } from '../duckdb-exec';

const SCHEMA ={
    "dimensions": [
        {
            "name": "other_dimension",
            "sql": "'dashboard_others'",
            "type": "string"
        },
        {
            "name": "primary_part_id",
            "sql": "primary_part_id",
            "type": "string"
        },
        {
            "name": "id",
            "sql": "id",
            "type": "string"
        },
        {
            "name": "ticket_prioritized",
            "sql": "CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END",
            "type": "string"
        },
    ],
    "measures": [
        {
            "name": "count_star",
            "sql": "COUNT(DISTINCT id)",
            "type": "number"
        }
    ],
    "name": "person",
    "sql": "SELECT * FROM person"
};

const QUERY = {
    "dimensions": [
        "person.other_dimension"
    ],
    "measures": [
        "person.count_star"
    ],
    "timeDimensions": [],
    "type": "sql",
    "filters": [
        {
            "and": [
                {
                    "member": "person__ticket_prioritized",
                    "operator": "notEquals",
                    "values": [
                        "no"
                    ]
                }
            ]
        }
    ]
}

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

  it('Should construct the SQL query and apply contains filter', async () => {
    const sql = await cubeQueryToSQL(QUERY, SCHEMA);
    const expectedSQL = `SELECT COUNT(DISTINCT id) AS person__count_star ,   'dashboard_others' AS person__other_dimension FROM (SELECT *, CASE WHEN primary_part_id LIKE '%enhancement%' THEN 'yes' ELSE 'no' END AS person__ticket_prioritized  FROM (SELECT * FROM person) AS person) AS person WHERE ((person__ticket_prioritized != 'no')) GROUP BY person__other_dimension`
    expect(sql).toBe(expectedSQL);
    console.info('SQL: ', sql);
    const output: any = await duckdbExec(sql);
    const expectQueryResult = [{"person__count_star": 2, "person__other_dimension": "dashboard_others"}]
    expect(output).toEqual(expectQueryResult);
  });

});

