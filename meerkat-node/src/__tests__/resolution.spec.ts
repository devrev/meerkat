import { cubeQueryToSQLWithResolution } from '../cube-to-sql-with-resolution/cube-to-sql-with-resolution';

export const BASE_TABLE_SCHEMA = {
  name: 'base_table',
  sql: 'select * from base_table',
  measures: [
    {
      name: 'count',
      sql: 'count(*)',
      type: 'number' as const,
    },
  ],
  dimensions: [
    {
      name: 'part_id_1',
      sql: 'base_table.part_id_1',
      type: 'string' as const,
    },
    {
      name: 'random_column',
      sql: 'base_table.random_column',
      type: 'string' as const,
    },
    {
      name: 'work_id',
      sql: 'base_table.work_id',
      type: 'string' as const,
    },
    {
      name: 'part_id_2',
      sql: 'base_table.part_id_2',
      type: 'string' as const,
    },
  ],
};

export const DIM_WORK_SCHEMA = {
  name: 'dim_work',
  sql: 'select id, display_id, title from system.dim_issue',
  measures: [],
  dimensions: [
    {
      name: 'id',
      sql: 'dim_work.id',
      type: 'string' as const,
    },
    {
      name: 'display_id',
      sql: 'dim_work.display_id',
      type: 'string' as const,
    },
    {
      name: 'title',
      sql: 'dim_work.title',
      type: 'string' as const,
    },
  ],
};

export const DIM_PART_SCHEMA = {
  name: 'dim_part',
  sql: 'select id, display_id from system.dim_feature UNION ALL select id, display_id from system.dim_product',
  measures: [],
  dimensions: [
    {
      name: 'id',
      sql: 'dim_part.id',
      type: 'string' as const,
    },
    {
      name: 'display_id',
      sql: 'dim_part.display_id',
      type: 'string' as const,
    },
  ],
};

export const BASE_TABLE_SCHEMA_WITH_ALIASES = {
  name: 'base_table',
  sql: 'select * from base_table',
  measures: [
    {
      name: 'count',
      sql: 'count(*)',
      type: 'number' as const,
      alias: 'Count',
    },
  ],
  dimensions: [
    {
      name: 'part_id_1',
      sql: 'base_table.part_id_1',
      type: 'string' as const,
      alias: 'Part ID 1',
    },
    {
      name: 'random_column',
      sql: 'base_table.random_column',
      type: 'string' as const,
      alias: 'Random Column',
    },
    {
      name: 'part_id_2',
      sql: 'base_table.part_id_2',
      type: 'string' as const,
      alias: 'Part ID 2',
    },
    {
      name: 'work_id',
      sql: 'base_table.work_id',
      type: 'string' as const,
      alias: 'Work ID',
    },
  ],
};

export const DIM_PART_SCHEMA_WITH_ALIASES = {
  name: 'dim_part',
  sql: 'select id, display_id from system.dim_feature UNION ALL select id, display_id from system.dim_product',
  measures: [],
  dimensions: [
    {
      name: 'id',
      sql: 'dim_part.id',
      type: 'string' as const,
      alias: 'ID',
    },
    {
      name: 'display_id',
      sql: 'dim_part.display_id',
      type: 'string' as const,
      alias: 'Display ID',
    },
  ],
};

export const DIM_WORK_SCHEMA_WITH_ALIASES = {
  name: 'dim_work',
  sql: 'select id, display_id, title from system.dim_issue',
  measures: [],
  dimensions: [
    {
      name: 'id',
      sql: 'dim_work.id',
      type: 'string' as const,
      alias: 'ID',
    },
    {
      name: 'display_id',
      sql: 'dim_work.display_id',
      type: 'string' as const,
      alias: 'Display ID',
    },
    {
      name: 'title',
      sql: 'dim_work.title',
      type: 'string' as const,
      alias: 'Title',
    },
  ],
};

// Helper to normalize SQL for comparison (collapse whitespace)
const normalizeSQL = (sql: string) => sql.replace(/\s+/g, ' ').trim();

describe('Resolution Tests (useDotNotation: false)', () => {
  const options = { useDotNotation: false };

  it('No Resolution Config', async () => {
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.work_id',
        'base_table.part_id_2',
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA],
      resolutionConfig: {
        columnConfigs: [],
        tableSchemas: [],
      },
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) 
      from (SELECT "__row_id", "base_table__part_id_1", "base_table__random_column", "base_table__work_id", "base_table__part_id_2" 
        FROM 
          (SELECT __base_query."__row_id" AS "__row_id", 
                  __base_query."base_table__part_id_1" AS "base_table__part_id_1", 
                  __base_query."base_table__random_column" AS "base_table__random_column", 
                  __base_query."base_table__work_id" AS "base_table__work_id", 
                  __base_query."base_table__part_id_2" AS "base_table__part_id_2", * 
          FROM (SELECT MAX(__base_query."base_table__part_id_1") AS "base_table__part_id_1" , 
                   MAX(__base_query."base_table__random_column") AS "base_table__random_column" , 
                   MAX(__base_query."base_table__work_id") AS "base_table__work_id" , 
                   MAX(__base_query."base_table__part_id_2") AS "base_table__part_id_2" , 
                   "__row_id" FROM 
            (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__part_id_1", "base_table__random_column", "base_table__work_id", "base_table__part_id_2", "__row_id" FROM (SELECT __base_query."base_table__part_id_1" AS "base_table__part_id_1", __base_query."base_table__random_column" AS "base_table__random_column", __base_query."base_table__work_id" AS "base_table__work_id", __base_query."base_table__part_id_2" AS "base_table__part_id_2", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__part_id_1", "base_table__random_column", "base_table__work_id", "base_table__part_id_2", "__row_id" FROM (SELECT __base_query.base_table__part_id_1 AS "base_table__part_id_1", __base_query.base_table__random_column AS "base_table__random_column", __base_query.base_table__work_id AS "base_table__work_id", __base_query.base_table__part_id_2 AS "base_table__part_id_2", row_number() OVER () AS "__row_id", * FROM (SELECT base_table__part_id_1, base_table__random_column, base_table__work_id, base_table__part_id_2 FROM (SELECT base_table.part_id_1 AS base_table__part_id_1, base_table.random_column AS base_table__random_column, base_table.work_id AS base_table__work_id, base_table.part_id_2 AS base_table__part_id_2, * FROM (select * from base_table) AS base_table) AS base_table) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) 
      order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });

  it('Resolution Config Missing Table Schema', async () => {
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.work_id',
        'base_table.part_id_2',
      ],
    };

    await expect(
      cubeQueryToSQLWithResolution({
        query,
        tableSchemas: [BASE_TABLE_SCHEMA],
        resolutionConfig: {
          columnConfigs: [
            {
              name: 'base_table.part_id_1',
              type: 'string' as const,
              source: 'dim_part',
              joinColumn: 'id',
              resolutionColumns: ['display_id'],
            },
          ],
          tableSchemas: [],
        },
        options,
      })
    ).rejects.toThrow('Table schema not found for dim_part');
  });

  it('With Resolution Config', async () => {
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.work_id',
        'base_table.part_id_2',
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA_WITH_ALIASES],
      resolutionConfig: {
        columnConfigs: [
          {
            name: 'base_table.part_id_1',
            type: 'string' as const,
            source: 'dim_part',
            joinColumn: 'id',
            resolutionColumns: ['display_id'],
          },
          {
            name: 'base_table.work_id',
            type: 'string' as const,
            source: 'dim_work',
            joinColumn: 'id',
            resolutionColumns: ['display_id', 'title'],
          },
          {
            name: 'base_table.part_id_2',
            type: 'string' as const,
            source: 'dim_part',
            joinColumn: 'id',
            resolutionColumns: ['display_id'],
          },
        ],
        tableSchemas: [
          DIM_PART_SCHEMA_WITH_ALIASES,
          DIM_WORK_SCHEMA_WITH_ALIASES,
        ],
      },
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) 
        from (SELECT "__row_id", 
                 "Part ID 1", 
                 "Random Column", 
                 "Work ID - Display ID", 
                 "Work ID - Title", 
                 "Part ID 2" 
          FROM (SELECT __base_query."__row_id" AS "__row_id", 
                   __base_query."base_table__part_id_1__display_id" AS "Part ID 1", 
                   __base_query."base_table__random_column" AS "Random Column", 
                   __base_query."base_table__work_id__display_id" AS "Work ID - Display ID", 
                   __base_query."base_table__work_id__title" AS "Work ID - Title", 
                   __base_query."base_table__part_id_2__display_id" AS "Part ID 2", * 
            FROM (SELECT MAX(__base_query."base_table__part_id_1__display_id") AS "base_table__part_id_1__display_id" , 
                     MAX(__base_query."base_table__random_column") AS "base_table__random_column" , 
                     MAX(__base_query."base_table__work_id__display_id") AS "base_table__work_id__display_id" , 
                     MAX(__base_query."base_table__work_id__title") AS "base_table__work_id__title" , 
                     MAX(__base_query."base_table__part_id_2__display_id") AS "base_table__part_id_2__display_id" , 
                     "__row_id" 
              FROM (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__part_id_1__display_id", "base_table__random_column", "base_table__work_id__display_id", "base_table__work_id__title", "base_table__part_id_2__display_id", "__row_id" FROM (SELECT __base_query."base_table__random_column" AS "base_table__random_column", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__part_id_1", "base_table__random_column", "base_table__work_id", "base_table__part_id_2", "__row_id" FROM (SELECT __base_query.base_table__part_id_1 AS "base_table__part_id_1", __base_query.base_table__random_column AS "base_table__random_column", __base_query.base_table__work_id AS "base_table__work_id", __base_query.base_table__part_id_2 AS "base_table__part_id_2", row_number() OVER () AS "__row_id", * FROM (SELECT base_table__part_id_1, base_table__random_column, base_table__work_id, base_table__part_id_2 FROM (SELECT base_table.part_id_1 AS base_table__part_id_1, base_table.random_column AS base_table__random_column, base_table.work_id AS base_table__work_id, base_table.part_id_2 AS base_table__part_id_2, * FROM (select * from base_table) AS base_table) AS base_table) AS __base_query) AS __base_query) AS __base_query LEFT JOIN (SELECT base_table__part_id_1.display_id AS "base_table__part_id_1__display_id", * FROM (select id, display_id from system.dim_feature UNION ALL select id, display_id from system.dim_product) AS base_table__part_id_1) AS base_table__part_id_1 ON __base_query.base_table__part_id_1 = base_table__part_id_1.id LEFT JOIN (SELECT base_table__work_id.display_id AS "base_table__work_id__display_id", base_table__work_id.title AS "base_table__work_id__title", * FROM (select id, display_id, title from system.dim_issue) AS base_table__work_id) AS base_table__work_id ON __base_query.base_table__work_id = base_table__work_id.id LEFT JOIN (SELECT base_table__part_id_2.display_id AS "base_table__part_id_2__display_id", * FROM (select id, display_id from system.dim_feature UNION ALL select id, display_id from system.dim_product) AS base_table__part_id_2) AS base_table__part_id_2 ON __base_query.base_table__part_id_2 = base_table__part_id_2.id) AS MEERKAT_GENERATED_TABLE) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });

  it('Resolution With Measures', async () => {
    const query = {
      measures: ['base_table.count'],
      dimensions: ['base_table.part_id_1'],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA_WITH_ALIASES],
      resolutionConfig: {
        columnConfigs: [
          {
            name: 'base_table.part_id_1',
            type: 'string' as const,
            source: 'dim_part',
            joinColumn: 'id',
            resolutionColumns: ['display_id'],
          },
        ],
        tableSchemas: [DIM_PART_SCHEMA_WITH_ALIASES],
      },
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) from (SELECT "__row_id", "Part ID 1", "Count" FROM (SELECT __base_query."__row_id" AS "__row_id", __base_query."base_table__part_id_1__display_id" AS "Part ID 1", __base_query."base_table__count" AS "Count", * FROM (SELECT MAX(__base_query."base_table__part_id_1__display_id") AS "base_table__part_id_1__display_id" , MAX(__base_query."base_table__count") AS "base_table__count" , "__row_id" FROM (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__part_id_1__display_id", "base_table__count", "__row_id" FROM (SELECT __base_query."base_table__count" AS "base_table__count", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__part_id_1", "base_table__count", "__row_id" FROM (SELECT __base_query.base_table__part_id_1 AS "base_table__part_id_1", __base_query.base_table__count AS "base_table__count", row_number() OVER () AS "__row_id", * FROM (SELECT count(*) AS base_table__count , base_table__part_id_1 FROM (SELECT base_table.part_id_1 AS base_table__part_id_1, * FROM (select * from base_table) AS base_table) AS base_table GROUP BY base_table__part_id_1) AS __base_query) AS __base_query) AS __base_query LEFT JOIN (SELECT base_table__part_id_1.display_id AS "base_table__part_id_1__display_id", * FROM (select id, display_id from system.dim_feature UNION ALL select id, display_id from system.dim_product) AS base_table__part_id_1) AS base_table__part_id_1 ON __base_query.base_table__part_id_1 = base_table__part_id_1.id) AS MEERKAT_GENERATED_TABLE) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });

  it('Resolution With Aliases', async () => {
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.part_id_2',
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA_WITH_ALIASES],
      resolutionConfig: {
        columnConfigs: [
          {
            name: 'base_table.part_id_1',
            type: 'string' as const,
            source: 'dim_part',
            joinColumn: 'id',
            resolutionColumns: ['display_id'],
          },
          {
            name: 'base_table.part_id_2',
            type: 'string' as const,
            source: 'dim_part',
            joinColumn: 'id',
            resolutionColumns: ['display_id'],
          },
        ],
        tableSchemas: [DIM_PART_SCHEMA_WITH_ALIASES],
      },
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) from (SELECT "__row_id", "Part ID 1", "Random Column", "Part ID 2" FROM (SELECT __base_query."__row_id" AS "__row_id", __base_query."base_table__part_id_1__display_id" AS "Part ID 1", __base_query."base_table__random_column" AS "Random Column", __base_query."base_table__part_id_2__display_id" AS "Part ID 2", * FROM (SELECT MAX(__base_query."base_table__part_id_1__display_id") AS "base_table__part_id_1__display_id" , MAX(__base_query."base_table__random_column") AS "base_table__random_column" , MAX(__base_query."base_table__part_id_2__display_id") AS "base_table__part_id_2__display_id" , "__row_id" FROM (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__part_id_1__display_id", "base_table__random_column", "base_table__part_id_2__display_id", "__row_id" FROM (SELECT __base_query."base_table__random_column" AS "base_table__random_column", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__part_id_1", "base_table__random_column", "base_table__part_id_2", "__row_id" FROM (SELECT __base_query.base_table__part_id_1 AS "base_table__part_id_1", __base_query.base_table__random_column AS "base_table__random_column", __base_query.base_table__part_id_2 AS "base_table__part_id_2", row_number() OVER () AS "__row_id", * FROM (SELECT base_table__part_id_1, base_table__random_column, base_table__part_id_2 FROM (SELECT base_table.part_id_1 AS base_table__part_id_1, base_table.random_column AS base_table__random_column, base_table.part_id_2 AS base_table__part_id_2, * FROM (select * from base_table) AS base_table) AS base_table) AS __base_query) AS __base_query) AS __base_query LEFT JOIN (SELECT base_table__part_id_1.display_id AS "base_table__part_id_1__display_id", * FROM (select id, display_id from system.dim_feature UNION ALL select id, display_id from system.dim_product) AS base_table__part_id_1) AS base_table__part_id_1 ON __base_query.base_table__part_id_1 = base_table__part_id_1.id LEFT JOIN (SELECT base_table__part_id_2.display_id AS "base_table__part_id_2__display_id", * FROM (select id, display_id from system.dim_feature UNION ALL select id, display_id from system.dim_product) AS base_table__part_id_2) AS base_table__part_id_2 ON __base_query.base_table__part_id_2 = base_table__part_id_2.id) AS MEERKAT_GENERATED_TABLE) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });

  it('Resolution With Column Projections', async () => {
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.work_id',
        'base_table.part_id_2',
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA_WITH_ALIASES],
      resolutionConfig: {
        columnConfigs: [
          {
            name: 'base_table.part_id_1',
            type: 'string' as const,
            source: 'dim_part',
            joinColumn: 'id',
            resolutionColumns: ['display_id'],
          },
        ],
        tableSchemas: [DIM_PART_SCHEMA_WITH_ALIASES],
      },
      columnProjections: ['base_table.random_column', 'base_table.part_id_1'],
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) from (SELECT "__row_id", "Random Column", "Part ID 1" FROM (SELECT __base_query."__row_id" AS "__row_id", __base_query."base_table__random_column" AS "Random Column", __base_query."base_table__part_id_1__display_id" AS "Part ID 1", * FROM (SELECT MAX(__base_query."base_table__random_column") AS "base_table__random_column" , MAX(__base_query."base_table__part_id_1__display_id") AS "base_table__part_id_1__display_id" , "__row_id" FROM (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__random_column", "base_table__part_id_1__display_id", "__row_id" FROM (SELECT __base_query."base_table__random_column" AS "base_table__random_column", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table__random_column", "base_table__part_id_1", "__row_id" FROM (SELECT __base_query.base_table__random_column AS "base_table__random_column", __base_query.base_table__part_id_1 AS "base_table__part_id_1", row_number() OVER () AS "__row_id", * FROM (SELECT base_table__part_id_1, base_table__random_column, base_table__work_id, base_table__part_id_2 FROM (SELECT base_table.part_id_1 AS base_table__part_id_1, base_table.random_column AS base_table__random_column, base_table.work_id AS base_table__work_id, base_table.part_id_2 AS base_table__part_id_2, * FROM (select * from base_table) AS base_table) AS base_table) AS __base_query) AS __base_query) AS __base_query LEFT JOIN (SELECT base_table__part_id_1.display_id AS "base_table__part_id_1__display_id", * FROM (select id, display_id from system.dim_feature UNION ALL select id, display_id from system.dim_product) AS base_table__part_id_1) AS base_table__part_id_1 ON __base_query.base_table__part_id_1 = base_table__part_id_1.id) AS MEERKAT_GENERATED_TABLE) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });
});

describe('Resolution Tests (useDotNotation: true)', () => {
  const options = { useDotNotation: true };

  it('No Resolution Config', async () => {
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.work_id',
        'base_table.part_id_2',
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA],
      resolutionConfig: {
        columnConfigs: [],
        tableSchemas: [],
      },
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) 
      from (SELECT "__row_id", "base_table.part_id_1", "base_table.random_column", "base_table.work_id", "base_table.part_id_2" 
        FROM 
          (SELECT __base_query."__row_id" AS "__row_id", 
                  __base_query."base_table.part_id_1" AS "base_table.part_id_1", 
                  __base_query."base_table.random_column" AS "base_table.random_column", 
                  __base_query."base_table.work_id" AS "base_table.work_id", 
                  __base_query."base_table.part_id_2" AS "base_table.part_id_2", * 
          FROM (SELECT MAX(__base_query."base_table.part_id_1") AS "base_table.part_id_1" , 
                   MAX(__base_query."base_table.random_column") AS "base_table.random_column" , 
                   MAX(__base_query."base_table.work_id") AS "base_table.work_id" , 
                   MAX(__base_query."base_table.part_id_2") AS "base_table.part_id_2" , 
                   "__row_id" FROM 
            (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.work_id", "base_table.part_id_2", "__row_id" FROM (SELECT __base_query."base_table.part_id_1" AS "base_table.part_id_1", __base_query."base_table.random_column" AS "base_table.random_column", __base_query."base_table.work_id" AS "base_table.work_id", __base_query."base_table.part_id_2" AS "base_table.part_id_2", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.work_id", "base_table.part_id_2", "__row_id" FROM (SELECT __base_query."base_table.part_id_1" AS "base_table.part_id_1", __base_query."base_table.random_column" AS "base_table.random_column", __base_query."base_table.work_id" AS "base_table.work_id", __base_query."base_table.part_id_2" AS "base_table.part_id_2", row_number() OVER () AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.work_id", "base_table.part_id_2" FROM (SELECT base_table.part_id_1 AS "base_table.part_id_1", base_table.random_column AS "base_table.random_column", base_table.work_id AS "base_table.work_id", base_table.part_id_2 AS "base_table.part_id_2", * FROM (select * from base_table) AS base_table) AS base_table) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) 
      order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });

  it('Resolution Config Missing Table Schema', async () => {
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.work_id',
        'base_table.part_id_2',
      ],
    };

    await expect(
      cubeQueryToSQLWithResolution({
        query,
        tableSchemas: [BASE_TABLE_SCHEMA],
        resolutionConfig: {
          columnConfigs: [
            {
              name: 'base_table.part_id_1',
              type: 'string' as const,
              source: 'dim_part',
              joinColumn: 'id',
              resolutionColumns: ['display_id'],
            },
          ],
          tableSchemas: [],
        },
        options,
      })
    ).rejects.toThrow('Table schema not found for dim_part');
  });

  it('With Resolution Config', async () => {
    // Note: Resolution with join lookups uses underscore notation internally.
    // This test uses empty columnConfigs to verify dot notation in base SQL.
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.work_id',
        'base_table.part_id_2',
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA_WITH_ALIASES],
      resolutionConfig: {
        columnConfigs: [],
        tableSchemas: [],
      },
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) 
        from (SELECT "__row_id", 
                 "Part ID 1", 
                 "Random Column", 
                 "Work ID", 
                 "Part ID 2" 
          FROM (SELECT __base_query."__row_id" AS "__row_id", 
                   __base_query."base_table.part_id_1" AS "Part ID 1", 
                   __base_query."base_table.random_column" AS "Random Column", 
                   __base_query."base_table.work_id" AS "Work ID", 
                   __base_query."base_table.part_id_2" AS "Part ID 2", * 
            FROM (SELECT MAX(__base_query."base_table.part_id_1") AS "base_table.part_id_1" , 
                     MAX(__base_query."base_table.random_column") AS "base_table.random_column" , 
                     MAX(__base_query."base_table.work_id") AS "base_table.work_id" , 
                     MAX(__base_query."base_table.part_id_2") AS "base_table.part_id_2" , 
                     "__row_id" 
              FROM (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.work_id", "base_table.part_id_2", "__row_id" FROM (SELECT __base_query."base_table.part_id_1" AS "base_table.part_id_1", __base_query."base_table.random_column" AS "base_table.random_column", __base_query."base_table.work_id" AS "base_table.work_id", __base_query."base_table.part_id_2" AS "base_table.part_id_2", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.work_id", "base_table.part_id_2", "__row_id" FROM (SELECT __base_query."base_table.part_id_1" AS "base_table.part_id_1", __base_query."base_table.random_column" AS "base_table.random_column", __base_query."base_table.work_id" AS "base_table.work_id", __base_query."base_table.part_id_2" AS "base_table.part_id_2", row_number() OVER () AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.work_id", "base_table.part_id_2" FROM (SELECT base_table.part_id_1 AS "base_table.part_id_1", base_table.random_column AS "base_table.random_column", base_table.work_id AS "base_table.work_id", base_table.part_id_2 AS "base_table.part_id_2", * FROM (select * from base_table) AS base_table) AS base_table) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });

  it('Resolution With Measures', async () => {
    // Note: Resolution with join lookups uses underscore notation internally.
    // This test uses empty columnConfigs to verify dot notation in base SQL.
    const query = {
      measures: ['base_table.count'],
      dimensions: ['base_table.part_id_1'],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA_WITH_ALIASES],
      resolutionConfig: {
        columnConfigs: [],
        tableSchemas: [],
      },
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) from (SELECT "__row_id", "Part ID 1", "Count" FROM (SELECT __base_query."__row_id" AS "__row_id", __base_query."base_table.part_id_1" AS "Part ID 1", __base_query."base_table.count" AS "Count", * FROM (SELECT MAX(__base_query."base_table.part_id_1") AS "base_table.part_id_1" , MAX(__base_query."base_table.count") AS "base_table.count" , "__row_id" FROM (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.count", "__row_id" FROM (SELECT __base_query."base_table.part_id_1" AS "base_table.part_id_1", __base_query."base_table.count" AS "base_table.count", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.count", "__row_id" FROM (SELECT __base_query."base_table.part_id_1" AS "base_table.part_id_1", __base_query."base_table.count" AS "base_table.count", row_number() OVER () AS "__row_id", * FROM (SELECT count(*) AS "base_table.count" , "base_table.part_id_1" FROM (SELECT base_table.part_id_1 AS "base_table.part_id_1", * FROM (select * from base_table) AS base_table) AS base_table GROUP BY "base_table.part_id_1") AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });

  it('Resolution With Aliases', async () => {
    // Note: Resolution with join lookups uses underscore notation internally.
    // This test uses empty columnConfigs to verify dot notation in base SQL.
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.part_id_2',
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA_WITH_ALIASES],
      resolutionConfig: {
        columnConfigs: [],
        tableSchemas: [],
      },
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) from (SELECT "__row_id", "Part ID 1", "Random Column", "Part ID 2" FROM (SELECT __base_query."__row_id" AS "__row_id", __base_query."base_table.part_id_1" AS "Part ID 1", __base_query."base_table.random_column" AS "Random Column", __base_query."base_table.part_id_2" AS "Part ID 2", * FROM (SELECT MAX(__base_query."base_table.part_id_1") AS "base_table.part_id_1" , MAX(__base_query."base_table.random_column") AS "base_table.random_column" , MAX(__base_query."base_table.part_id_2") AS "base_table.part_id_2" , "__row_id" FROM (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.part_id_2", "__row_id" FROM (SELECT __base_query."base_table.part_id_1" AS "base_table.part_id_1", __base_query."base_table.random_column" AS "base_table.random_column", __base_query."base_table.part_id_2" AS "base_table.part_id_2", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.part_id_2", "__row_id" FROM (SELECT __base_query."base_table.part_id_1" AS "base_table.part_id_1", __base_query."base_table.random_column" AS "base_table.random_column", __base_query."base_table.part_id_2" AS "base_table.part_id_2", row_number() OVER () AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.part_id_2" FROM (SELECT base_table.part_id_1 AS "base_table.part_id_1", base_table.random_column AS "base_table.random_column", base_table.part_id_2 AS "base_table.part_id_2", * FROM (select * from base_table) AS base_table) AS base_table) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });

  it('Resolution With Column Projections', async () => {
    // Note: Resolution with join lookups uses underscore notation internally.
    // This test uses empty columnConfigs to verify dot notation in base SQL.
    const query = {
      measures: [],
      dimensions: [
        'base_table.part_id_1',
        'base_table.random_column',
        'base_table.work_id',
        'base_table.part_id_2',
      ],
    };

    const sql = await cubeQueryToSQLWithResolution({
      query,
      tableSchemas: [BASE_TABLE_SCHEMA_WITH_ALIASES],
      resolutionConfig: {
        columnConfigs: [],
        tableSchemas: [],
      },
      columnProjections: ['base_table.random_column', 'base_table.part_id_1'],
      options,
    });

    const expectedSQL = `
      select * exclude(__row_id) from (SELECT "__row_id", "Random Column", "Part ID 1" FROM (SELECT __base_query."__row_id" AS "__row_id", __base_query."base_table.random_column" AS "Random Column", __base_query."base_table.part_id_1" AS "Part ID 1", * FROM (SELECT MAX(__base_query."base_table.random_column") AS "base_table.random_column" , MAX(__base_query."base_table.part_id_1") AS "base_table.part_id_1" , "__row_id" FROM (SELECT __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.random_column", "base_table.part_id_1", "__row_id" FROM (SELECT __base_query."base_table.random_column" AS "base_table.random_column", __base_query."base_table.part_id_1" AS "base_table.part_id_1", __base_query."__row_id" AS "__row_id", * FROM (SELECT "base_table.random_column", "base_table.part_id_1", "__row_id" FROM (SELECT __base_query."base_table.random_column" AS "base_table.random_column", __base_query."base_table.part_id_1" AS "base_table.part_id_1", row_number() OVER () AS "__row_id", * FROM (SELECT "base_table.part_id_1", "base_table.random_column", "base_table.work_id", "base_table.part_id_2" FROM (SELECT base_table.part_id_1 AS "base_table.part_id_1", base_table.random_column AS "base_table.random_column", base_table.work_id AS "base_table.work_id", base_table.part_id_2 AS "base_table.part_id_2", * FROM (select * from base_table) AS base_table) AS base_table) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query) AS __base_query GROUP BY __row_id) AS __base_query) AS __base_query) order by __row_id
    `;

    expect(normalizeSQL(sql)).toBe(normalizeSQL(expectedSQL));
  });
});
