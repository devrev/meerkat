export const TEST_QUERIES = [
  `
      WITH group_by_query AS (
        SELECT
            hvfhs_license_num,
            COUNT(*)
        FROM
            taxi.parquet
        GROUP BY
            hvfhs_license_num
    ),

    full_query AS (
        SELECT
            *
        FROM
            taxi.parquet
    )

  SELECT
      COUNT(*)
  FROM
      group_by_query
  LEFT JOIN
      full_query
  ON
      group_by_query.hvfhs_license_num = full_query.hvfhs_license_num
  LIMIT 1
    `,
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet',
  "SELECT * FROM taxi.parquet WHERE originating_base_num='B03404' LIMIT 100",
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet GROUP BY hvfhs_license_num',
  'SELECT * FROM taxi.parquet ORDER BY bcf LIMIT 100',

  `
    WITH group_by_query AS (
      SELECT
          hvfhs_license_num,
          COUNT(*)
      FROM
          taxi.parquet
      GROUP BY
          hvfhs_license_num
  ),

  full_query AS (
      SELECT
          *
      FROM
          taxi.parquet
  )

  SELECT
    COUNT(*)
  FROM
    group_by_query
  LEFT JOIN
    full_query
  ON
    group_by_query.hvfhs_license_num = full_query.hvfhs_license_num
  LIMIT 1
  `,
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet',
  "SELECT * FROM taxi.parquet WHERE originating_base_num='B03404' LIMIT 100",
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet GROUP BY hvfhs_license_num',
  'SELECT * FROM taxi.parquet ORDER BY bcf LIMIT 100',
  `
  WITH group_by_query AS (
    SELECT
        hvfhs_license_num,
        COUNT(*)
    FROM
        taxi.parquet
    GROUP BY
        hvfhs_license_num
  ),

  full_query AS (
    SELECT
        *
    FROM
        taxi.parquet
  )

  SELECT
  COUNT(*)
  FROM
  group_by_query
  LEFT JOIN
  full_query
  ON
  group_by_query.hvfhs_license_num = full_query.hvfhs_license_num
  LIMIT 1
  `,
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet',
  "SELECT * FROM taxi.parquet WHERE originating_base_num='B03404' LIMIT 100",
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet GROUP BY hvfhs_license_num',
  'SELECT * FROM taxi.parquet ORDER BY bcf LIMIT 100',
];
