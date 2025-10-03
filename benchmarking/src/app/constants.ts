export const TEST_QUERIES = [
  `
      WITH group_by_query AS (
        SELECT
            hvfhs_license_num,
            COUNT(*)
        FROM
            taxi
        GROUP BY
            hvfhs_license_num
    ),

    full_query AS (
        SELECT
            *
        FROM
            taxi
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
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi',
  "SELECT * FROM taxi WHERE originating_base_num='B03404' LIMIT 100",
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi GROUP BY hvfhs_license_num',
  'SELECT * FROM taxi ORDER BY bcf LIMIT 100',
  `
    WITH group_by_query AS (
      SELECT
          hvfhs_license_num,
          COUNT(*)
      FROM
          taxi
      GROUP BY
          hvfhs_license_num
  ),

  full_query AS (
      SELECT
          *
      FROM
          taxi
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
  // 'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi',
  // "SELECT * FROM taxi WHERE originating_base_num='B03404' LIMIT 100",
  // 'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi GROUP BY hvfhs_license_num',
  // 'SELECT * FROM taxi ORDER BY bcf LIMIT 100',
  // `
  // WITH group_by_query AS (
  //   SELECT
  //       hvfhs_license_num,
  //       COUNT(*)
  //   FROM
  //       taxi
  //   GROUP BY
  //       hvfhs_license_num
  // ),

  // full_query AS (
  //   SELECT
  //       *
  //   FROM
  //       taxi
  // )

  // SELECT
  // COUNT(*)
  // FROM
  // group_by_query
  // LEFT JOIN
  // full_query
  // ON
  // group_by_query.hvfhs_license_num = full_query.hvfhs_license_num
  // LIMIT 1
  // `,
  // 'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi',
  // "SELECT * FROM taxi WHERE originating_base_num='B03404' LIMIT 100",
  // 'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi GROUP BY hvfhs_license_num',
  // 'SELECT * FROM taxi ORDER BY bcf LIMIT 100',
  // 'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi_json',
  // 'SELECT * FROM taxi_json WHERE price >= 1.0005812645 LIMIT 100',
  // 'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi_json GROUP BY order_count',
  // 'SELECT * FROM taxi_json ORDER BY seconds_in_bucket LIMIT 100',
];
