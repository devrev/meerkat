export const TEST_QUERIES = [
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet',
  "SELECT * FROM taxi.parquet WHERE originating_base_num='B03404' LIMIT 100",
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet GROUP BY hvfhs_license_num',
  'SELECT * as total_count FROM taxi.parquet ORDER BY bcf LIMIT 100',
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
  'SELECT * FROM taxi.parquet WHERE congestion_surcharge >= 1.0005812645 LIMIT 100',
  'SELECT CAST(COUNT(*) as VARCHAR) as total_count FROM taxi.parquet GROUP BY wav_match_flag',
  'SELECT dispatching_base_num, AVG(driver_pay) as avg_driver_pay FROM taxi.parquet GROUP BY dispatching_base_num ORDER BY avg_driver_pay DESC LIMIT 5',
  'SELECT shared_request_flag, shared_match_flag, AVG(trip_time) as avg_trip_time FROM taxi.parquet GROUP BY shared_request_flag, shared_match_flag ORDER BY avg_trip_time DESC',
  'SELECT *, trip_time / trip_miles as time_per_mile FROM taxi.parquet WHERE trip_miles > 0 ORDER BY time_per_mile DESC LIMIT 10',
];
