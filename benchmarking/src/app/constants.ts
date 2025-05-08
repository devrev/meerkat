import { tableName } from './query-benchmarking/dummy-data';

const QUERY_1 = `SELECT * FROM taxi limit 50`;

const QUERY_2 = `(SELECT *
FROM taxi t1 
WHERE t1.PULocationID NOT IN (
  SELECT PULocationID 
  FROM in_memory_taxi_trips
))
UNION
(SELECT *
FROM in_memory_taxi_trips) limit 50`;

const QUERY_3 = `(SELECT *
    FROM taxi t1 
    WHERE NOT EXISTS (
        SELECT 1 
        FROM in_memory_taxi_trips t2 
        WHERE t2.PULocationID = t1.PULocationID
    ))
    UNION ALL
    (SELECT * FROM in_memory_taxi_trips) limit 50`;

const QUERY_4 = `SELECT t1.*,
FROM taxi t1
LEFT ANTI JOIN ${tableName} t2
ON t1.PULocationID = t2.PULocationID
UNION ALL
SELECT *,
FROM ${tableName} limit 50`;

const BASE_QUERY = `(SELECT * FROM taxi LIMIT 50) AS taxi`;

export const TEST_QUERIES = [
  `SELECT count(*) FROM taxi`,
  // `select * from (
  //   SELECT * FROM ${BASE_QUERY} WHERE taxi.PULocationID NOT IN (SELECT ${tableName}.PULocationID FROM ${tableName})
  //   UNION ALL (SELECT * FROM ${tableName}))`,
];
