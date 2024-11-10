export const TEST_QUERIES = [
  // "SELECT json_serialize_sql('SELECT CASE WHEN AVG(score_value) <= -(1/3) THEN ''At Risk'' WHEN AVG(score_value) >= (1/3) THEN ''Healthy'' ELSE ''Neutral'' END')",
  // "SELECT json_serialize_sql('SELECT COALESCE((select value from consignment_count cc2 where cc2.account_id = account_metrics__account and cast(cast(cc2.timestamp_nsecs as timestamp) as date) = cast(current_date - 1 as date) order by timestamp_nsecs desc limit 1),0)')",
  "SELECT json_serialize_sql('SELECT COALESCE(MEDIAN(next_resp_time), 0)')",
];
