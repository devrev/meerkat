export const MEASURES = [
  {
    sql_expression:
      "AVG(DATE_DIFF('minute', created_date, first_response_time))",
  },
  {
    sql_expression: 'array_length(array_distinct(flatten(ARRAY_AGG(issues))))',
  },
  {
    sql_expression: 'SUM(cumulative_count_id)',
  },
  {
    sql_expression:
      "100.0 * (\nCOUNT(DISTINCT CASE WHEN state != 'closed' AND 'don:core:dvrv-us-1:devo/0:tag/1606' = ANY(tag_ids) AND state IS NOT NULL THEN id END)\n/ NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0)\n)",
  },
  {
    sql_expression: 'max(p50_upstream_service_time) / 1000',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/7Gtyici3' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT user_id) FILTER(user_percentage >= 0.6)',
  },
  {
    sql_expression:
      'CASE WHEN COUNT(id) > 1 THEN AVG(mtbf_hours) ELSE null END',
  },
  {
    sql_expression: 'SUM(CASE WHEN priority = 1 THEN 1 ELSE 0 END)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/21JF3hmR' THEN metric_set_value END)",
  },
  {
    sql_expression:
      "COUNT( DISTINCT (CASE WHEN (range_state='open') THEN id END))",
  },
  {
    sql_expression:
      "count(CASE WHEN stage in ('On Hold', 'On-hold', 'On hold') AND te is true THEN ticket END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'U-Capture' THEN id END)  || ''",
  },
  {
    sql_expression: 'AVG(time_to_open)',
  },
  {
    sql_expression: 'SUM("In Progress")',
  },
  {
    sql_expression:
      "count(distinct CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '2' THEN id END) || ''",
  },
  {
    sql_expression:
      "count(distinct CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '5' THEN id END) || ''",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'In Progress/ Scoping' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Client approval pending on estimate' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Tech Doc Inprogress' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Test Cases in Progress' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression: 'SUM(visits_count)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) AND status = 'miss' THEN id END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN final_answer <> '' THEN 1 END) / COUNT(*))*100",
  },
  {
    sql_expression: 'ANY_VALUE(closed_date)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/16bjTrSu7' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_SALESFORCE_SERVICE' THEN duration END)/SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_SALESFORCE_SERVICE' THEN count_sync END)",
  },
  {
    sql_expression: 'AVG(completed_issue)',
  },
  {
    sql_expression: "sum(case when priority = 'P3' then 1 end)",
  },
  {
    sql_expression: "count(case when visibility='INTERNAL' then 1 end)",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT(CASE WHEN metric_status = 'miss' THEN id END))* 100.0 / COUNT(DISTINCT(CASE WHEN metric_status = 'hit' OR metric_status = 'miss' THEN id END)))",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) != DATE_TRUNC('day', record_hour)  AND DATE_TRUNC('day', actual_close_date) = DATE_TRUNC('day', record_hour) THEN id END)",
  },
  {
    sql_expression: 'COUNT(CASE WHEN opp_id IS NOT NULL THEN 1 ELSE NULL END)',
  },
  {
    sql_expression:
      'COUNT(CASE WHEN target_close_date < CURRENT_DATE THEN 1 ELSE NULL END)',
  },
  {
    sql_expression: 'SUM(daily_active_users)/COUNT(DISTINCT(created_date))',
  },
  {
    sql_expression:
      'SUM(mean_reciprocal_rank * total_queries) / SUM(total_queries)',
  },
  {
    sql_expression: 'count( id)',
  },
  {
    sql_expression:
      '((sum(provisioning)/COUNT(DISTINCT(timestamp))))/((sum(total_expected)/COUNT(DISTINCT(timestamp))))*100',
  },
  {
    sql_expression:
      'ARRAY_AGG(DISTINCT id) FILTER (WHERE      (sprint_id is not null)     )',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN CONCAT(rev_uid, rev_oid) = '' THEN NULL ELSE CONCAT(rev_uid, rev_oid) END)",
  },
  {
    sql_expression:
      "100.0 * (\n    COUNT(DISTINCT CASE \n        WHEN state != 'closed' AND \n           ( ('don:core:dvrv-us-1:devo/0:tag/2124' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13375' = ANY(tag_ids)) AND \n            state IS NOT NULL  THEN id  END) \n    / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0))\n\n",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Assist' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM(api_success_count)',
  },
  {
    sql_expression:
      '(COUNT(CASE WHEN metric_completed_in <= 60 THEN id END)/count(id)) * 100',
  },
  {
    sql_expression:
      "(select CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL ) AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL ) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 /(COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' THEN id END))) ELSE NULL END)",
  },
  {
    sql_expression: "ANY_VALUE(DATE_TRUNC('week',created_date))",
  },
  {
    sql_expression: "avg(datediff('day',created_date,current_date))",
  },
  {
    sql_expression: 'MEDIAN(first_resp_time_arr)',
  },
  {
    sql_expression: 'SUM(total_responses)',
  },
  {
    sql_expression: 'any_value(created_date)',
  },
  {
    sql_expression: "COUNT(*)  || ''",
  },
  {
    sql_expression:
      "CAST(AVG(datediff('day', CASE WHEN custom_fields ->> 'ctype__created_at_cfid' IS NOT NULL THEN Cast( custom_fields ->> 'ctype__created_at_cfid' AS TIMESTAMP) ELSE created_date END, current_date)) AS INT) || ' days'",
  },
  {
    sql_expression:
      "AVG(CAST(json_extract(custom_fields, 'tnt__sonar_bugs') AS INTEGER)) FILTER (CAST(json_extract(custom_fields, 'tnt__sonar_bugs') AS INTEGER) IS NOT NULL)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN status = 'hit' THEN sla_id END)/count(sla_id)) * 100",
  },
  {
    sql_expression: 'SUM(duration)/SUM(count_sync)',
  },
  {
    sql_expression: 'max(p90_upstream_service_time) / 1000',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN status = 'miss' THEN conv_id END))",
  },
  {
    sql_expression: 'count(distinct conv)',
  },
  {
    sql_expression: 'sum(api_response_time * api_hits) / sum(api_hits)',
  },
  {
    sql_expression: 'avg(delta_percent)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/uN6ECfdm' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/3' AND json_extract_string(metric, '$.status') == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression:
      "COUNT( DISTINCT CASE WHEN (range_state='closed') THEN id END)",
  },
  {
    sql_expression:
      '(SUM(CAST(total_nodes AS INTEGER)) / SUM(CAST(total_nodes AS INTEGER))) * 100',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Hostfuse' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM(total_user_sessions)',
  },
  {
    sql_expression: 'SUM(count_meeting)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN acc_billing_country = 'APJ' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'QA on Demo' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression: "SUM(CASE WHEN page = 'Homepage' THEN visits_count END)",
  },
  {
    sql_expression: 'ROUND(SUM(total_bytes_billed/(1024*1024*1024)),2)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/1' AND json_extract_string(metric, '$.status') == 'hit' THEN ticket_id END)*100/COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/1' THEN ticket_id END))",
  },
  {
    sql_expression:
      "count(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) == DATE_TRUNC('day', record_date) THEN ticket END)",
  },
  {
    sql_expression: "COUNT(CASE WHEN stage = 'completed' then id end)",
  },
  {
    sql_expression:
      "SUM(total_contributions_on_part) FILTER (type = 'github.pull_request.merged' OR type = 'bitbucket.pull_request.fulfilled')",
  },
  {
    sql_expression: 'COUNT(DISTINCT issue_id)',
  },
  {
    sql_expression:
      "COUNT(distinct CASE WHEN json_extract_string(stage_json, '$.name') = 'awaiting_development' THEN id END)  || ''",
  },
  {
    sql_expression: 'max(curr_p95_latency) / 1000',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/Ds2FRuuX' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "(SUM(weighted_contribution) FILTER(source_part LIKE '%enhancement%')) / SUM(weighted_contribution) * 100",
  },
  {
    sql_expression:
      "SUM(CASE WHEN owner = 'don:identity:dvrv-us-1:devo/1B2GHUnRr:devu/616' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression: "sum(case when priority = 'P0' then 1 end)",
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='awaiting_development' and 'don:core:dvrv-us-1:devo/0:tag/1606'=ANY(tag_ids) and severity_name='High' and state != 'closed'  AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "COUNT( DISTINCT CASE WHEN ((product_effort_planned IS NULL )                  OR (target_start_date IS NULL OR target_start_date = '')                  OR (target_close_date IS NULL OR target_close_date = '')) THEN id ELSE NULL END) ",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE      WHEN metric_status = 'miss'  AND metric_stage!='completed'    THEN id  END)",
  },
  {
    sql_expression: "COUNT(CASE WHEN sla_stage = 'completed' THEN id END)",
  },
  {
    sql_expression: 'SUM(total_successful_queries)',
  },
  {
    sql_expression: 'round(avg(mtbf_hours))',
  },
  {
    sql_expression: 'SUM(total_views)',
  },
  {
    sql_expression: 'COUNT(profile)',
  },
  {
    sql_expression: 'COUNT(validate)',
  },
  {
    sql_expression:
      "CAST(COUNT(DISTINCT(id)) AS FLOAT) / NULLIF(DATEDIFF('day', MIN(created_date), MAX(created_date)) / 7 + 1, 0)",
  },
  {
    sql_expression: 'any_value(total_survey_dispatched)',
  },
  {
    sql_expression:
      'COALESCE((select value from orders_with_cn_value cc2 where cc2.account_id = account_metrics__account and cast(cast(cc2.timestamp_nsecs as timestamp) as date) = cast(current_date - 1 as date) order by timestamp_nsecs desc limit 1),0)',
  },
  {
    sql_expression: 'SUM(csat_sum)/SUM(csat_count)',
  },
  {
    sql_expression: 'max(p95_latency) / 1000',
  },
  {
    sql_expression: "COUNT(CASE WHEN metric_status = 'miss' THEN id END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN stage_json ->> 'name' IN ( 'On Hold', 'On-hold', 'On hold' ) THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN first_time_fix = 'Yes' OR first_time_fix = 'true' THEN id END)  || ''",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN stage = 'breached' THEN id END)* 100.0 / COUNT(*))",
  },
  {
    sql_expression:
      "AVG(CAST(json_extract(custom_fields, 'tnt__sonar_coverage') AS INTEGER)) FILTER (CAST(json_extract(custom_fields, 'tnt__sonar_coverage') AS INTEGER) IS NOT NULL)",
  },
  {
    sql_expression: 'min(min_duration)',
  },
  {
    sql_expression: 'count(distinct ticketId)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN te is true AND state='open' THEN ticket END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN ctype__sla_has_breached = false THEN id WHEN status = 'hit' THEN id END)/count(id)) * 100",
  },
  {
    sql_expression: 'sum(dead_clicks) / count(*)',
  },
  {
    sql_expression: 'COUNT( id)',
  },
  {
    sql_expression: "AVG(datediff('day', created_date, actual_close_date))",
  },
  {
    sql_expression: 'AVG(act_result)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'U-Assist' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM("Started: Signed up")',
  },
  {
    sql_expression: 'SUM("Activated: Integrated")',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN status != '' THEN ticket_id END))",
  },
  {
    sql_expression: 'SUM(SUM(amount)) OVER (PARTITION BY owned_by_id)',
  },
  {
    sql_expression: 'sum(daily_tickets_created)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/1EYSCQzBT' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Ready for scoping' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Sprint Planning' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'in_testing' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'in_development' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Ready for Merge' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Client approval pending on estimate' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'RCA IN PROGRESS' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression: 'SUM(total_changes)',
  },
  {
    sql_expression:
      "MEDIAN(DATE_DIFF('minute', created_date, actual_close_date))",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', record_hour) = DATE_TRUNC('day', created_date) and state = 'closed' THEN id END)",
  },
  {
    sql_expression: 'avg(metric_set_value)',
  },
  {
    sql_expression: 'SUM(executions_count)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN stage = 'breached' THEN datediff('day',created_date,current_date) END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) AND status = 'hit' THEN id END)",
  },
  {
    sql_expression:
      'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY time_to_open + time_to_review + time_to_approve + time_to_merge + avg_time_to_deploy)',
  },
  {
    sql_expression:
      "COUNT(*) FILTER(state = 'in_progress' AND CURRENT_DATE()>target_close_date)",
  },
  {
    sql_expression:
      'CASE WHEN SUM(ARRAY_LENGTH(next_resp_time_arr) + ARRAY_LENGTH(first_resp_time_arr) + ARRAY_LENGTH(resolution_time_arr)) > 0 THEN 100 - (SUM(CASE WHEN ARRAY_LENGTH(next_resp_time_arr) > 0 THEN total_second_resp_breaches_ever ELSE 0 END + CASE WHEN ARRAY_LENGTH(first_resp_time_arr) > 0 THEN total_first_resp_breaches_ever ELSE 0 END + CASE WHEN ARRAY_LENGTH(resolution_time_arr) > 0 THEN total_resolution_breaches_ever ELSE 0 END) / SUM(ARRAY_LENGTH(next_resp_time_arr) + ARRAY_LENGTH(first_resp_time_arr) + ARRAY_LENGTH(resolution_time_arr))) * 100 END',
  },
  {
    sql_expression: 'ANY_VALUE(target_close_date)',
  },
  {
    sql_expression:
      "COUNT(Distinct CASE WHEN json_extract_string(stage_json, '$.name') = 'awaiting_sales' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(Distinct CASE WHEN json_extract_string(stage_json, '$.name') = 'awaiting_vulnerabilities' THEN id END)  || ''",
  },
  {
    sql_expression:
      "CASE WHEN max(curr_p90_latency) > LAG(max(curr_p90_latency)) OVER (ORDER BY max(curr_timestamp) asc) THEN 'Yes' ELSE 'No' END",
  },
  {
    sql_expression: "CAST( strftime('%H', formatted_created_date) AS VARCHAR)",
  },
  {
    sql_expression: 'ANY_VALUE(incident_count_ratio)',
  },
  {
    sql_expression: 'COUNT(distinct issue_id)',
  },
  {
    sql_expression: 'ANY_VALUE(sprint_board_count)',
  },
  {
    sql_expression:
      'sum(case when sprint_board_count>0 then 1 else 0 end )/count(*)',
  },
  {
    sql_expression:
      "sum(case when lifecycle_details = 'CLOSED' or lifecycle_details = 'CLOSE_REQUESTED' then 1 else 0 end)",
  },
  {
    sql_expression: "sum(case when exception_type = 'CRASH' then 1 else 0 end)",
  },
  {
    sql_expression: 'any_value(sprint_start_date)',
  },
  {
    sql_expression:
      'ARRAY_AGG(id) FILTER (WHERE target_close_date > sprint_end_date)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/5WLaGLPW' THEN metric_set_value END)",
  },
  {
    sql_expression:
      "AVG(AVG(CASE WHEN pr_approved_time IS NULL AND pr_reviewed_time IS NULL THEN DATE_SUB('second', pr_opened_time, pr_merged_time) WHEN pr_approved_time IS NULL THEN DATE_SUB('second', pr_reviewed_time, pr_merged_time) ELSE DATE_SUB('second', pr_approved_time, pr_merged_time) END)) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)",
  },
  {
    sql_expression:
      '((sum(unhealthy)/COUNT(DISTINCT(timestamp))))/((sum(total_expected)/COUNT(DISTINCT(timestamp))))*100',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN sla_stage = 'completed' THEN id END)* 100.0 / COUNT(*))",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/2JxwhYm6' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN metric_summary.value ->> 'metric_definition_id' LIKE '%metric_definition/3%' and metric_status = 'hit' THEN id END)/COUNT(CASE WHEN metric_summary.value ->> 'metric_definition_id' LIKE '%metric_definition/3%' and metric_status in ('miss','hit') THEN id END)) * 100",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'KAI' THEN id END)  || ''",
  },
  {
    sql_expression: 'COUNT(DISTINCT opp_id)',
  },
  {
    sql_expression: 'max(p90_latency) / 1000',
  },
  {
    sql_expression:
      "(SELECT CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND ARRAY_LENGTH(first_resp_time_arr) > 0 AND (total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 / (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND ARRAY_LENGTH(first_resp_time_arr) > 0 AND (total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL) THEN id END))) ELSE NULL END AS result)",
  },
  {
    sql_expression:
      "AVG(CAST(json_extract(custom_fields, 'tnt__sonar_code_smells') AS INTEGER)) FILTER (CAST(json_extract(custom_fields, 'tnt__sonar_code_smells') AS INTEGER) IS NOT NULL)",
  },
  {
    sql_expression: 'any_value(stage)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN te is true AND state='in_progress' THEN ticket END)",
  },
  {
    sql_expression:
      "MEDIAN(FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT))) || ''",
  },
  {
    sql_expression:
      'COUNT(CASE WHEN ctype__u_service_improvementsu_service_improvements_cfid = true THEN id END)',
  },
  {
    sql_expression:
      "AVG(DATE_DIFF('minute', COALESCE(created_at, opened_at, created_date)::TIMESTAMP, actual_close_date))",
  },
  {
    sql_expression:
      'count(CASE WHEN service_improvement_status is false THEN id END)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN ctype__sla_has_breached = false THEN id WHEN metric_status = 'hit' THEN id END)/count(id)) * 100",
  },
  {
    sql_expression: 'count(distinct identifier)',
  },
  {
    sql_expression: 'sum(api_errors)',
  },
  {
    sql_expression:
      "CASE WHEN AVG(score_value) <= -(1/3) THEN 'At Risk' WHEN AVG(score_value) >= (1/3) THEN 'Healthy' ELSE 'Neutral' END",
  },
  {
    sql_expression: 'avg(prev_score_value)*100',
  },
  {
    sql_expression:
      "count(distinct(case when category = 'at_risk' then account_id else null end))",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN gen_answer <> '' THEN 1 END) / COUNT(*))*100",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE when state != 'closed' AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: "AVG(DATEDIFF('minute', created_date, actual_close_date))",
  },
  {
    sql_expression: 'SUM(count_offline)',
  },
  {
    sql_expression: 'SUM(count_email)',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) AND status = 'hit' THEN id END) / (COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) AND status = 'hit' THEN id END) + COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) AND status = 'miss' THEN id END))) * 100",
  },
  {
    sql_expression: 'SUM(amount)',
  },
  {
    sql_expression:
      "printf('%.2f%%',SUM(CAST(total_prs_with_cypress_tests as DOUBLE)) / SUM(CAST(total_prs as DOUBLE)) * 100)",
  },
  {
    sql_expression: 'SUM(CAST(todos as DOUBLE))',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/6cqTR14h' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/LzS6CEY7' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='awaiting_product_assist' and 'don:core:dvrv-us-1:devo/0:tag/508'=ANY(tag_ids) and severity_name='High' and state != 'closed'  AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='awaiting_product_assist' and 'don:core:dvrv-us-1:devo/0:tag/508'=ANY(tag_ids) and severity_name='Blocker' and state != 'closed'  AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN work_status = 'in_progress' THEN total_estimated_effort_hours ELSE 0 END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN state = 'closed' AND actual_close_date != '1970-01-01T00:00:00.000Z' THEN DATEDIFF('minute', created_date, actual_close_date) END)",
  },
  {
    sql_expression: 'avg(count_of_active_snapins)',
  },
  {
    sql_expression: 'AVG(duration_double)',
  },
  {
    sql_expression:
      'Sum(support_insights_ticket_metrics_summary__ticket_count)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/ogNAgdmp:metric_set/daqVrrZ9' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "SUM(total_contributions_on_part) FILTER (type = 'github.pull_request.opened' OR type = 'bitbucket.pull_request.created')",
  },
  {
    sql_expression: 'round(sum(total_expected)/COUNT(DISTINCT(timestamp)))',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', modified_date) = DATE_TRUNC('day', record_hour) AND status = 'hit' THEN id END)",
  },
  {
    sql_expression:
      'CASE WHEN (opp_close_date <= ANY_VALUE(latest_close_date_before_today)) THEN (SUM(SUM(actual_amount)) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING)) END',
  },
  {
    sql_expression:
      'PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY time_to_merge)',
  },
  {
    sql_expression: 'count(source_channel)',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN metric_status = 'hit' THEN id END) * 100.0 / (COUNT(DISTINCT CASE WHEN metric_status = 'hit' THEN id END) + COUNT(DISTINCT CASE WHEN metric_status = 'miss' THEN id END)))",
  },
  {
    sql_expression:
      "COUNT(distinct case when  range_state ='closed'  then  id end)",
  },
  {
    sql_expression:
      'COUNT(CASE WHEN target_close_date >= CURRENT_DATE AND target_close_date < sprint_end_date THEN 1 ELSE NULL END)',
  },
  {
    sql_expression:
      'COUNT(CASE WHEN target_close_date > sprint_end_date THEN 1 ELSE NULL END)',
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \ntraversed_stage ='awaiting_product_assist' and traversed_state != 'closed'  AND traversed_state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: 'ANY_VALUE(employment_status)',
  },
  {
    sql_expression: 'SUM(val)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN custom_fields->>'ctype__total_alerts' IS NOT NULL THEN CAST(custom_fields->>'ctype__total_alerts' AS INT) ELSE 0 END)/7",
  },
  {
    sql_expression:
      'MAX_BY(COALESCE(external_actor_display_id, actor) ,record_date)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT active_id) FILTER (final_state = 'in_progress' AND active_id <> '')",
  },
  {
    sql_expression: 'any_value(sla_score_latest)',
  },
  {
    sql_expression:
      "AVG(AVG(DATE_SUB('second', pr_opened_time, pr_reviewed_time))) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)",
  },
  {
    sql_expression: 'COUNT(DISTINCT(opps))',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Analyze NG' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Analyze' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Analyze NG' THEN id END)  || ''",
  },
  {
    sql_expression:
      'case when count(distinct display_id) > 1 then round((sum(mtbf_days) / (count(*) - 1))) else null end',
  },
  {
    sql_expression: 'MEDIAN(next_resp_time_arr)',
  },
  {
    sql_expression:
      "AVG(FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)))",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/2' AND (json_extract_string(metric, '$.status') == 'hit' OR json_extract_string(metric, '$.status') == 'in_progress') THEN ticket_id END)*100/COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/2' THEN ticket_id END))",
  },
  {
    sql_expression: 'array_distinct((ARRAY_AGG(latest_part_id)))',
  },
  {
    sql_expression: "COUNT(CASE WHEN state='open' THEN ticket END)",
  },
  {
    sql_expression: 'SUM(daily_active_users)',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN state=='closed' THEN id END)/COUNT(DISTINCT id))*100",
  },
  {
    sql_expression: 'abs(avg(value))',
  },
  {
    sql_expression:
      'AVG(SUM(contribution)) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/lG0eqLha' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'U-Capture' THEN ticket END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Workspace' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM("Duplicate")',
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN status = 'hit' THEN id END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN is_metric_completed = 'completed' THEN id END)/count(id)) * 100",
  },
  {
    sql_expression: 'any_value(stage_name)',
  },
  {
    sql_expression:
      "100.0 * (\n    COUNT(DISTINCT CASE \n        WHEN state != 'closed' AND \n           ( 'don:core:dvrv-us-1:devo/0:tag/2129' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13376' = ANY(tag_ids)) AND \n            state IS NOT NULL  THEN id  END) \n    / NULLIF(COUNT(DISTINCT CASE \n                  WHEN state != 'closed' AND \n                       state IS NOT NULL AND \n                       NOT ('don:core:dvrv-us-1:devo/0:tag/13381' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13380' = ANY(tag_ids)) THEN id END), 0))\n",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/5WLaGLPW' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/2JxwhYm6' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/10VEbARzh' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/10qRijQBr' THEN metric_set_value END)",
  },
  {
    sql_expression:
      'CASE when SUM(total_objects) > 0 then (SUM(valid_objects) * 100)/SUM(total_objects) else 100 end',
  },
  {
    sql_expression:
      "sum(CAST(JSON_EXTRACT(custom_fields, '$.ctype__amount') AS DOUBLE))",
  },
  {
    sql_expression: "COUNT(CASE WHEN stage = 'completed' THEN id END)",
  },
  {
    sql_expression: 'ANY_VALUE(total_lines_modified)',
  },
  {
    sql_expression: 'SUM(closed_tickets)/COUNT(*)',
  },
  {
    sql_expression: 'SUM(total_communication_contributions)',
  },
  {
    sql_expression:
      "COUNT(distinct CASE WHEN JSON_EXTRACT_STRING(stage_json, '$.name') != 'duplicate' THEN id END)",
  },
  {
    sql_expression: "sum(case when priority = 'P2' then 1 end)",
  },
  {
    sql_expression:
      "sum(case when exception_type = 'ANR' then 1 else 0 end) * 100 / count(*)",
  },
  {
    sql_expression: 'COUNT(DISTINCT COALESCE(user_id, actor))',
  },
  {
    sql_expression:
      'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY time_to_review)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/Ds2FRuuX' THEN metric_set_value END)",
  },
  {
    sql_expression: 'ANY_VALUE(user_percentage) * 100',
  },
  {
    sql_expression:
      "(CASE WHEN ({MEERKAT}. <= (MAX(CASE WHEN date_diff('day', current_date(), {MEERKAT}.opp_close_date) < 0 THEN {MEERKAT}.opp_close_date ELSE '1754-08-30' END) OVER ())) THEN (SUM(SUM(actual_amount)) OVER (ORDER BY {MEERKAT}.opp_close_date ROWS UNBOUNDED PRECEDING)) END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN stage_json ->> 'name' IN ( 'On Hold', 'On-hold', 'On hold' ) THEN id END)  || ''",
  },
  {
    sql_expression: 'avg(curr_score_value)',
  },
  {
    sql_expression: 'COUNT(*) / COUNT(DISTINCT record_date)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Analyze' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Quantify' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'X-Console/Interact' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'WorkFlow (J)' THEN id END)  || ''",
  },
  {
    sql_expression:
      "MEDIAN(DATE_DIFF('minute', created_date, first_response_time))",
  },
  {
    sql_expression:
      "(SELECT CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND ARRAY_LENGTH(resolution_time_arr) > 0 AND (total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 / (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND ARRAY_LENGTH(resolution_time_arr) > 0 AND (total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL) THEN id END))) ELSE NULL END AS result)",
  },
  {
    sql_expression: 'SUM(COALESCE(total_second_resp_breaches, 0))',
  },
  {
    sql_expression: 'SUM(total_survey_dispatched)',
  },
  {
    sql_expression: 'count(distinct ticket)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN stage_json->>'name' in ('On Hold', 'On-hold', 'On hold') THEN 'On Hold' WHEN state = 'in_progress' THEN 'In Progress'  END)",
  },
  {
    sql_expression: "COUNT(DISTINCT id) || ''",
  },
  {
    sql_expression: "COUNT(CASE WHEN severity = '7' THEN id END) || ''",
  },
  {
    sql_expression: 'any_value(sla_stage)',
  },
  {
    sql_expression:
      "100.0 * COUNT(DISTINCT CASE WHEN CURRENT_DATE - CAST(created_date AS DATE) > 3 AND state != 'closed' AND state IS NOT NULL THEN id END) / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0)",
  },
  {
    sql_expression: 'AVG(mttr)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/FhAaKJlu' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/lG0eqLha' THEN metric_set_value END)",
  },
  {
    sql_expression: 'COUNT(acv)',
  },
  {
    sql_expression: 'SUM("Strong Upside")',
  },
  {
    sql_expression: 'COUNT(Distinct id)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN first_time_fix = 'No' OR first_time_fix = 'false' THEN id END)  || ''",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Cancelled' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'In UAT - Hypercare (QA Pending)' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Deploy Pend on Demo' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT(project))',
  },
  {
    sql_expression:
      "count(case when visibility='INTERNAL' then 1 else NULL end)",
  },
  {
    sql_expression: 'MEDIAN(actual_close_date::DATE - created_date::DATE)',
  },
  {
    sql_expression: "COUNT(CASE WHEN stage = 'active' THEN id END)",
  },
  {
    sql_expression:
      'AVG(CASE WHEN TRY_CAST(ui_cache AS BOOLEAN) THEN TRY_CAST(duration AS DOUBLE) END) as avg_duration_with_cache',
  },
  {
    sql_expression:
      "COUNT(*) FILTER(stage = 'in_development' AND DATE_ADD(CURRENT_DATE, INTERVAL (estimated_effort / 24) DAY) > target_close_date)",
  },
  {
    sql_expression: 'Count(distinct(id))',
  },
  {
    sql_expression:
      "COUNT(Distinct CASE WHEN json_extract_string(stage_json, '$.name') = 'awaiting_product_assist' THEN id END)  || ''",
  },
  {
    sql_expression:
      "AVG(CASE WHEN json_extract_string(stage_json, '$.name') in ('On Hold', 'On-hold', 'On hold') THEN DATEDIFF('minutes', created_date, current_date) END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'ctype__x1260822517949_cfid' IS NULL and subtype in ('partner','supported_employee') THEN id END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-global:metric-set/1' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: "COUNT(CASE WHEN state = 'closed' THEN id ELSE NULL END)",
  },
  {
    sql_expression: 'round(sum(unhealthy)/COUNT(DISTINCT(timestamp)))',
  },
  {
    sql_expression:
      "SUM(CASE WHEN final_component = 'External Loader' THEN duration END)/SUM(CASE WHEN final_component = 'External Loader' THEN count_sync END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN final_component = 'External Extractor' THEN duration END)/SUM(CASE WHEN final_component = 'External Extractor' THEN count_sync END)",
  },
  {
    sql_expression:
      "sum(case when lifecycle_details = 'PENDING_WITH_CUSTOMER' or lifecycle_details = 'PENDING_WITH_ORACLE' then 1 else 0 end)",
  },
  {
    sql_expression: 'sum(healthy)/count(distinct(timestamp))',
  },
  {
    sql_expression: 'SUM(user_code_total_contributions)',
  },
  {
    sql_expression: 'ROUND(AVG(acv))',
  },
  {
    sql_expression: "COALESCE(SUM(amount) FILTER(direction = 'Income'), 0)",
  },
  {
    sql_expression:
      'count(DISTINCT CASE WHEN actual_close_date == record_date THEN ticket END)',
  },
  {
    sql_expression: 'any_value(health_score_latest)',
  },
  {
    sql_expression: 'ROUND(SUM(acv)/COUNT(DISTINCT(opps)),2)',
  },
  {
    sql_expression: 'COUNT(submitted)',
  },
  {
    sql_expression:
      "100.0 * (\n    COUNT(DISTINCT CASE \n        WHEN state != 'closed' AND \n           ( ('don:core:dvrv-us-1:devo/0:tag/2124' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13375' = ANY(tag_ids)) AND \n            state IS NOT NULL \n        THEN id \n    END) \n    / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0)\n\n",
  },
  {
    sql_expression:
      'COUNT(DISTINCT id) FILTER (record_date > target_close_date)',
  },
  {
    sql_expression: 'sum(measure3)',
  },
  {
    sql_expression: 'avg(measure2)',
  },
  {
    sql_expression:
      "100.0 * (\nCOUNT(DISTINCT CASE WHEN state != 'closed' AND 'don:core:dvrv-us-1:devo/0:tag/2124' = ANY(tag_ids) AND state IS NOT NULL THEN id END)\n/ NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0)\n)",
  },
  {
    sql_expression: 'sum(object_version_su)',
  },
  {
    sql_expression:
      "count(distinct CASE WHEN stage_name in ('work_in_progress') THEN ticket END)",
  },
  {
    sql_expression:
      "count(distinct CASE WHEN tnt__escalated is true AND state='open' THEN id END)",
  },
  {
    sql_expression:
      "count(distinct CASE WHEN tnt__escalated is true AND state='in_progress' THEN id END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Capture' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Workspace (J)' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'JIS (J)' THEN id END)  || ''",
  },
  {
    sql_expression: 'ROUND(resolution_time_hours)',
  },
  {
    sql_expression: '(COUNT(ticket_id))',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/3' AND json_extract_string(metric, '$.status') == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression:
      "COUNT(DISTINCT id) - COUNT(CASE WHEN stage_json ->> 'name' IN ( 'On Hold', 'On-hold', 'On hold' ) THEN id END) || ''",
  },
  {
    sql_expression:
      "COUNT( CASE WHEN state != 'closed' and state is not NULL THEN id END)",
  },
  {
    sql_expression:
      "list_aggregate(CAST(json_extract_string(dim_ticket.surveys_aggregation_json, '$[*].minimum') AS integer[]), 'min')",
  },
  {
    sql_expression:
      "SUM(CAST(json_extract(custom_fields, 'tnt__sonar_code_smells') AS INTEGER)) FILTER (CAST(json_extract(custom_fields, 'tnt__sonar_code_smells') AS INTEGER) IS NOT NULL)",
  },
  {
    sql_expression:
      "AVG(DATE_DIFF('minute', created_date::timestamp, first_response_time))",
  },
  {
    sql_expression:
      "COUNT(distinct CASE WHEN state = 'open' THEN conversation END)",
  },
  {
    sql_expression:
      'count(CASE WHEN service_improvement_status is true THEN id END)',
  },
  {
    sql_expression: 'sum(dead_clicks)',
  },
  {
    sql_expression: 'sum(api_hits)',
  },
  {
    sql_expression:
      'array_agg(distinct issue_id) FILTER (issue_id IS NOT NULL)',
  },
  {
    sql_expression: 'SUM(incident_count)',
  },
  {
    sql_expression:
      "(CASE WHEN (opp_close_date <= (MAX(CASE WHEN date_diff('day', current_date(), opp_close_date) < 0 THEN opp_close_date ELSE '1754-08-30' END) OVER ())) THEN (SUM(SUM(actual_amount)) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING)) END)",
  },
  {
    sql_expression:
      'AVG(time_to_open + time_to_review + time_to_approve + time_to_merge)',
  },
  {
    sql_expression: 'SUM("Converted: Consuming")',
  },
  {
    sql_expression: 'SUM("Won")',
  },
  {
    sql_expression: 'SUM("Closed")',
  },
  {
    sql_expression: "COUNT(CASE WHEN state = 'in_progress' THEN id END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state = 'closed' AND stage = 'resolved' THEN id END)",
  },
  {
    sql_expression: 'SUM(total_code_contributions)',
  },
  {
    sql_expression: 'avg(monthly_distinct_revuser_logins)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Ready for Dev' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'in_review' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'In UAT' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Test cases automated' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'On hold' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Ready for QA' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN target_close_quarter = '2025-Q4' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT closed_id) FILTER (final_state = 'deployed')",
  },
  {
    sql_expression:
      'ROUND(AVG(24 * (CAST(ticket_close_date AS DATE) - CAST(ticket_created_date AS DATE))), 1)',
  },
  {
    sql_expression:
      "COUNT(Distinct CASE WHEN json_extract_string(stage_json, '$.name') = 'awaiting_delivery' THEN id END)  || ''",
  },
  {
    sql_expression:
      "SUM(CASE WHEN severity_name = 'Medium' AND COALESCE(total_resolution_breaches, 0) > 0  THEN 1 ELSE 0 END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN final_component = 'DevRev Extractor' THEN duration END)/SUM(CASE WHEN final_component = 'DevRev Extractor' THEN count_sync END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_HUBSPOT' THEN duration END)/SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_HUBSPOT' THEN count_sync END)",
  },
  {
    sql_expression: "count(case when visibility='INTERNAL' then 1  end)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN owner = 'don:identity:dvrv-us-1:devo/1B2GHUnRr:devu/595' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression: 'sum(case when sprint_board_count>0 then 1 else 0 end )',
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN metric_status='hit' then id end)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN sla_stage = 'completed' THEN id END)",
  },
  {
    sql_expression: 'NULLIF(completed_in, 0)',
  },
  {
    sql_expression:
      "case when (SUM(real_amount)) < 0 then format('(${:t,})', @(SUM(real_amount))::INTEGER) else format('${:t,}', (SUM(real_amount))::INTEGER) end",
  },
  {
    sql_expression: "COUNT(DISTINCT closed_id) FILTER (closed_id <> '')",
  },
  {
    sql_expression: 'sum(measure1)',
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER((state = 'open' OR state = 'in_progress') AND CURRENT_DATE<=target_close_date AND DATE_ADD(CURRENT_DATE, INTERVAL (estimated_effort / 24) DAY) > target_close_date)",
  },
  {
    sql_expression:
      'avg(case when timestamp_nsecs >= current_date - 30 then value else 0 end)',
  },
  {
    sql_expression:
      'COALESCE((select value from cod_reconciliation cc2 where cc2.account_id = account_metrics__account and cast(cast(cc2.timestamp_nsecs as timestamp) as date) = cast(current_date - 1 as date) order by timestamp_nsecs desc limit 1),0)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN metric_summary.value ->> 'metric_definition_id' LIKE '%metric_definition/2%' and metric_status = 'hit' THEN id END)/COUNT(CASE WHEN metric_summary.value ->> 'metric_definition_id' LIKE '%metric_definition/2%' and metric_status in ('miss','hit') THEN id END)) * 100",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Quantify Connectivity' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Quantify Connectivity' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM(api_server_error_count)',
  },
  {
    sql_expression: 'COUNT(DISTINCT owned_by_id)',
  },
  {
    sql_expression:
      "CASE WHEN metric_status = 'hit' THEN 'Completed' WHEN metric_status = 'miss' THEN 'Breached' END",
  },
  {
    sql_expression: 'array_distinct(flatten(ARRAY_AGG(issues)))',
  },
  {
    sql_expression: 'max(max_upstream_service_time) / 1000',
  },
  {
    sql_expression: 'avg(value)',
  },
  {
    sql_expression: 'COUNT(DISTINCT primary_part_id )',
  },
  {
    sql_expression:
      'CASE WHEN SUM(ARRAY_LENGTH(next_resp_time_arr) + ARRAY_LENGTH(first_resp_time_arr) + ARRAY_LENGTH(resolution_time_arr)) > 0 THEN 100 - (SUM(CASE WHEN ARRAY_LENGTH(next_resp_time_arr) > 0 THEN total_second_resp_breaches_ever ELSE 0 END + CASE WHEN ARRAY_LENGTH(first_resp_time_arr) > 0 THEN total_first_resp_breaches_ever ELSE 0 END + CASE WHEN ARRAY_LENGTH(resolution_time_arr) > 0 THEN total_resolution_breaches_ever ELSE 0 END) / SUM(ARRAY_LENGTH(next_resp_time_arr) + ARRAY_LENGTH(first_resp_time_arr) + ARRAY_LENGTH(resolution_time_arr))) * 100 ELSE 0 END',
  },
  {
    sql_expression: "COUNT(CASE WHEN state != 'closed' THEN 1 END)",
  },
  {
    sql_expression: 'COUNT(sat_result)',
  },
  {
    sql_expression:
      'SUM(CAST(total_nodes AS INTEGER)) - SUM(CAST(total_nodes AS INTEGER))',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'JIS' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM("Started: Exploring")',
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN status = 'miss' THEN id END)",
  },
  {
    sql_expression: 'AVG(review_time)',
  },
  {
    sql_expression: 'sum(daily_issues_created)',
  },
  {
    sql_expression: 'avg(monthly_distinct_devuser_logins)',
  },
  {
    sql_expression:
      "count(distinct CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '1' THEN id END) || ''",
  },
  {
    sql_expression: "SUM(CASE WHEN source_name = 'Other' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'QA on production' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'On hold' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      'AVG(COUNT(number_deployments)) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
  },
  {
    sql_expression:
      ' CASE  WHEN SUM(ARRAY_LENGTH(next_resp_time_arr) + ARRAY_LENGTH(first_resp_time_arr) + ARRAY_LENGTH(resolution_time_arr)) > 0 THEN  100 - (SUM( CASE WHEN ARRAY_LENGTH(next_resp_time_arr) > 0 THEN COALESCE(total_second_resp_breaches_ever, 0) ELSE 0 END  + CASE WHEN ARRAY_LENGTH(first_resp_time_arr) > 0 THEN COALESCE(total_first_resp_breaches_ever, 0) ELSE 0 END  + CASE WHEN ARRAY_LENGTH(resolution_time_arr) > 0 THEN COALESCE(total_resolution_breaches_ever, 0) ELSE 0 END) / NULLIF(SUM(ARRAY_LENGTH(next_resp_time_arr) + ARRAY_LENGTH(first_resp_time_arr) + ARRAY_LENGTH(resolution_time_arr)), 0)) * 100 ELSE NULL END',
  },
  {
    sql_expression:
      "CASE WHEN ARRAY_LENGTH(ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-01-01', '2024-02-01', '2024-03-01') THEN id END) FILTER (WHERE CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-01-01', '2024-02-01', '2024-03-01') THEN id END IS NOT NULL)) > 0 THEN ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-01-01', '2024-02-01', '2024-03-01') THEN id END) FILTER (WHERE CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-01-01', '2024-02-01', '2024-03-01') THEN id END IS NOT NULL) ELSE NULL END",
  },
  {
    sql_expression: "COUNT(distinct CASE WHEN state = 'closed' THEN id END)",
  },
  {
    sql_expression:
      " select CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL ) AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL ) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 /(COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL )AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL )THEN id END))) ELSE NULL END",
  },
  {
    sql_expression: 'ROUND(AVG(duration_hours))',
  },
  {
    sql_expression:
      'ROUND(AVG(SUM(daily_active_users)) OVER (ORDER BY created_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/ogNAgdmp:metric_set/daqVrrZ9' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/ogNAgdmp:metric_set/1FNrpudPE' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: 'avg(mttr_hours)',
  },
  {
    sql_expression:
      "COUNT( DISTINCT CASE WHEN (source='tickets' and range_state = 'closed') and state = 'closed' THEN id END)",
  },
  {
    sql_expression:
      "CASE WHEN max(curr_p50_latency) > LAG(max(curr_p50_latency)) OVER (ORDER BY max(curr_timestamp) asc) THEN 'Yes' ELSE 'No' END",
  },
  {
    sql_expression:
      'AVG(SUM(IFNULL(contribution, 0))) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
  },
  {
    sql_expression: 'AVG(ticket_count)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN final_component = 'Transformer' THEN duration END)/SUM(CASE WHEN final_component = 'Transformer' THEN count_sync END)",
  },
  {
    sql_expression: 'AVG(all_issue)',
  },
  {
    sql_expression:
      'COUNT(DISTINCT user_id) FILTER(focus_score >= focus_filter_val)',
  },
  {
    sql_expression: 'COUNT(DISTINCT submitter)',
  },
  {
    sql_expression:
      'SUM(SUM(amount)) OVER (PARTITION BY {MEERKAT}.owned_by_id)',
  },
  {
    sql_expression: 'any_value(sprint_end_date)',
  },
  {
    sql_expression: 'SUM(COUNT(*)) OVER (PARTITION BY member_ids)',
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER (     WHERE target_close_date < CURRENT_DATE      AND stage_name != 'Ready for Dev' )",
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='awaiting_development' and 'don:core:dvrv-us-1:devo/0:tag/1606'=ANY(tag_ids) and severity_name='Blocker' and state != 'closed'  AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN sla_stage = 'active' THEN id END)",
  },
  {
    sql_expression:
      "100.0 * (\n    COUNT(DISTINCT CASE \n        WHEN state != 'closed' AND \n           ( 'don:core:dvrv-us-1:devo/0:tag/2129' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13376' = ANY(tag_ids)) AND \n            state IS NOT NULL  THEN id  END) \n    / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0))\n\n",
  },
  {
    sql_expression:
      "100.0 * (\nCOUNT(DISTINCT CASE WHEN state != 'closed' AND ('don:core:dvrv-us-1:devo/0:tag/2124' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13375' = ANY(tag_ids) )AND state IS NOT NULL THEN id END)\n/ NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0",
  },
  {
    sql_expression: 'ANY_VALUE(ARRAY_LENGTH(issues))',
  },
  {
    sql_expression: 'avg(measure1)',
  },
  {
    sql_expression: 'MIN(duration_seconds)',
  },
  {
    sql_expression: 'COALESCE(AVG(total_first_resp_time), 0)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/1EYSCQzBT' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Capture NG' THEN id END)  || ''",
  },
  {
    sql_expression: "COUNT(id) || ''",
  },
  {
    sql_expression:
      "COUNT( distinct CASE WHEN state = 'closed' THEN conversation END)",
  },
  {
    sql_expression: 'any_value(customer_type)',
  },
  {
    sql_expression:
      "(COUNT(CASE  WHEN metric_status = 'hit' THEN id END)/count(id)) * 100",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN status = 'hit' then id end)/Count(id) *100",
  },
  {
    sql_expression: 'MAX(modified_date)',
  },
  {
    sql_expression: 'SUM(returning_users_count)',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN status == 'miss' THEN conv_id END))",
  },
  {
    sql_expression: 'sum(hits)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/NlEqpWWq' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/NlEqpWWq' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      'COUNT(DISTINCT user_id) FILTER(user_percentage < 0.6 AND user_percentage >= 0.05)',
  },
  {
    sql_expression: 'SUM(user_percentage)',
  },
  {
    sql_expression:
      '(SUM(contributions) / ANY_VALUE(avg_user_contrib)) / SUM(user_percentage)',
  },
  {
    sql_expression: "AVG(DATE_DIFF('minute', issue_date, issue_closed_date))",
  },
  {
    sql_expression:
      "COUNT(distinct CASE WHEN issue_subtype = 'njuvewk7oxvgs4din5zgkltborygc43tnfqy4ltomx2f6mjrgmwtav3jonzvkzltfzss243xobwg64tu' and range_state = 'closed'  THEN id END)",
  },
  {
    sql_expression: "CONCAT(ROUND(IFNULL(AVG(breached), 0) * 100, 2), '%')",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Hostfuse' THEN ticket END)  || ''",
  },
  {
    sql_expression: 'MAX(stage_ordinal)',
  },
  {
    sql_expression: "COUNT(distinct id)  || ''",
  },
  {
    sql_expression:
      "count(distinct CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '4' THEN id END) || ''",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN sla_has_breached = false THEN id WHEN status = 'hit' THEN id END)/count(id)) * 100",
  },
  {
    sql_expression:
      "SUM(CASE WHEN source_name = 'Events: Meetups' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Scope Approved' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'in_deployment' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'duplicate' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Ready for QA' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression: 'MAX(affected_area) * SUM(total_changes)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed' AND 'don:core:dvrv-us-1:devo/0:tag/508'= ANY(tag_ids) AND state IS NOT NULL THEN id END)\n",
  },
  {
    sql_expression: 'SUM(CASE WHEN fy_year = 2025 THEN acv ELSE 0 END)',
  },
  {
    sql_expression: 'COUNT(DISTINCT account_id)',
  },
  {
    sql_expression:
      "(select CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL ) AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL ) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 /(COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL )AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL )THEN id END))) ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/tmD9r4ID:metric_set/1A3ZQnUqS' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state ='closed' AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT devu_id)',
  },
  {
    sql_expression: 'ANY_VALUE(skills)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT id) FILTER (final_state = 'in_progress' AND active_id IS NULL AND id <> '')",
  },
  {
    sql_expression:
      "count(distinct case when ticket_linked ='yes' then id end)",
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER (WHERE stage_name IN (   'Sprint Planning',    'Ready for Dev',    'In Dev',    'Code Review') )",
  },
  {
    sql_expression: 'SUM(article_upvotes)',
  },
  {
    sql_expression: 'count(node_id)',
  },
  {
    sql_expression:
      "count(distinct CASE WHEN tnt__escalated is true AND state='closed' THEN id END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'X-Platform' THEN id END)  || ''",
  },
  {
    sql_expression:
      "SUM(CASE WHEN type like '%attachments%' THEN created_in_devrev+created_in_external+updated_in_devrev+updated_in_external END)",
  },
  {
    sql_expression: 'COUNT(completed_issue)',
  },
  {
    sql_expression: 'COUNT(created_date)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) THEN id END)",
  },
  {
    sql_expression: 'round(avg(mttr_hours))',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__follow_up' IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state !='closed' AND (primary_part_id = ' ' OR primary_part_id IS NULL) THEN id END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/1' AND (json_extract_string(metric, '$.status') == 'hit' OR json_extract_string(metric, '$.status') == 'in_progress') THEN ticket_id END)*100/COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/1' THEN ticket_id END))",
  },
  {
    sql_expression:
      "SUM(CASE WHEN id != 'don:identity:dvrv-us-1:devo/0:account/1F5aezyGo' THEN active_users ELSE 0 END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed'  AND state IS NOT NULL  AND sla_stage!='' AND sla_stage NOT NULL THEN id END )",
  },
  {
    sql_expression: "COUNT(CASE WHEN status = 'miss' THEN id END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/1EYSCQzBT' THEN metric_set_value END)",
  },
  {
    sql_expression:
      "case WHEN actual_close_date > created_date THEN date_diff('minutes', created_date, actual_close_date) ELSE null END ",
  },
  {
    sql_expression: 'SUM(all_issue)',
  },
  {
    sql_expression: 'sum(rage_clicks)',
  },
  {
    sql_expression: 'avg(min_value)',
  },
  {
    sql_expression:
      "100.0 * COUNT(DISTINCT CASE WHEN CURRENT_DATE - CAST(created_date AS DATE) > 5 AND state != 'closed' AND state IS NOT NULL THEN id END) / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0)",
  },
  {
    sql_expression: 'AVG(awaiting_product_assist_duration_days)',
  },
  {
    sql_expression: 'ANY_VALUE(owned_by_ids)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/21JF3hmR' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'SUM(second_half_contributions)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'WorkFlow' THEN ticket END)  || ''",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN sla_has_breached = false THEN id WHEN metric_status = 'hit' THEN id END)/count(DISTINCT id)) * 100",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Sprint Planning' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Internal UAT' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Scope Approved' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'wont_fix' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'External scope approval' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Short Term Backlog' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Scoping' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed' and DATE_TRUNC('day', record_hour) = DATE_TRUNC('day', created_date) THEN id END)",
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN stage = 'queued' THEN id END)",
  },
  {
    sql_expression: 'COUNT(number_deployments)',
  },
  {
    sql_expression: 'COUNT(1)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed' AND 'don:core:dvrv-us-1:devo/0:tag/508' = ANY(tag_ids) AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: "(count(CASE WHEN status='hit' THEN id END)/count(*))*100",
  },
  {
    sql_expression:
      "printf('%.2f%% (%.2f%%)', SUM(CAST(COALESCE(max_lines_covered, '0') AS DOUBLE)) / SUM(CAST(COALESCE(max_lines_total, '0') AS DOUBLE)) * 100, (SUM(CAST(COALESCE(max_lines_covered, '0') AS DOUBLE)) / SUM(CAST(COALESCE(max_lines_total, '0') AS DOUBLE))) * 100 - (SUM(CAST(COALESCE(min_lines_covered, '0') AS DOUBLE)) / SUM(CAST(COALESCE(min_lines_total, '0') AS DOUBLE))) * 100)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN target_close_quarter = '2025-Q1' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "CASE WHEN ARRAY_LENGTH(ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-04-01', '2024-05-01', '2024-06-01') THEN id END) FILTER (WHERE CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-04-01', '2024-05-01', '2024-06-01') THEN id END IS NOT NULL)) > 0 THEN ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-04-01', '2024-05-01', '2024-06-01') THEN id END) FILTER (WHERE CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-04-01', '2024-05-01', '2024-06-01') THEN id END IS NOT NULL) ELSE NULL END",
  },
  {
    sql_expression:
      "COUNT (DISTINCT CASE WHEN (FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT))) IN (4, 5) THEN id END) * 100.0 / COUNT (DISTINCT CASE WHEN (FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT))) IN (1, 2, 3, 4, 5) THEN id END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/ogNAgdmp:metric_set/16Nr9jwny' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/ogNAgdmp:metric_set/16Nr9jwny' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: 'SUM(referred_count)',
  },
  {
    sql_expression:
      "CASE WHEN max(curr_p95_latency) > LAG(max(curr_p95_latency)) OVER (ORDER BY max(curr_timestamp) asc) THEN 'Yes' ELSE 'No' END",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state != 'closed' AND state IS NOT NULL AND TRIM('\"' FROM JSON_EXTRACT(owned_by_ids, '$[0]')) IN ('don:identity:dvrv-us-1:devo/1cog4dkFxx:svcacc/6') THEN id END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN severity_name = 'High' AND COALESCE(total_resolution_breaches, 0) > 0  THEN 1 ELSE 0 END)",
  },
  {
    sql_expression: 'max(issues_count)',
  },
  {
    sql_expression:
      "CASE WHEN AVG(score_value) <= (1/3) THEN 'at_risk' WHEN AVG(score_value) >= (1/3) THEN 'healthy' ELSE 'neutral' END",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/tmD9r4ID:metric_set/REgRIsII' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'AVG(cycle_time_value)',
  },
  {
    sql_expression:
      'ROUND( SUM( CASE WHEN stage_json LIKE \'\'%"stage_id":"don:core:dvrv-us-1:devo/0:custom_stage/27"%\'\' THEN 1 ELSE 0 END) / NULLIF( SUM( CASE WHEN stage_json LIKE \'\'%"stage_id":"don:core:dvrv-us-1:devo/0:custom_stage/27"%\'\' OR stage_json LIKE \'\'%"stage_id":"don:core:dvrv-us-1:devo/0:custom_stage/5"%\'\' THEN 1 ELSE 0 END), 0), 3)*100',
  },
  {
    sql_expression: 'avg(avg_issues)',
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER (WHERE      (product_effort_planned IS NULL)      OR (target_start_date IS NULL OR target_start_date = '')      OR (target_close_date IS NULL OR target_close_date = ''))",
  },
  {
    sql_expression: "COALESCE(SUM(amount) FILTER(direction = 'Expense'), 0)",
  },
  {
    sql_expression: 'any_value(customer_score_latest)',
  },
  {
    sql_expression: 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY merge_time)',
  },
  {
    sql_expression:
      "AVG(AVG(DATE_SUB('second', pr_reviewed_time, pr_approved_time))) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)",
  },
  {
    sql_expression:
      'PERCENTILE_CONT(0.9) WITHIN GROUP ( ORDER BY avg_time_to_deploy)',
  },
  {
    sql_expression:
      "100.0 * (\n    COUNT(DISTINCT CASE \n        WHEN state != 'closed' AND \n           ( ('don:core:dvrv-us-1:devo/0:tag/2124' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13375' = ANY(tag_ids)) AND \n            state IS NOT NULL \n        THEN id \n    END) \n    / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0))\n\n",
  },
  {
    sql_expression:
      "ANY_VALUE(datediff('day', dim_ticket.modified_date, current_date)) || ' days'",
  },
  {
    sql_expression: 'COUNT(id_sh)',
  },
  {
    sql_expression: "count(case when state != 'closed' then id end)",
  },
  {
    sql_expression:
      'AVG(AVG(COALESCE(time_to_open, 0) + COALESCE(time_to_review, 0) + COALESCE(time_to_approve, 0) + COALESCE(time_to_merge, 0))) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
  },
  {
    sql_expression:
      "((COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '6 day' AND CURRENT_DATE THEN rev_uid ELSE NULL END)- COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '13 day' AND CURRENT_DATE - INTERVAL '6 day'THEN rev_uid ELSE NULL END))/COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '6 day' AND CURRENT_DATE THEN rev_uid ELSE NULL END))*100",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN metric_summary.value ->> 'metric_definition_id' LIKE '%metric_definition/1%' and metric_status = 'hit' THEN id END)/COUNT(CASE WHEN metric_summary.value ->> 'metric_definition_id' LIKE '%metric_definition/1%' and metric_status in ('miss','hit') THEN id END)) * 100",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Quantify Platform' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'X-Conversa' THEN id END)  || ''",
  },
  {
    sql_expression: 'COUNT(DISTINCT id)',
  },
  {
    sql_expression:
      'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY time_to_open + time_to_review + time_to_approve + time_to_merge)',
  },
  {
    sql_expression: "COUNT(CASE WHEN severity = '5' THEN id END) || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN metric_status = 'hit' then id end)/Count(DISTINCT id) * 100",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN metric_status = 'in_progress' then id end)",
  },
  {
    sql_expression:
      "ROUND(COUNT(DISTINCT CASE WHEN status = 'miss' THEN conv_id END)/COUNT(DISTINCT owned_by_id))",
  },
  {
    sql_expression:
      "AVG(CASE WHEN state='closed' AND actual_close_date > created_date THEN DATE_DIFF('minutes',created_date, actual_close_date ) END)",
  },
  {
    sql_expression: 'min(min_upstream_service_time) / 1000',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/7Gtyici3' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT user_id) FILTER(user_percentage < 0.05)',
  },
  {
    sql_expression:
      "count(distinct id where JSON_VALUE(stage_json, '$.name') = 'triage' )",
  },
  {
    sql_expression: 'count(distinct id_su)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'U-Analyze' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM("Discovered: Engage later")',
  },
  {
    sql_expression: 'SUM(total_events)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN first_time_fix = 'Yes' OR first_time_fix = 'true' THEN id END)/COUNT( id ))*100",
  },
  {
    sql_expression: 'SUM(sum_csat_rating)/SUM(count_csat_rating)',
  },
  {
    sql_expression: 'median(age)',
  },
  {
    sql_expression:
      "date_diff('minutes', created_date, cast(current_timestamp as TIMESTAMP)) ",
  },
  {
    sql_expression:
      "count(distinct CASE WHEN te is true AND state='open' THEN ticket END)",
  },
  {
    sql_expression: "count(CASE WHEN status='hit' THEN id END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Ready For Deployment' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Solutioning Approval Pending' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/Ds2FRuuX' THEN curr_metric_set_value END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT(session_id, request_id))',
  },
  {
    sql_expression: 'sum(sales_data_dr.amount)',
  },
  {
    sql_expression: "SUM(CASE WHEN retry_count = '2' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression:
      "AVG(datediff('hour', LEAST(record_date, identified_at), resolved_at))",
  },
  {
    sql_expression:
      "COUNT(case when custom_fields->>'ctype__support_priority' = 'Medium' then  id end)",
  },
  {
    sql_expression:
      'AVG(CASE WHEN NOT TRY_CAST(ui_cache AS BOOLEAN) THEN TRY_CAST(duration AS DOUBLE) END)',
  },
  {
    sql_expression: 'sum(total_contributions_on_part)',
  },
  {
    sql_expression:
      " AVG(FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)))",
  },
  {
    sql_expression: 'ANY_VALUE(current_sprint)',
  },
  {
    sql_expression:
      '(COUNT(DISTINCT CASE WHEN is_spam THEN ticket_id END))*100/(COUNT(DISTINCT ticket_id))',
  },
  {
    sql_expression:
      "SUM(SUM(CASE WHEN CAST(JSON_EXTRACT_STRING(annual_contract_value,'$.amount') AS INTEGER) = 0 THEN 25000 ELSE CAST(JSON_EXTRACT_STRING(annual_contract_value,'$.amount') AS INTEGER) END)) OVER (PARTITION BY {MEERKAT}.owner)",
  },
  {
    sql_expression: "sum(case when priority = 'P4' then 1 end)",
  },
  {
    sql_expression: 'SUM(contributions)',
  },
  {
    sql_expression: 'MEDIAN(completed_in)',
  },
  {
    sql_expression:
      'count(DISTINCT CASE WHEN created_date == record_date THEN ticket END)',
  },
  {
    sql_expression: 'count(distinct primary_part_id)',
  },
  {
    sql_expression: 'count(distinct issue)',
  },
  {
    sql_expression:
      '((sum(healthy)/COUNT(DISTINCT(timestamp))))/((sum(total_expected)/COUNT(DISTINCT(timestamp))))*100',
  },
  {
    sql_expression: 'SUM(total_duration_ms)/(SUM(total_views)*1000)',
  },
  {
    sql_expression: 'sum(cast(replacement_count as integer))',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN severity_name = 'Blocker' THEN id END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT id_devo)',
  },
  {
    sql_expression:
      'case when any_value(dat) <= day(current_date) then count(distinct(cm_id)) end',
  },
  {
    sql_expression:
      "CASE WHEN severity = 7 AND actual_close_date > created_date THEN date_diff('minutes', created_date, actual_close_date) ELSE null END",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'WorkFlow (J)' THEN id END)  || ''",
  },
  {
    sql_expression:
      'case when sum(api_failure_count) > 0 then sum(api_server_error_count) else null end',
  },
  {
    sql_expression: 'AVG(total_resolution_time)',
  },
  {
    sql_expression: 'SUM(acv)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN stage = 'completed' THEN id END)* 100.0 / COUNT(*))",
  },
  {
    sql_expression:
      "SUM(CAST(json_extract(custom_fields, 'tnt__sonar_vulnerabilities') AS INTEGER)) FILTER (CAST(json_extract(custom_fields, 'tnt__sonar_vulnerabilities') AS INTEGER) IS NOT NULL)",
  },
  {
    sql_expression: 'any_value(record_date)',
  },
  {
    sql_expression: 'COUNT(ticket)',
  },
  {
    sql_expression: 'COUNT(state)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/2JxwhYm6' THEN metric_set_value END)",
  },
  {
    sql_expression: 'AVG(reporter_count)',
  },
  {
    sql_expression: 'AVG(api_hits)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/18aD8RPm' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "((SUM(CAST(total_nodes AS INTEGER))) - (SUM(case when nodes = 'nan' then 0 else CAST(nodes AS INTEGER) end)))",
  },
  {
    sql_expression:
      "AVG(DATE_DIFF('minute', issue_created_date, issue_actual_close_date))",
  },
  {
    sql_expression:
      "COUNT(DISTINCT id) FILTER (state IN ('open','in_progress'))",
  },
  {
    sql_expression:
      'CASE WHEN SUM(total_responses) != 0 THEN SUM(sum_rating)/SUM(total_responses) ELSE 0 END',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/21JF3hmR' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: "COUNT(CASE WHEN stage = 'breached' then id end)",
  },
  {
    sql_expression: 'median(total_resolution_time)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN stage in ('On Hold', 'On-hold', 'On hold') THEN DATEDIFF('day', created_date, current_date) END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'U-Self Serve' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM("Pipeline")',
  },
  {
    sql_expression: 'any_value(id)',
  },
  {
    sql_expression:
      "list_aggregate((CAST(json_extract_string(surveys_aggregation_json, '$[*].average') as integer[])), 'sum')/len((CAST(json_extract_string(surveys_aggregation_json, '$[*].average') as integer[])))",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Code Review' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'backlog' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'duplicate' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression: "SUM(CASE WHEN page = 'import' THEN actions_count END)",
  },
  {
    sql_expression: 'ANY_VALUE(avg_elapsed_times)',
  },
  {
    sql_expression:
      "ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-07-01', '2024-08-01', '2024-09-01') THEN id END) FILTER (WHERE id IS NOT NULL)",
  },
  {
    sql_expression:
      "COUNT(case when custom_fields->>'ctype__support_priority' = 'High' then  id end)",
  },
  {
    sql_expression:
      'AVG(CASE WHEN NOT TRY_CAST(ui_cache AS BOOLEAN) THEN TRY_CAST(duration AS DOUBLE) END) as avg_duration_without_cache',
  },
  {
    sql_expression: 'SUM(total_resolution_breaches)',
  },
  {
    sql_expression: 'ANY_VALUE(final_state)',
  },
  {
    sql_expression: 'max(curr_p90_latency) / 1000',
  },
  {
    sql_expression:
      "CASE WHEN customer_type = 'paying' THEN 'Paying' ELSE 'Not Paying' END",
  },
  {
    sql_expression: 'sum(updated)',
  },
  {
    sql_expression: 'sum(duration)/SUM(count_sync)',
  },
  {
    sql_expression:
      "(SUM(weighted_contribution) FILTER(source_part_id LIKE '%enhancement%')) / SUM(weighted_contribution) * 100",
  },
  {
    sql_expression:
      "count(case when visibility='EXTERNAL' and user_id like '%revu%' then 1 end)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN owner = 'don:identity:dvrv-us-1:devo/1B2GHUnRr:devu/11' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression: 'SUM(cnt)',
  },
  {
    sql_expression: 'sum(is_rage)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/tmD9r4ID:metric_set/1A3ZQnUqS' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      " CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL ) AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL ) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 /(COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL )AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL )THEN id END))) ELSE NULL END",
  },
  {
    sql_expression: 'AVG(avg_bottom)',
  },
  {
    sql_expression: 'AVG(cycle_time_val)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/1EYSCQzBT' THEN metric_set_value END)",
  },
  {
    sql_expression: 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY review_time)',
  },
  {
    sql_expression: 'floor(avg(weekly_distinct_devuser_logins))',
  },
  {
    sql_expression:
      'ARRAY_AGG(DISTINCT id) FILTER (WHERE target_close_date < CURRENT_DATE )',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER (     WHERE target_close_date < CURRENT_DATE      AND stage_name != 'Ready for Dev' )",
  },
  {
    sql_expression: 'avg(measure4)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN stage IN ('On Hold', 'On-hold', 'On hold') THEN ticket  END)",
  },
  {
    sql_expression: 'count(distinct(pm_id))',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Assist' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN json_extract_string(stage_json, '$.name') IN ('On Hold', 'On-hold', 'On hold') THEN ticket  END)",
  },
  {
    sql_expression: "COUNT(CASE WHEN severity = '8' THEN id END) || ''",
  },
  {
    sql_expression:
      "SUM(CAST(json_extract(custom_fields, 'tnt__sonar_security_hotspots') AS INTEGER)) FILTER (CAST(json_extract(custom_fields, 'tnt__sonar_security_hotspots') AS INTEGER) IS NOT NULL)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN status = 'miss' THEN sla_id END)/count(sla_id)) * 100",
  },
  {
    sql_expression: 'avg(score_value)',
  },
  {
    sql_expression:
      'array_length(array_distinct(flatten(ARRAY_AGG(customers))))',
  },
  {
    sql_expression: 'ANY_VALUE(average_ticket_count_per_day)',
  },
  {
    sql_expression:
      "COUNT(distinct CASE WHEN issue_subtype = 'njuvewk7oxvgs4din5zgkltborygc43tnfqy4ltomx2f6mjrgmwtav3jonzvkzltfzss243xobwg64tu' and range_state = 'open' THEN id END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT rev_oid)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN actual_close_date > created_date THEN DATEDIFF('day', created_date, actual_close_date) END)",
  },
  {
    sql_expression:
      'ROUND(SUM(daily_active_users)/COUNT(DISTINCT(created_date)), 0)',
  },
  {
    sql_expression: 'AVG(com_result)',
  },
  {
    sql_expression: 'COALESCE(AVG(first_resp_time), 0)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Quantify' THEN id END)  || ''",
  },
  {
    sql_expression: 'AVG(time_to_approve)',
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN stage = 'resolved' THEN id END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'In Progress/ Scoping' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'wont_fix' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "printf('%.2f%%',SUM(CAST(total_prs_with_tests as DOUBLE)) / SUM(CAST(total_prs as DOUBLE)) * 100)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/10VEbARzh' THEN metric_set_value END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) and (state = 'open' or state = 'in_progress' or state = 'closed') THEN id END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/1EYSCQzBT' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN page = 'Details page' THEN visits_count END)",
  },
  {
    sql_expression:
      "ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-04-01', '2024-05-01', '2024-06-01') THEN id END) FILTER (WHERE id IS NOT NULL)",
  },
  {
    sql_expression:
      "printf('%.0fd %.0fh %.0fm (%+.2f%%)',floor(new_final_val / 86400), floor((new_final_val % 86400) / 3600), floor((new_final_val % 3600) / 60), ((new_final_val - old_final_val) / old_final_val) * 100)",
  },
  {
    sql_expression: 'SUM(issue_count)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN current_date - created_date > interval '5' day THEN id END)",
  },
  {
    sql_expression:
      "sum(case when lifecycle_details = 'PENDING_WITH_ORACLE' then 1 else 0 end)",
  },
  {
    sql_expression:
      "(CASE WHEN ({MEERKAT}.opp_close_date >= (MAX(CASE WHEN date_diff('day', current_date(), {MEERKAT}.opp_close_date) < 0 THEN {MEERKAT}.opp_close_date ELSE '1754-08-30' END) OVER ())) THEN ((SUM(CASE WHEN SUM(forecast_amount) IS NOT NULL THEN SUM(forecast_amount) ELSE 0 END) OVER (ORDER BY {MEERKAT}.opp_close_date ROWS UNBOUNDED PRECEDING) + (SUM(CASE WHEN SUM(actual_amount) IS NOT NULL THEN SUM(actual_amount) ELSE 0 END) OVER (ORDER BY {MEERKAT}.opp_close_date ROWS UNBOUNDED PRECEDING)))) END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN (CAST(JSON_EXTRACT_STRING(annual_contract_value,'$.amount') AS INTEGER) = 0 OR CAST(JSON_EXTRACT_STRING(annual_contract_value,'$.amount') AS INTEGER) IS NULL) THEN 25000 ELSE CAST(JSON_EXTRACT_STRING(annual_contract_value,'$.amount') AS INTEGER) END)",
  },
  {
    sql_expression:
      "ARRAY_AGG( DISTINCT id) FILTER(WHERE target_close_date::DATE < CURRENT_DATE AND stage_name != 'Ready for Dev')",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state != 'closed' AND state IS NOT NULL AND TRIM('\"' FROM JSON_EXTRACT(owned_by_ids, '$[0]')) IN ('don:identity:dvrv-us-1:devo/eFlrN4hu:svcacc/3') THEN id END)",
  },
  {
    sql_expression:
      '100*sum(case when total_issues>0 then 1 else 0 end)/count(*)',
  },
  {
    sql_expression: 'ROUND(AVG(DISTINCT distinct_watcher_count))',
  },
  {
    sql_expression: 'COUNT(DISTINCT user_id) FILTER (active_user IS NULL)',
  },
  {
    sql_expression: 'count(sync_type_su)',
  },
  {
    sql_expression:
      "(SELECT CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(resolution_time_arr) > 0) AND (total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 / (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(resolution_time_arr) > 0) AND (total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL) THEN id END))) ELSE NULL END)",
  },
  {
    sql_expression: 'avg(prev_score_value)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN severity_name = 'Low' AND actual_close_date > created_date THEN DATEDIFF('day', created_date, actual_close_date) END)",
  },
  {
    sql_expression:
      'cast(percentile_cont(0.90) within group (order by cast(duration as int)) as int)',
  },
  {
    sql_expression: 'AVG(growth_percentage)',
  },
  {
    sql_expression: "AVG(DATE_DIFF('minute', created_date, actual_close_date))",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/1' AND (json_extract_string(metric, '$.status') == 'hit' OR json_extract_string(metric, '$.status') == 'in_progress') THEN ticket_id END)*100/COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/1' THEN ticket_id END))",
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER (WHERE json_extract_path_text(stage_json, 'name') IN ('Internal UAT', 'QA on production', 'Ready For Deployment', 'In UAT', 'In UAT - Hypercare (QA Pending)', 'Live', 'Completed'))",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state !='closed' AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: '(COUNT(CASE WHEN owned_by_ids IS NULL THEN conv_id END))',
  },
  {
    sql_expression:
      "ROUND(COUNT(DISTINCT CASE WHEN state != 'closed' THEN id END)/COUNT(DISTINCT owned_by_id))",
  },
  {
    sql_expression: 'SUM(completed_issue)',
  },
  {
    sql_expression: 'count(distinct account_id)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'JIA' THEN ticket END)  || ''",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'in_review' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Solutioning Approved' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/LzS6CEY7' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='accepted' and 'don:core:dvrv-us-1:devo/0:tag/508'=ANY(tag_ids) and severity_name='Blocker' AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN work_status = 'open' THEN total_estimated_effort_hours ELSE 0 END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT id) FILTER (final_state = 'in-progress' AND active_id IS NULL)",
  },
  {
    sql_expression: 'COUNT( id )',
  },
  {
    sql_expression:
      'AVG(CASE WHEN TRY_CAST(ui_cache AS BOOLEAN) THEN TRY_CAST(duration AS DOUBLE) END)',
  },
  {
    sql_expression: 'ANY_VALUE(total_file_count)',
  },
  {
    sql_expression:
      'LAG(max(curr_p90_latency)) OVER (ORDER BY max(curr_timestamp) asc) / 1000',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN primary_part_id = 'don:core:dvrv-us-1:devo/1cog4dkFxx:product/28'  THEN id END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN final_component = 'Orchestrator' THEN duration END)/SUM(CASE WHEN final_component = 'Orchestrator' THEN count_sync END)",
  },
  {
    sql_expression: 'sum(undelivered)/count(distinct(timestamp))',
  },
  {
    sql_expression: '100 - any_value(health_score_latest)',
  },
  {
    sql_expression:
      "AVG(AVG(DATE_SUB('second', first_commit_time, pr_opened_time))) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)",
  },
  {
    sql_expression: 'sum(measure4)',
  },
  {
    sql_expression:
      "((SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health) AND node_status IN ('Customer','HEN'))/(SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health)))*100",
  },
  {
    sql_expression:
      "CASE WHEN severity = 5 AND actual_close_date > created_date THEN date_diff('minutes', created_date, actual_close_date) ELSE null END",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'HostFuse (J)' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN array_length(responses, 1) < 2 AND stage = 'resolved' THEN 1 END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/3' AND (json_extract_string(metric, '$.status') == 'hit' OR json_extract_string(metric, '$.status') == 'in_progress') THEN ticket_id END)*100/COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/3' THEN ticket_id END))",
  },
  {
    sql_expression: 'AVG(TRY_CAST(duration AS DOUBLE))',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN metric_status = 'hit' then id end)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN metric_status = 'hit' THEN id END)* 100.0 / COUNT(*))",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/2' AND (json_extract_string(metric, '$.status') == 'hit' OR json_extract_string(metric, '$.status') == 'in_progress') THEN ticket_id END)*100/COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/2' THEN ticket_id END))",
  },
  {
    sql_expression:
      "100.0 * (COUNT(DISTINCT CASE WHEN subtype = 'Onboarding' AND state = 'closed' AND state IS NOT NULL THEN id END) / NULLIF(COUNT(DISTINCT CASE WHEN subtype = 'Onboarding' AND state IS NOT NULL THEN id END), 0))",
  },
  {
    sql_expression: 'array_agg(distinct issue_id)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN first_time_fix = 'Yes' OR first_time_fix = 'true' THEN id END)/COUNT(id))*100",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN json_extract_string(stage_json, '$.name') in ('On Hold', 'On-hold', 'On hold') THEN ticket END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state='closed' AND actual_close_date > created_date THEN ticket END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '2' THEN id END) || ''",
  },
  {
    sql_expression:
      "(COUNT (DISTINCT CASE WHEN  tnt__first_time_fix = 'Yes' OR tnt__first_time_fix = 'true' THEN id END)/COUNT(distinct id))*100",
  },
  {
    sql_expression:
      'FLOOR((SUM(total_time_spent_ms) / COUNT(DISTINCT id)) / (60 * 1000))',
  },
  {
    sql_expression: 'array_length(array_distinct(flatten(ARRAY_AGG(tickets))))',
  },
  {
    sql_expression: 'ANY_VALUE(activity_date)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/Syg7eazB' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: 'SUM("Converted: Renewing")',
  },
  {
    sql_expression:
      'SUM(COUNT(*)) OVER (ORDER BY created_date ROWS UNBOUNDED PRECEDING)',
  },
  {
    sql_expression:
      'SUM(SUM(forecast_amount)) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING)',
  },
  {
    sql_expression:
      'count(distinct CASE WHEN needs_response = true THEN id END)',
  },
  {
    sql_expression: 'sum(daily_distinct_devuser_logins)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN acc_billing_country = 'EMEA' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Ready For Deployment' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Problem Statement review' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Designing' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Long Term Backlog' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'QA on dev' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'High Level Solutioning' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression: 'SUM(CAST(total_prs as INT))',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/6cqTR14h' THEN metric_set_value END)",
  },
  {
    sql_expression: 'SUM(usage_count)',
  },
  {
    sql_expression:
      "AVG(DATE_DIFF('minute', COALESCE(custom_fields ->> 'ctype__created_at_cfid', custom_fields ->> 'ctype__opened_at_cfid', created_date)::TIMESTAMP, actual_close_date))",
  },
  {
    sql_expression: 'ANY_VALUE(ticket_count_ratio)',
  },
  {
    sql_expression: 'COUNT(issue_id)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN state='open' THEN DATEDIFF('minutes', created_date,current_date) END)",
  },
  {
    sql_expression:
      'LAG(max(curr_p50_latency)) OVER (ORDER BY max(curr_timestamp) asc) / 1000',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-global:metric-set/123' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'SUM(IFNULL(contribution, 0))',
  },
  {
    sql_expression: 'SUM(COUNT(*)) OVER (PARTITION BY {MEERKAT}.member_ids)',
  },
  {
    sql_expression:
      "count(distinct case when category = 'healthy' then account_id else null end)",
  },
  {
    sql_expression:
      'COUNT( DISTINCT CASE WHEN target_close_date > sprint_end_date THEN id ELSE NULL END)',
  },
  {
    sql_expression:
      "ROUND(100.0 * COUNT( DISTINCT CASE WHEN stage_name in ('Sprint Planning',   'Ready for Dev',   'In Dev',   'Code Review',   'Internal UAT',   'In UAT',   'Live',   'QA on production',   'Test cases automated',   'Completed') THEN id ELSE NULL END) / NULLIF(COUNT( DISTINCT id), 0), 2)",
  },
  {
    sql_expression:
      "CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL ) AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL ) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 /(COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL )AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL )THEN id END))) ELSE NULL END",
  },
  {
    sql_expression: 'AVG(mtbf_secs)',
  },
  {
    sql_expression:
      "ROUND(100.0 * COUNT( DISTINCT CASE WHEN stage_name = 'Ready for Dev' THEN id ELSE NULL END) / NULLIF(COUNT( DISTINCT id), 0), 2)",
  },
  {
    sql_expression: 'floor(avg(weekly_distinct_revuser_logins))',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER (WHERE DATEDIFF('day', created_date, CURRENT_DATE) > 60)",
  },
  {
    sql_expression: 'SUM(article_downvotes)',
  },
  {
    sql_expression:
      'COUNT(DISTINCT id) FILTER (record_date < target_close_date)',
  },
  {
    sql_expression:
      "((COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '6 day' AND CURRENT_DATE THEN rev_oid ELSE NULL END)- COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '13 day' AND CURRENT_DATE - INTERVAL '6 day'THEN rev_oid ELSE NULL END))/COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '6 day' AND CURRENT_DATE THEN rev_oid ELSE NULL END))*100",
  },
  {
    sql_expression:
      "((COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '29 day' AND CURRENT_DATE THEN rev_uid ELSE NULL END)- COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '59 day' AND CURRENT_DATE - INTERVAL '29 day'THEN rev_uid ELSE NULL END))/COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '29 day' AND CURRENT_DATE THEN rev_uid ELSE NULL END))*100",
  },
  {
    sql_expression:
      'COALESCE((select value from trip_count cc2 where cc2.account_id = account_metrics__account and cast(cast(cc2.timestamp_nsecs as timestamp) as date) = cast(current_date - 1 as date) order by timestamp_nsecs desc limit 1),0)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Q Sales' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Self Serve' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM(api_calls_count)',
  },
  {
    sql_expression:
      'CASE WHEN SUM(ARRAY_LENGTH(first_resp_time_arr)) + SUM(ARRAY_LENGTH(next_resp_time_arr)) + SUM(ARRAY_LENGTH(resolution_time_arr)) > 0 THEN 100 - ((SUM(total_first_resp_breaches_ever) + SUM(total_resolution_breaches_ever) + SUM(total_second_resp_breaches_ever)) / (SUM(ARRAY_LENGTH(first_resp_time_arr)) + SUM(ARRAY_LENGTH(next_resp_time_arr)) + SUM(ARRAY_LENGTH(resolution_time_arr)))) ELSE NULL END',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed' THEN datediff('day',created_date,current_date) END)",
  },
  {
    sql_expression: "(COUNT(CASE WHEN status = 'miss' THEN id END))",
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER (WHERE json_extract_path_text(stage_json, 'name') = 'Live' AND subtype = 'jira_shipsy.atlassian.net_core_issues.bug')",
  },
  {
    sql_expression:
      "CASE WHEN dim_opportunity.value IS NULL OR JSON_EXTRACT_string(dim_opportunity.value, 'amount') = '' THEN NULL ELSE CAST(JSON_EXTRACT_STRING(dim_opportunity.value, 'amount') AS DOUBLE) END",
  },
  {
    sql_expression:
      "AVG(CAST(json_extract(custom_fields, 'tnt__sonar_security_hotspots') AS INTEGER)) FILTER (CAST(json_extract(custom_fields, 'tnt__sonar_security_hotspots') AS INTEGER) IS NOT NULL)",
  },
  {
    sql_expression: 'any_value(created_by_id)',
  },
  {
    sql_expression: 'COUNT(CASE WHEN tnt__follow_up IS NULL THEN id END)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN sprint_id IS NOT NULL and sprint_id <> '' THEN 1 END) * 100.0 / CAST(COUNT(*) AS FLOAT)",
  },
  {
    sql_expression:
      "MEDIAN(DATEDIFF('minute', created_date, actual_close_date))",
  },
  {
    sql_expression:
      "ANY_VALUE(CAST(json_extract(response, '$.feedback') AS VARCHAR(255)))",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/Syg7eazB' THEN metric_set_value END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/FhAaKJlu' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE when state IS NOT NULL and state='closed' and stage='resolved' THEN id END)",
  },
  {
    sql_expression: 'AVG(time_to_merge)',
  },
  {
    sql_expression: 'SUM("Upside")',
  },
  {
    sql_expression: "COUNT(CASE WHEN stage = 'breached' THEN id END)",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) AND status = 'miss' THEN id END) / (COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) AND status = 'hit' THEN id END) + COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) AND status = 'miss' THEN id END))) * 100",
  },
  {
    sql_expression:
      'count(CASE WHEN service_improvement_status = true THEN id END)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN acc_billing_country = 'AMER' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'In Dev' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'in_testing' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Reassign to customer support' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'PR Changes Requested' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'External scope approval' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/1wxVcQw0' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "count(case when visibility='EXTERNAL' then 1 else NULL end)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN work_status = 'closed' THEN total_estimated_effort_hours ELSE 0 END)",
  },
  {
    sql_expression: 'SUM(total_bytes_billed) * (6.25 / POWER(10, 12))',
  },
  {
    sql_expression: 'avg(severity_rank)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/3' AND (json_extract_string(metric, '$.status') == 'hit' OR json_extract_string(metric, '$.status') == 'in_progress') THEN ticket_id END)*100/COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/3' THEN ticket_id END))",
  },
  {
    sql_expression:
      'CASE WHEN SUM(ARRAY_LENGTH(next_resp_time_arr) + ARRAY_LENGTH(first_resp_time_arr) + ARRAY_LENGTH(resolution_time_arr)) > 0 THEN 100 - (SUM(CASE WHEN ARRAY_LENGTH(next_resp_time_arr) > 0 THEN total_second_resp_breaches_ever ELSE 0 END + CASE WHEN ARRAY_LENGTH(first_resp_time_arr) > 0 THEN total_first_resp_breaches_ever ELSE 0 END + CASE WHEN ARRAY_LENGTH(resolution_time_arr) > 0 THEN total_resolution_breaches_ever ELSE 0 END) / SUM(ARRAY_LENGTH(next_resp_time_arr) + ARRAY_LENGTH(first_resp_time_arr) + ARRAY_LENGTH(resolution_time_arr))) * 100 ELSE NULL END',
  },
  {
    sql_expression:
      "SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_GITHUB' THEN duration END)/SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_GITHUB' THEN count_sync END)",
  },
  {
    sql_expression: 'ARRAY_AGG(DISTINCT id)',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER (     WHERE target_close_date::DATE < CURRENT_DATE      AND stage_name != 'Ready for Dev' )",
  },
  {
    sql_expression: 'avg(sprints_count)',
  },
  {
    sql_expression: 'array_distinct(ARRAY_AGG(type))',
  },
  {
    sql_expression:
      'AVG(AVG(NULLIF(time_to_open, 0))) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
  },
  {
    sql_expression:
      "(1 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END)/ NULLIF(COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) +COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 or total_second_resp_breaches_ever is null ) AND ( total_first_resp_breaches_ever = 0 or total_first_resp_breaches_ever is null ) AND ( total_resolution_breaches_ever = 0 or total_resolution_breaches_ever is null )THEN id END), 0))) * 100 ",
  },
  {
    sql_expression:
      'ARRAY_AGG(DISTINCT id) FILTER (WHERE      (actual_effort is null)     )',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id)  FILTER (WHERE stage_name IN (   'PROBLEM STATEMENT DEFINED',    'Client Approval Pending on Estimate',    'Ready for Scoping',    'In-Progress',    'External Scope Approval',    'Scope Review Pending',    'Scope Approved',    'Tech Approval Pending',    'Ready for Scoping') )",
  },
  {
    sql_expression: 'SUM(total_views)/COUNT(DISTINCT(article_id))',
  },
  {
    sql_expression:
      "(COUNT(distinct CASE WHEN first_time_fix = 'Yes' OR first_time_fix = 'true' THEN id END)/COUNT(distinct id)) * 100  || ''",
  },
  {
    sql_expression: 'sum(is_open)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN severity_name = 'Medium' AND actual_close_date > created_date THEN DATEDIFF('day', created_date, actual_close_date) END)",
  },
  {
    sql_expression: 'AVG(monthly_runs)',
  },
  {
    sql_expression: "COUNT(CASE WHEN state != 'closed' THEN id END)",
  },
  {
    sql_expression: 'any_value(tag_ids)',
  },
  {
    sql_expression: "MEDIAN(datediff('day', created_date, actual_close_date))",
  },
  {
    sql_expression:
      "ROUND(COUNT(DISTINCT CASE WHEN dim_ticket.state == 'closed' THEN id END)/COUNT(DISTINCT owned_by_id))",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/3' AND json_extract_string(metric, '$.status') == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '1' THEN id END) || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN json_extract_string(stage_json, '$.name') IN ('On Hold', 'On-hold', 'On hold') THEN id  END)",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN status == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression: 'avg(metric_set_value)*100',
  },
  {
    sql_expression:
      "count(distinct(case when category = 'healthy' then account_id else null end))",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/1Ci68K8xp' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: 'SUM(CASE WHEN priority = 4 THEN 1 ELSE 0 END)',
  },
  {
    sql_expression:
      "ANY_VALUE(CAST(json_extract(response, '$.rating') AS BIGINT))",
  },
  {
    sql_expression: 'AVG(time_to_review)',
  },
  {
    sql_expression: 'SUM("Discovered: Pre-qualified")',
  },
  {
    sql_expression: 'SUM("Omitted")',
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN status = 'closed' THEN id END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN first_time_fix = 'Yes' OR first_time_fix = 'true' THEN id END)/COUNT(CASE WHEN first_time_fix != '' THEN id END)) * 100  || ''",
  },
  {
    sql_expression:
      'COUNT(CASE WHEN service_improvement_status = true THEN id END)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Open' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Cancelled' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Solutioning Approval Pending' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Client dependency' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Solutioning Approved' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression: 'ANY_VALUE(cnt)',
  },
  {
    sql_expression: 'SUM(run_count)',
  },
  {
    sql_expression: 'SUM(actions_count)',
  },
  {
    sql_expression:
      "ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-01-01', '2024-02-01', '2024-03-01') THEN tnt__customers END) FILTER (WHERE tnt__customers IS NOT NULL)",
  },
  {
    sql_expression:
      "COALESCE((COUNT(CASE WHEN final_answer <> '' THEN 1 END) / COUNT(*))*100,0)",
  },
  {
    sql_expression:
      "EXTRACT(DAY FROM  ANY_VALUE(record_date - target_close_date)) || ' days ' || EXTRACT(HOUR FROM  ANY_VALUE(record_date - target_close_date)) || ' hours '",
  },
  {
    sql_expression: 'MEDIAN(CAST(duration_ms AS DOUBLE) / 1000)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state !='closed' and state is not null AND primary_part_id = 'don:core:dvrv-us-1:devo/1cog4dkFxx:product/28'  THEN id END)",
  },
  {
    sql_expression: 'SUM(created_tickets)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN state = 'closed' THEN 1 END) * 100.0) / COUNT(*)",
  },
  {
    sql_expression: 'AVG(spillover_issue)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN owner = 'don:identity:dvrv-us-1:devo/1B2GHUnRr:devu/5' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression: "sum(case when priority = 'P1' then 1 end)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/tmD9r4ID:metric_set/15qMdjAza' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "(CASE WHEN ({MEERKAT}.opp_close_date <= (MAX(CASE WHEN date_diff('day', current_date(), {MEERKAT}.opp_close_date) < 0 THEN {MEERKAT}.opp_close_date ELSE '1754-08-30' END) OVER ())) THEN (SUM(SUM(actual_amount)) OVER (ORDER BY {MEERKAT}.opp_close_date ROWS UNBOUNDED PRECEDING)) END)",
  },
  {
    sql_expression:
      "SUM(contributions) FILTER (type = 'github.pull_request.merged' OR type = 'bitbucket.pull_request.fulfilled')",
  },
  {
    sql_expression: 'COUNT(DISTINCT opened_id) FILTER (opened_id IS NOT NULL)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT active_id) FILTER (final_state = 'in_progress' AND active_id IS NOT NULL)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN sprint_id IS NULL or sprint_id = '' THEN 1 END) * 100.0 / CAST(COUNT(*) AS FLOAT)",
  },
  {
    sql_expression: 'sum(provisioning)/count(distinct(timestamp))',
  },
  {
    sql_expression: 'AVG(mtta_seconds)',
  },
  {
    sql_expression:
      "ROUND(100.0 * COUNT(CASE WHEN stage_name = 'Ready for Dev' THEN 1 ELSE NULL END) / NULLIF(COUNT(id), 0), 2)",
  },
  {
    sql_expression:
      "printf('$%.2f', (SUM(user_income + partner_income) - SUM(dev_cost + cloud_cost)))",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/2JxwhYm6' THEN metric_set_value END)",
  },
  {
    sql_expression: 'SUM(count_sync)',
  },
  {
    sql_expression: 'COUNT(DISTINCT CONCAT(rev_uid, rev_oid))',
  },
  {
    sql_expression: 'MEDIAN (resolution_time_hours)',
  },
  {
    sql_expression: 'any_value(total_responses)',
  },
  {
    sql_expression: 'COUNT(id)',
  },
  {
    sql_expression: 'COUNT(*) OVER (PARTITION BY id)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN 'don:identity:dvrv-us-1:devo/0:svcacc/273' = ANY(dim_ticket.owned_by_ids) THEN ticket_id END))",
  },
  {
    sql_expression: 'count(conversation)',
  },
  {
    sql_expression:
      'sum(case when errors > 0 then 1 else 0 end) * 100 / count(*)',
  },
  {
    sql_expression: 'count(distinct(account_id))',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/uN6ECfdm' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'SUM(contributions) / ANY_VALUE(avg_user_contrib)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'WorkFlow' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM("Archived")',
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN stage = 'completed' THEN id END)",
  },
  {
    sql_expression: "COUNT(CASE WHEN stage = 'resolved' THEN id END)",
  },
  {
    sql_expression: 'any_value(applies_to_part_ids)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Archived' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Live' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression: 'count(id_su)',
  },
  {
    sql_expression:
      "case when round((sum(mtbf_days) / (count(*) - 1))) > 0 then round((sum(mtbf_days) / (count(*) - 1))) else 'NA' end",
  },
  {
    sql_expression: 'count(case when modified_date = created_date then 1 end)',
  },
  {
    sql_expression: 'ANY_VALUE(record_date)',
  },
  {
    sql_expression: 'AVG(num_articles_clicked)',
  },
  {
    sql_expression:
      "DATE_TRUNC('week', CASE WHEN custom_fields ->> 'ctype__created_at_cfid' IS NOT NULL THEN Cast( custom_fields ->> 'ctype__created_at_cfid' AS TIMESTAMP) ELSE created_date END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN CAST(JSON_EXTRACT_STRING(annual_contract_value,'$.amount') AS INTEGER) = 0 THEN 25000 ELSE CAST(JSON_EXTRACT_STRING(annual_contract_value,'$.amount') AS INTEGER) END)",
  },
  {
    sql_expression: "avg(DATEDIFF('minute', created_date, actual_close_date))",
  },
  {
    sql_expression:
      'AVG(AVG(NULLIF(time_to_review, 0))) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
  },
  {
    sql_expression: 'sum(already_exists)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_ZENDESK' THEN duration END)/SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_ZENDESK' THEN count_sync END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT meeting_id)',
  },
  {
    sql_expression:
      "sum(case when lifecycle_details = 'CLOSED' or lifecycle_details = 'CLOSE_REQUESTED' then 1 else 0 end) - sum(case when lifecycle_details = 'PENDING_WITH_CUSTOMER' or lifecycle_details = 'PENDING_WITH_ORACLE' then 1 else 0 end)",
  },
  {
    sql_expression: 'ANY_VALUE(old_record_date)',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER (WHERE      (product_effort_planned IS NULL)      OR (target_start_date IS NULL OR target_start_date = '')      OR (target_close_date IS NULL OR target_close_date = ''))",
  },
  {
    sql_expression:
      "format('${:t,}', SUM(amount) FILTER(type = 'User')::INTEGER)",
  },
  {
    sql_expression: "COUNT(DISTINCT opened_id) FILTER (opened_id <> '')",
  },
  {
    sql_expression:
      'SUM(mean_reciprocal_rank * total_queries) / SUM(total_queries) * 10',
  },
  {
    sql_expression: 'sum(updated_in_devrev) + sum(updated_in_external)',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER (WHERE      (state = 'open')     )",
  },
  {
    sql_expression: 'SUM(article_downvotes_change)',
  },
  {
    sql_expression: '(AVG(host_serial_count))',
  },
  {
    sql_expression: 'sum(cast(total_nodes as integer))',
  },
  {
    sql_expression:
      "COUNT(DISTINCT id) - COUNT(distinct CASE WHEN stage_json ->> 'name' IN ( 'On Hold', 'On-hold', 'On hold' ) THEN id END) || ''",
  },
  {
    sql_expression: 'COUNT(id_su)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'JIS (J)' THEN id END)  || ''",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN status = 'miss' THEN ticket_id END))",
  },
  {
    sql_expression:
      "COUNT(id) - COUNT(CASE WHEN first_time_fix = 'Yes' OR first_time_fix = 'true' THEN id END)  || ''",
  },
  {
    sql_expression: "COUNT(CASE WHEN severity = '6' THEN id END) || ''",
  },
  {
    sql_expression: 'array_distinct(flatten(ARRAY_AGG(tickets)))',
  },
  {
    sql_expression: 'count( conversation)',
  },
  {
    sql_expression: 'array_agg(distinct id)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/16bjTrSu7' THEN metric_set_value END)",
  },
  {
    sql_expression: '-abs(avg(score_value))*100',
  },
  {
    sql_expression:
      "count(distinct(case when category = 'neutral' then account_id else null end))",
  },
  {
    sql_expression:
      "COUNT(distinct CASE WHEN issue_subtype = 'jira_uniphore.atlassian.net_engineering bug intake_issues.bug' and range_state = 'open' and issue_state != 'closed' THEN id END)",
  },
  {
    sql_expression: 'SUM(CASE WHEN priority = 3 THEN 1 ELSE 0 END)',
  },
  {
    sql_expression: 'SUM(first_half_contributions)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Quantify' THEN ticket END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Workspace' THEN ticket END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'JIA' THEN id END)  || ''",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/3irx4dJ6' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN source_name = 'Events: Effortless' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'triage' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression: "SUM(CASE WHEN page = 'open' THEN actions_count END)",
  },
  {
    sql_expression:
      "CASE WHEN ARRAY_LENGTH(ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-10-01', '2024-11-01', '2024-12-01') THEN id END) FILTER (WHERE CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-10-01', '2024-11-01', '2024-12-01') THEN id END IS NOT NULL)) > 0 THEN ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-10-01', '2024-11-01', '2024-12-01') THEN id END) FILTER (WHERE CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-10-01', '2024-11-01', '2024-12-01') THEN id END IS NOT NULL) ELSE NULL END",
  },
  {
    sql_expression: 'sum(count)',
  },
  {
    sql_expression:
      'MEDIAN(CASE WHEN total_user_sessions != 0 THEN (total_session_length/total_user_sessions) ELSE 0 END)',
  },
  {
    sql_expression: 'ANY_VALUE(total_lines_deleted)',
  },
  {
    sql_expression: 'avg(app_launch_time)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN severity_name = 'High' THEN id END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN severity_name = 'Low' THEN id END)",
  },
  {
    sql_expression:
      "printf('%.2f %%(%.2f %%)', SUM(CAST(COALESCE(max_lines_covered, '0') AS DOUBLE)) / SUM(CAST(COALESCE(max_lines_total, '0') AS DOUBLE)) * 100, (SUM(CAST(COALESCE(max_lines_covered, '0') AS DOUBLE)) / SUM(CAST(COALESCE(max_lines_total, '0') AS DOUBLE))) * 100 - (SUM(CAST(COALESCE(min_lines_covered, '0') AS DOUBLE)) / SUM(CAST(COALESCE(min_lines_total, '0') AS DOUBLE))) * 100)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT id) FILTER (final_state = 'in_progress' AND active_id IS NULL AND id IS NOT NULL)",
  },
  {
    sql_expression: 'round(sum(healthy)/COUNT(DISTINCT(timestamp)))',
  },
  {
    sql_expression: "COUNT(CASE WHEN sla_stage = 'breached' THEN id END)",
  },
  {
    sql_expression: "count(distinct case when issue_linked ='yes' then id end)",
  },
  {
    sql_expression: 'AVG(duration_ms)/1000',
  },
  {
    sql_expression: '(AVG(total_nodes))',
  },
  {
    sql_expression:
      "((SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health) AND node_status IN ('HEN'))/(SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health)))*100",
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER((stage IN ('Problem statement review','Problem Statement Defined','Client approval pending on estimate','Ready for Scoping','In Progress/ Scoping','Designing','Scope Review Pending','Scope Approved','External scope approval','Tech Approval Pending','Sprint Planning','Ready for Dev')) AND CURRENT_DATE<=target_close_date AND DATE_ADD(CAST(CURRENT_TIMESTAMP AS TIMESTAMP),INTERVAL (planned_dev_effort) HOUR) > target_close_date)",
  },
  {
    sql_expression: 'any_value(sum_rating)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN severity_name = 'High' AND actual_close_date > created_date THEN DATEDIFF('day', created_date, actual_close_date) END)",
  },
  {
    sql_expression:
      'avg(case when timestamp_nsecs >= current_date - 15 then value else 0 end)',
  },
  {
    sql_expression: 'avg(avg_members)',
  },
  {
    sql_expression:
      "SUM(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].count') AS INT)) || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'KAI' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'X-Conversa' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'HostFuse (J)' THEN id END)  || ''",
  },
  {
    sql_expression: 'SUM(COALESCE(total_resolution_breaches, 0))',
  },
  {
    sql_expression: 'COUNT(sla_stage)',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN dim_ticket.status == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression: 'COUNT(DISTINCT ticket)',
  },
  {
    sql_expression:
      "(COUNT(CAST( strftime('%H', CASE WHEN custom_fields ->> 'ctype__created_at_cfid' IS NOT NULL THEN Cast( custom_fields ->> 'ctype__created_at_cfid' AS TIMESTAMP) ELSE created_date END) AS VARCHAR)) * 100.0 / SUM(COUNT(id)) OVER ())",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed' and metric_status ='miss' then id end)",
  },
  {
    sql_expression:
      "MEDIAN(FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)))",
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN stage = 'breached' then id end)",
  },
  {
    sql_expression: 'array_agg(distinct user_id)',
  },
  {
    sql_expression: 'round((sum(mtbf_days) / (count(*) - 1)))',
  },
  {
    sql_expression: "COUNT(DISTINCT id) FILTER (state = 'closed')",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN turing_interacted = 'yes' AND is_undeflected = 'no' THEN id END)/COUNT(DISTINCT CASE WHEN is_resolved_today = 'yes' THEN id END))",
  },
  {
    sql_expression: 'any_value(sla)',
  },
  {
    sql_expression:
      "count(CASE WHEN state='open' AND te is true THEN ticket END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN state = 'open' OR state = 'in_progress' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN status != '' THEN ticket_id END))*100/(COUNT(DISTINCT ticket_id))",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/16bjTrSu7' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'PROBLEM STATEMENT DEFINED' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'RCA IN PROGRESS' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Deploy Pend on Demo' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Test Cases in Progress' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT CASE WHEN state IS NOT NULL THEN id END)',
  },
  {
    sql_expression: 'COUNT(distinct id_su)',
  },
  {
    sql_expression: 'COUNT(DISTINCT opened_id)',
  },
  {
    sql_expression:
      "count(DISTINCT CASE WHEN DATE_TRUNC('day', actual_close_date) == DATE_TRUNC('day', record_date) THEN ticket END)",
  },
  {
    sql_expression:
      'case when ROUND(AVG(coalesce((mtbf_hours),0))) > 0 then ROUND(AVG(coalesce((mtbf_hours),0))) else null end',
  },
  {
    sql_expression: 'PERCENTILE_DISC(0.95) WITHIN GROUP ( ORDER BY res_time)',
  },
  {
    sql_expression:
      'PERCENTILE_DISC(0.80) WITHIN GROUP ( ORDER BY completed_in)',
  },
  {
    sql_expression: 'max(max_duration)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/tmD9r4ID:metric_set/17s49oci4' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN metric_status = 'miss' THEN id END)",
  },
  {
    sql_expression:
      "COUNT(distinct case when range_state= 'open' then  id end)",
  },
  {
    sql_expression: 'COALESCE(AVG(next_resp_time), 0)',
  },
  {
    sql_expression: 'any_value(availability_score_latest)',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER (WHERE stage_name IN (   'Internal UAT',    'In UAT',    'Live',    'QA on production',    'Test cases automated',    'Completed') )",
  },
  {
    sql_expression:
      'CASE WHEN SUM(actual_amount) is NOT NULL THEN SUM(SUM(actual_amount)) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING) END',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN sla_stage = 'breached' THEN id END)* 100.0 / COUNT(*))",
  },
  {
    sql_expression:
      "AVG(CASE WHEN severity_name = 'Blocker' AND actual_close_date > created_date THEN DATEDIFF('day', created_date, actual_close_date) END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'X-Console/Interact' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Quantify Platform' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Quantify' THEN id END)  || ''",
  },
  {
    sql_expression: "COUNT( issueId)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', actual_close_date) = DATE_TRUNC('day', record_hour) THEN id END)",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/2' AND json_extract_string(metric, '$.status') == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression: 'sum(count_sync)',
  },
  {
    sql_expression: 'avg(max_value)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/1Ci68K8xp' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "(datediff('day', MIN(created_date), MAX(actual_close_date)))",
  },
  {
    sql_expression: 'count(distinct id )',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/Syg7eazB' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'U-Self Serve' THEN ticket END)  || ''",
  },
  {
    sql_expression:
      "count(distinct CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '3' THEN id END) || ''",
  },
  {
    sql_expression:
      "SUM(CASE WHEN source_name = 'Events: Webinar' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Scope review pending' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'backlog' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT(object_id))',
  },
  {
    sql_expression: 'ROUND(AVG(total_bytes_billed) / (1024 * 1024*1024), 2)',
  },
  {
    sql_expression:
      "CASE WHEN ARRAY_LENGTH(ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-07-01', '2024-08-01', '2024-09-01') THEN id END) FILTER (WHERE CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-07-01', '2024-08-01', '2024-09-01') THEN id END IS NOT NULL)) > 0 THEN ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-07-01', '2024-08-01', '2024-09-01') THEN id END) FILTER (WHERE CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-07-01', '2024-08-01', '2024-09-01') THEN id END IS NOT NULL) ELSE NULL END",
  },
  {
    sql_expression: "count(date_diff('days',created_date,actual_close_date))",
  },
  {
    sql_expression: "COUNT(*) FILTER(state = 'open')",
  },
  {
    sql_expression: 'count(*id)',
  },
  {
    sql_expression:
      "COUNT(Distinct case when source= 'issues' then  id end)  || ''",
  },
  {
    sql_expression: 'max(curr_p50_latency) / 1000',
  },
  {
    sql_expression: "json_extract_string(stage_json, '$.stage_id')",
  },
  {
    sql_expression: 'MEDIAN(unhealthy)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed' and state is not NULL THEN id END)",
  },
  {
    sql_expression: 'ANY_VALUE(ticket_count_age)',
  },
  {
    sql_expression: 'sum(created)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN owner = 'don:identity:dvrv-us-1:devo/1B2GHUnRr:devu/660' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression:
      "sum(case when lifecycle_details = 'PENDING_WITH_CUSTOMER' then 1 else 0 end)",
  },
  {
    sql_expression: 'sum(is_rage) * 100 / count(*)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/tmD9r4ID:metric_set/17s49oci4' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "(CASE WHEN (opp_close_date >= (MAX(CASE WHEN date_diff('day', current_date(), opp_close_date) < 0 THEN opp_close_date END) OVER ())) THEN ((SUM(CASE WHEN SUM(forecast_amount) IS NOT NULL THEN SUM(forecast_amount) ELSE 0 END) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING) + (SUM(CASE WHEN SUM(actual_amount) IS NOT NULL THEN SUM(actual_amount) ELSE 0 END) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING)))) END)",
  },
  {
    sql_expression:
      "count(distinct case when category = 'neutral' then account_id else null end)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) != DATE_TRUNC('day', record_hour)  AND DATE_TRUNC('day', actual_close_date) = DATE_TRUNC('day', record_hour) AND group_id = 'don:identity:dvrv-us-1:devo/jGGq3WwX:group/6' THEN id END)",
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='awaiting_product_assist' and 'don:core:dvrv-us-1:devo/0:tag/1606'=ANY(tag_ids) and severity_name='Blocker' and state != 'closed'  AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: 'count(distinct applies_to_part_id)',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER (WHERE DATEDIFF('day', created_date, CURRENT_DATE) > 90)",
  },
  {
    sql_expression: '(AVG(time_to_repair))/1440',
  },
  {
    sql_expression:
      "100-((SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health) AND health_status IN ('Healthy'))/(SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health)))*100",
  },
  {
    sql_expression: "ARRAY_AGG(id) FILTER(state='open')",
  },
  {
    sql_expression: "ARRAY_AGG(id) FILTER(state IN ('open','in_progress'))",
  },
  {
    sql_expression:
      "CASE WHEN severity = 8 AND actual_close_date > created_date THEN date_diff('minutes', created_date, actual_close_date) ELSE null END",
  },
  {
    sql_expression:
      'COALESCE((select value from cod_orders_count cc2 where cc2.account_id = account_metrics__account and cast(cast(cc2.timestamp_nsecs as timestamp) as date) = cast(current_date - 1 as date) order by timestamp_nsecs desc limit 1),0)',
  },
  {
    sql_expression:
      'cast(percentile_cont(0.75) within group (order by cast(duration as int)) as int)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Analyze on X-Plat' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Capture' THEN id END)  || ''",
  },
  {
    sql_expression: 'AVG(moving_average)',
  },
  {
    sql_expression: "SUM(planned_dev_effort) FILTER (state = 'closed')",
  },
  {
    sql_expression: "COUNT(CASE WHEN metric_status = 'hit' THEN id END)",
  },
  {
    sql_expression:
      "AVG(CAST(json_extract(custom_fields, 'tnt__sonar_vulnerabilities') AS INTEGER)) FILTER (CAST(json_extract(custom_fields, 'tnt__sonar_vulnerabilities') AS INTEGER) IS NOT NULL)",
  },
  {
    sql_expression: 'any_value(source_channel)',
  },
  {
    sql_expression: 'sum(duration)/(SUM(count_sync))',
  },
  {
    sql_expression: "COUNT(CASE WHEN status = 'hit' THEN id END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '3' THEN id END) || ''",
  },
  {
    sql_expression: 'avg(score_value)*100',
  },
  {
    sql_expression: 'COUNT(DISTINCT user_id)',
  },
  {
    sql_expression: 'COUNT(distinct id)',
  },
  {
    sql_expression: 'COUNT(distinct issue_id) FILTER (issue_id IS NOT NULL)',
  },
  {
    sql_expression: 'sum(total_estimated_effort_hours)',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN is_resolved_today='yes' THEN id END)/COUNT(DISTINCT id))*100",
  },
  {
    sql_expression: 'AVG(sat_result)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN state='closed' AND actual_close_date > created_date THEN DATE_DIFF('day',created_date, actual_close_date ) END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Q for Sales' THEN ticket END)  || ''",
  },
  {
    sql_expression: 'SUM("Discovered: Qualified")',
  },
  {
    sql_expression: 'any_value(account_ids)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/Ds2FRuuX' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN source_name = 'Inbound: Organic' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Open' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'in_development' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Designing' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/1wxVcQw0' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/1wxVcQw0' THEN metric_set_value END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT active_id) FILTER (final_state = 'in-progress')",
  },
  {
    sql_expression:
      "DATEDIFF('minute', any_value(created_date), any_value(CASE WHEN actual_close_date = '1970-01-01T00:00:00.000Z' THEN NULL ELSE actual_close_date END))",
  },
  {
    sql_expression:
      "COUNT( DISTINCT (CASE WHEN (source='tickets' and range_state ='open') THEN id END))",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', actual_close_date) = month and created_date < actual_close_date THEN id END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/5WLaGLPW' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'sum(case when total_issues>0 then 1 else 0 end)',
  },
  {
    sql_expression: 'ANY_VALUE(group_ids)',
  },
  {
    sql_expression: 'ANY_VALUE(location)',
  },
  {
    sql_expression:
      "format('${:t,}', SUM(amount) FILTER(type = 'Cloud')::INTEGER)",
  },
  {
    sql_expression: 'count(state)',
  },
  {
    sql_expression: 'AVG(article_published_to_mitigated)',
  },
  {
    sql_expression:
      'ARRAY_AGG( DISTINCT id) FILTER(WHERE target_close_date >= CURRENT_DATE AND target_close_date < sprint_end_date)',
  },
  {
    sql_expression:
      'CASE WHEN SUM(total_views) = 0 THEN 0 ELSE SUM(total_duration_ms)/SUM(total_views*1000) END',
  },
  {
    sql_expression: 'ANY_VALUE(active_status)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN stage in ('On Hold', 'On-hold', 'On hold') THEN DATEDIFF('minutes', created_date, current_date) END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN metric_summary.value ->> 'metric_definition_id' LIKE '%metric_definition/1%' and  metric_completed_in <= 60  and tnt_product = 'Quantify' THEN id END)  /COUNT(CASE WHEN metric_summary.value ->> 'metric_definition_id' LIKE '%metric_definition/1%' and tnt_product = 'Quantify' THEN id END)) * 100",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Self Serve' THEN id END)  || ''",
  },
  {
    sql_expression: 'max(p50_latency) / 1000',
  },
  {
    sql_expression:
      "SUM(CASE WHEN is_conversation_linked = 'yes' THEN 1 ELSE 0 END) AS total_yes_count",
  },
  {
    sql_expression:
      "ROUND((COUNT(DISTINCT CASE WHEN status = 'miss' THEN ticket_id END)/COUNT(DISTINCT owned_by_id)))",
  },
  {
    sql_expression: 'count(display_id)',
  },
  {
    sql_expression:
      "ROUND(COUNT(DISTINCT CASE WHEN dim_ticket.state != 'closed' THEN id END)/COUNT(DISTINCT owned_by_id))",
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN stage = 'active' then id end)",
  },
  {
    sql_expression:
      'count(CASE WHEN ctype__u_service_improvementsu_service_improvements_cfid = true THEN id END)',
  },
  {
    sql_expression:
      "CASE WHEN dim_opportunity.annual_contract_value IS NULL OR JSON_EXTRACT_string(dim_opportunity.annual_contract_value, 'amount') = '' THEN NULL ELSE CAST(JSON_EXTRACT_STRING(dim_opportunity.annual_contract_value, 'amount') AS DOUBLE) END",
  },
  {
    sql_expression: '(COUNT(DISTINCT conv_id))',
  },
  {
    sql_expression: 'abs(avg(score_value))*100',
  },
  {
    sql_expression:
      "CAST(AVG(datediff('day', created_date, current_date)) AS INT) || ' days'",
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN state = 'open' THEN id END)",
  },
  {
    sql_expression: 'SUM(count_survey)',
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'PR Changes Requested' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/2JxwhYm6' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      "COALESCE((COUNT(CASE WHEN gen_answer <> '' THEN 1 END) / COUNT(*))*100,0)",
  },
  {
    sql_expression: "COUNT(datediff('day', created_date, actual_close_date))",
  },
  {
    sql_expression:
      'CASE WHEN COUNT(DISTINCT CASE WHEN total_responses > 0 THEN 1 END) > 0 THEN COUNT(CASE WHEN total_responses > 0 AND floor(sum_rating / NULLIF(total_responses, 0)) IN (4, 5) THEN id END) * 100.0 / COUNT(DISTINCT CASE WHEN total_responses > 0 THEN id END) ELSE NULL END',
  },
  {
    sql_expression: 'COUNT(DISTINCT part_id)',
  },
  {
    sql_expression:
      "COUNT(*) FILTER( where state = 'closed' AND actual_close_date>target_close_date)",
  },
  {
    sql_expression:
      "printf('%.2f / 5 (%+.2f%%)', new_final_val, ((new_final_val - old_final_val) / old_final_val) * 100)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'ctype__x1260822517949_cfid' IS NULL and subtype in ('partner','supported_employee') and state !='closed' THEN id END)",
  },
  {
    sql_expression: 'round(sum(mtbf_days) / (count(*) - 1))',
  },
  {
    sql_expression:
      "SUM(CASE WHEN severity_name = 'Blocker' AND COALESCE(total_resolution_breaches, 0) > 0  THEN 1 ELSE 0 END)",
  },
  {
    sql_expression: 'round(sum(undelivered)/COUNT(DISTINCT(timestamp)))',
  },
  {
    sql_expression: 'round(sum(provisioning)/COUNT(DISTINCT(timestamp)))',
  },
  {
    sql_expression:
      "SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_SERVICENOW' THEN duration END)/SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_SERVICENOW' THEN count_sync END)",
  },
  {
    sql_expression:
      "sum(case when ux_evaluation = 'TOLERATING' then 1 else 0 end) * 100 / count(*)",
  },
  {
    sql_expression:
      'ROUND(AVG(EXTRACT(EPOCH FROM (CAST(ticket_time_updated AS TIMESTAMP) - CAST(ticket_time_created AS TIMESTAMP))) / 3600), 1)',
  },
  {
    sql_expression: "sum(case when exception_type = 'ANR' then 1 else 0 end)",
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER(WHERE stage_name in ('Sprint Planning',   'Ready for Dev',   'In Dev',   'Code Review',   'Internal UAT',   'In UAT',   'Live',   'QA on production',   'Test cases automated',   'Completed'))",
  },
  {
    sql_expression: 'SUM(code_contributions)',
  },
  {
    sql_expression: 'ANY_VALUE(title)',
  },
  {
    sql_expression: 'ANY_VALUE(experience_start_date)',
  },
  {
    sql_expression: 'ANY_VALUE(persona)',
  },
  {
    sql_expression: 'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY time_to_open)',
  },
  {
    sql_expression:
      "format('${:t,}', SUM(amount) FILTER(type = 'Dev')::INTEGER)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN custom_fields->>'ctype__total_alerts' IS NOT NULL THEN CAST(custom_fields->>'ctype__total_alerts' AS INT) ELSE 0 END)",
  },
  {
    sql_expression:
      '((sum(undelivered)/COUNT(DISTINCT(timestamp))))/((sum(total_expected)/COUNT(DISTINCT(timestamp))))*100',
  },
  {
    sql_expression: '((AVG(host_serial_count))/(AVG(total_nodes)))*100',
  },
  {
    sql_expression:
      "((SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health) AND health_status IN ('Healthy'))/(SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health)))*100",
  },
  {
    sql_expression:
      "((SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health) AND node_status IN ('Customer'))/(SELECT COUNT(node_id) FROM oci_fleet_health WHERE partition_ts = (SELECT MAX(partition_ts) FROM oci_fleet_health)))*100",
  },
  {
    sql_expression: 'COUNT(id_devo)',
  },
  {
    sql_expression: 'MAX(CAST(physical_loc as INT))',
  },
  {
    sql_expression:
      'COALESCE((select value from rider_payout_count cc2 where cc2.account_id = account_metrics__account and cast(cast(cc2.timestamp_nsecs as timestamp) as date) = cast(current_date - 1 as date) order by timestamp_nsecs desc limit 1),0)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'X-Platform' THEN id END)  || ''",
  },
  {
    sql_expression:
      'FLOOR(SUM(sum_rating) OVER (PARTITION BY id) / SUM(total_responses) OVER (PARTITION BY id))',
  },
  {
    sql_expression: 'count(distinct id)',
  },
  {
    sql_expression: 'quantile_cont(CAST(duration AS DOUBLE), 0.75) OVER ()',
  },
  {
    sql_expression: 'COUNT(CASE WHEN needs_response = true THEN id END)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN first_time_fix = 'Yes' OR first_time_fix = 'true' THEN id END)/COUNT(id)) * 100  || ''",
  },
  {
    sql_expression:
      "COUNT( CASE WHEN state = 'closed' and state is not NULL THEN id END)",
  },
  {
    sql_expression:
      "COUNT(*) / COUNT(DISTINCT Date_trunc('day', CASE WHEN custom_fields ->> 'ctype__created_at_cfid' IS NOT NULL THEN Cast( custom_fields ->> 'ctype__created_at_cfid' AS TIMESTAMP) ELSE created_date END))",
  },
  {
    sql_expression:
      "SUM(CAST(json_extract(custom_fields, 'tnt__sonar_bugs') AS INTEGER)) FILTER (CAST(json_extract(custom_fields, 'tnt__sonar_bugs') AS INTEGER) IS NOT NULL)",
  },
  {
    sql_expression: 'any_value(owned_by_ids)',
  },
  {
    sql_expression: "COUNT(CASE WHEN status = 'hit' then id end)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/3irx4dJ6' THEN metric_set_value END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '4' THEN id END) || ''",
  },
  {
    sql_expression:
      'AVG(SUM(daily_active_users)) OVER (ORDER BY created_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)',
  },
  {
    sql_expression: 'SUM(contribution)',
  },
  {
    sql_expression: 'COALESCE(MEDIAN(next_resp_time), 0)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'U-Analyze' THEN ticket END)  || ''",
  },
  {
    sql_expression:
      "SUM(CASE WHEN (json_extract_string(custom_fields, '$.tnt__stage') like '%Discovered%' or json_extract_string(custom_fields, '$.tnt__stage') like '%Started%' or json_extract_string(custom_fields, '$.tnt__stage') like '%Activated%') THEN 1 ELSE 0 END)",
  },
  {
    sql_expression:
      "100.0 * (\n    COUNT(DISTINCT CASE \n        WHEN state != 'closed' AND \n           ( 'don:core:dvrv-us-1:devo/0:tag/2124' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13375' = ANY(tag_ids)) AND \n            state IS NOT NULL  THEN id  END) \n    / NULLIF(COUNT(DISTINCT CASE \n                  WHEN state != 'closed' AND \n                       state IS NOT NULL AND \n                       NOT ('don:core:dvrv-us-1:devo/0:tag/13381' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13380' = ANY(tag_ids)) THEN id END), 0))\n",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN turing_effective='yes' THEN id END)/COUNT(DISTINCT id))*100",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Tech Approval Pending' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'in_deployment' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Test cases automated' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Scoping' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression: 'MAX(CAST(affected_area as INT))',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/10VEbARzh' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: ' COUNT(DISTINCT id)',
  },
  {
    sql_expression:
      "ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-01-01', '2024-02-01', '2024-03-01') THEN id END) FILTER (WHERE id IS NOT NULL)",
  },
  {
    sql_expression: 'COUNT(DISTINCT closed_id)',
  },
  {
    sql_expression: 'SUM(arr)',
  },
  {
    sql_expression: 'SUM(sat_result)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/ogNAgdmp:metric_set/8jZGgeBH' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: 'ANY_VALUE(opened_date)',
  },
  {
    sql_expression:
      "COUNT(case when source= 'issues' and state ='closed' and Json_extract_string(stage_json, '$.name') != 'resolved' then  id end)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN TRIM('\"' FROM JSON_EXTRACT(owned_by_ids, '$[0]')) IN ('don:identity:dvrv-us-1:devo/1cog4dkFxx:svcacc/6') THEN id END)",
  },
  {
    sql_expression: 'AVG(spillover_rate)',
  },
  {
    sql_expression:
      "sum(case when ux_evaluation = 'FRUSTRATED' then 1 else 0 end) * 100 / count(*)",
  },
  {
    sql_expression: 'avg(first_user_interaction)',
  },
  {
    sql_expression:
      "sum(case when exception_type = 'CRASH' then 1 else 0 end) * 100 / count(*)",
  },
  {
    sql_expression:
      "SUM(SUM(CASE WHEN CAST(JSON_EXTRACT_STRING(annual_contract_value,'$.amount') AS INTEGER) = 0 THEN 25000 ELSE CAST(JSON_EXTRACT_STRING(annual_contract_value,'$.amount') AS INTEGER) END)) OVER (PARTITION BY {MEERKAT}.owner_sdr)",
  },
  {
    sql_expression:
      'ARRAY_AGG(DISTINCT id) FILTER (WHERE target_close_date > sprint_end_date)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN stage_name = 'Ready for Dev' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "ARRAY_AGG( DISTINCT id) FILTER(WHERE target_close_date < CURRENT_DATE AND stage_name != 'Ready for Dev')",
  },
  {
    sql_expression: 'COUNT(e_article_id)',
  },
  {
    sql_expression: 'AVG(article_downvotes)',
  },
  {
    sql_expression: 'avg(measure3)',
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER(state = 'open' AND CURRENT_DATE()>target_close_date)",
  },
  {
    sql_expression:
      "CASE WHEN severity = 6 AND actual_close_date > created_date THEN date_diff('minutes', created_date, actual_close_date) ELSE null END",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Workspace (J)' THEN id END)  || ''",
  },
  {
    sql_expression: 'count(*)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/1' AND json_extract_string(metric, '$.status') == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression: 'count(id)',
  },
  {
    sql_expression: 'MAX(max_duration)',
  },
  {
    sql_expression: 'count(*) / count(distinct identifier)',
  },
  {
    sql_expression:
      'sum(case when dead_clicks > 0 then 1 else 0 end) * 100 / count(*)',
  },
  {
    sql_expression:
      'SUM(total_resolution_breaches) + SUM(total_second_resp_breaches) + SUM(total_first_resp_breaches)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN is_resolved_today='yes' THEN id END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN state='open' THEN DATEDIFF('day', created_date,current_date) END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'JIS' THEN ticket END)  || ''",
  },
  {
    sql_expression:
      "SUM(CASE WHEN state = 'open' OR state = 'in_progress' THEN (amount*fprobability/100) ELSE 0 END)",
  },
  {
    sql_expression:
      'CASE WHEN SUM(total_user_sessions) != 0 THEN (SUM(total_session_length) / SUM(total_user_sessions)) ELSE 0 END',
  },
  {
    sql_expression: 'MEDIAN(total_session_length)',
  },
  {
    sql_expression: 'sum(daily_distinct_revuser_logins)',
  },
  {
    sql_expression:
      "case WHEN date_trunc('year', dim_ticket.actual_close_date) >= '2000' THEN 1 else 0 END",
  },
  {
    sql_expression:
      'case WHEN dim_ticket.actual_close_date is not null THEN 1 else 0 END',
  },
  {
    sql_expression:
      "count(CASE WHEN service_improvement_status = 'Open' THEN id END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN source_name = 'Events: Exec Dinners' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN source_name = 'Partner' THEN acv ELSE 0 END)",
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN state != 'closed' THEN id END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Ready for Dev' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'prioritized' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Reassign to customer support' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'In UAT - Hypercare (QA Pending)' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'QA on Demo' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression: 'SUM(CAST(total_changes as INT))',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/5WLaGLPW' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/16bjTrSu7' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      'CASE WHEN COUNT(CASE WHEN total_responses > 0 THEN 1 END) > 0 THEN COUNT(CASE WHEN total_responses > 0 AND floor(sum_rating / NULLIF(total_responses, 0)) IN (4, 5) THEN 1 END) * 100.0 / COUNT(CASE WHEN total_responses > 0 THEN 1 END) ELSE NULL END',
  },
  {
    sql_expression: "SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression: 'SUM(installation_count)',
  },
  {
    sql_expression: "SUM(CASE WHEN action = 'install' THEN actions_count END)",
  },
  {
    sql_expression:
      "printf('%.2f%%',SUM(CAST(lines_covered as DOUBLE)) / SUM(CAST(lines_total as DOUBLE)) * 100)",
  },
  {
    sql_expression:
      "ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-04-01', '2024-05-01', '2024-06-01') THEN tnt__customers END) FILTER (WHERE tnt__customers IS NOT NULL)",
  },
  {
    sql_expression:
      "ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-10-01', '2024-11-01', '2024-12-01') THEN tnt__customers END) FILTER (WHERE tnt__customers IS NOT NULL)",
  },
  {
    sql_expression:
      "count(case when modified_date = actual_close_date and state = 'closed' then 1 end)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN stage = 'breached' THEN datediff('day',target_time,current_date) END)",
  },
  {
    sql_expression:
      "EXTRACT(DAY FROM  ANY_VALUE(target_close_date - record_date)) || ' days ' || EXTRACT(HOUR FROM  ANY_VALUE(target_close_date - record_date)) || ' hours '",
  },
  {
    sql_expression: "AVG(DATE_DIFF('day', issue_date, issue_closed_date))",
  },
  {
    sql_expression: 'SUM(closed_tickets)',
  },
  {
    sql_expression:
      '(SUM(weighted_contribution) / ANY_VALUE(avg_user_contrib)) / SUM(user_percentage)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN severity_name = 'Medium' THEN id END)",
  },
  {
    sql_expression:
      'CASE WHEN (opp_close_date >= ANY_VALUE(latest_close_date_before_today)) THEN ((SUM(CASE WHEN SUM(forecast_amount) IS NOT NULL THEN SUM(forecast_amount) ELSE 0 END) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING) + (SUM(CASE WHEN SUM(actual_amount) IS NOT NULL THEN SUM(actual_amount) ELSE 0 END) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING)))) END',
  },
  {
    sql_expression:
      'SUM(CASE WHEN acv IS NULL OR acv = 0 THEN 25000 ELSE acv END)',
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='awaiting_development' and state != 'closed'  AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: 'floor(avg(monthly_distinct_devuser_logins))',
  },
  {
    sql_expression: 'COUNT(DISTINCT(article_id))',
  },
  {
    sql_expression: '(AVG(time_to_break))',
  },
  {
    sql_expression:
      'AVG(time_to_open + time_to_review + time_to_approve + time_to_merge + avg_time_to_deploy)',
  },
  {
    sql_expression:
      "100.0 * (\nCOUNT(DISTINCT CASE WHEN state != 'closed' AND 'don:core:dvrv-us-1:devo/0:tag/2129' = ANY(tag_ids) AND state IS NOT NULL THEN id END)\n/ NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0)\n)",
  },
  {
    sql_expression: 'ROUND(AVG(diff_days), 2)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/Ds2FRuuX' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      'cast(percentile_cont(0.95) within group (order by cast(duration as int)) as int)',
  },
  {
    sql_expression:
      'cast(percentile_cont(0.50) within group (order by cast(duration as int)) as int)',
  },
  {
    sql_expression: "median(datediff('day',created_date,current_date))",
  },
  {
    sql_expression: 'SUM(COALESCE(total_first_resp_breaches, 0))',
  },
  {
    sql_expression: '(COUNT(DISTINCT ticket_id))',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state != 'closed' AND state IS NOT NULL AND owned_by_ids IS NULL THEN id END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN id = 'don:identity:dvrv-us-1:devo/0:account/1F5aezyGo' THEN active_users ELSE 0 END)",
  },
  {
    sql_expression: 'sum(duration)',
  },
  {
    sql_expression:
      "SUM(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].sum') AS DOUBLE))/SUM(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].count') AS DOUBLE))",
  },
  {
    sql_expression:
      "count(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) == DATE_TRUNC('day', record_date) THEN conv END)",
  },
  {
    sql_expression:
      'sum(case when rage_clicks > 0 then 1 else 0 end) * 100 / count(*)',
  },
  {
    sql_expression:
      "(COUNT(CAST( strftime('%H', created_date) AS VARCHAR)) * 100.0 / SUM(COUNT(id)) OVER ())",
  },
  {
    sql_expression: 'COUNT(DISTINCT ID)',
  },
  {
    sql_expression: 'SUM(active_users)',
  },
  {
    sql_expression: 'COUNT(DISTINCT(id))',
  },
  {
    sql_expression: 'AVG(total_duration)',
  },
  {
    sql_expression: 'ANY_VALUE(rev_oid)',
  },
  {
    sql_expression: 'SUM("Commit")',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed'  AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: 'max(count_of_active_snapins)',
  },
  {
    sql_expression: 'avg(weekly_distinct_devuser_logins)',
  },
  {
    sql_expression: 'any_value(stage_id)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN service_improvement_status = 'Closed' THEN id END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Bad Quality Ticket' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Internal UAT' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "100 * (SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*))",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed' AND 'don:core:dvrv-us-1:devo/787:tag/67143' = ANY(tag_ids) AND state IS NOT NULL THEN id END)\n",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__follow_up' IS NULL THEN id END)",
  },
  {
    sql_expression: 'COUNT(pr_url)',
  },
  {
    sql_expression:
      'case WHEN dim_ticket.created_date is not null THEN 1 else 0 END',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN sla_has_breached = false THEN id WHEN metric_status = 'hit' THEN id END)/count(id)) * 100",
  },
  {
    sql_expression: 'ANY_VALUE(total_lines_added)',
  },
  {
    sql_expression: 'SUM(act_result)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/3irx4dJ6' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "sum(case when ux_evaluation = 'SATISFIED' then 1 else 0 end) * 100 / count(*)",
  },
  {
    sql_expression: 'sum(unhealthy)/count(distinct(timestamp))',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER(WHERE stage_name in ('Live', 'Ready For Deployment','QA on production',  'Completed'))",
  },
  {
    sql_expression: 'AVG(avg_top)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN (target_close_date < CURRENT_DATE AND stage_name != 'Ready for Dev') THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "ROUND(100.0 * COUNT( DISTINCT CASE WHEN stage_name in ('Internal UAT',   'In UAT',   'Live',   'QA on production',   'Test cases automated',   'Completed') THEN id ELSE NULL END) / NULLIF(COUNT( DISTINCT id), 0), 2)",
  },
  {
    sql_expression:
      "format('${:t,}', SUM(amount) FILTER(type = 'Partner')::INTEGER)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/16bjTrSu7' THEN metric_set_value END)",
  },
  {
    sql_expression: 'sum(sync_trigger_count)',
  },
  {
    sql_expression: 'floor(avg(monthly_distinct_revuser_logins))',
  },
  {
    sql_expression: 'sum(created_in_devrev) + sum(created_in_external)',
  },
  {
    sql_expression: 'COUNT(evaluate)',
  },
  {
    sql_expression: 'sum(cast(average_fleet_time as integer))',
  },
  {
    sql_expression:
      'COALESCE((select value from consignment_count cc2 where cc2.account_id = account_metrics__account and cast(cast(cc2.timestamp_nsecs as timestamp) as date) = cast(current_date - 1 as date) order by timestamp_nsecs desc limit 1),0)',
  },
  {
    sql_expression:
      "100.0 * (COUNT(DISTINCT CASE WHEN severity_name = 'Blocker' AND state != 'closed' AND state IS NOT NULL THEN id END) / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0))",
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER (state = 'open' or state='in_progress')",
  },
  {
    sql_expression: 'quantile_cont(CAST(duration AS DOUBLE), 0.90) OVER ()',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN metric_status = 'in_progress' and metric_stage ='breached' then id end)",
  },
  {
    sql_expression:
      "COUNT(distinct CASE WHEN issue_subtype = 'jira_uniphore.atlassian.net_engineering bug intake_issues.bug' and range_state = 'closed' and issue_state = 'closed' THEN id END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/2' AND json_extract_string(metric, '$.status') == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression:
      "(CASE WHEN (opp_close_date >= (MAX(CASE WHEN date_diff('day', current_date(), opp_close_date) < 0 THEN opp_close_date ELSE '1754-08-30' END) OVER ())) THEN ((SUM(CASE WHEN SUM(forecast_amount) IS NOT NULL THEN SUM(forecast_amount) ELSE 0 END) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING) + (SUM(CASE WHEN SUM(actual_amount) IS NOT NULL THEN SUM(actual_amount) ELSE 0 END) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING)))) END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Interact (Legacy)' THEN ticket END)  || ''",
  },
  {
    sql_expression: 'SUM("Churned: Lost")',
  },
  {
    sql_expression: 'SUM("Converted: Expanding")',
  },
  {
    sql_expression:
      "SUM(CASE WHEN (json_extract_string(custom_fields, '$.tnt__stage') like '%Converted%') THEN 1 ELSE 0 END)",
  },
  {
    sql_expression: 'sum(daily_parts_created)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/3irx4dJ6' THEN curr_metric_set_value END)",
  },
  {
    sql_expression: "SUM(CASE WHEN retry_count = '3' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression: 'SUM(count)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN target_close_quarter = '2025-Q3' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-07-01', '2024-08-01', '2024-09-01') THEN tnt__customers END) FILTER (WHERE tnt__customers IS NOT NULL)",
  },
  {
    sql_expression:
      "ARRAY_AGG(CASE WHEN DATE_TRUNC('month', CAST(tnt__actual_product_release AS TIMESTAMP)) IN ('2024-10-01', '2024-11-01', '2024-12-01') THEN id END) FILTER (WHERE id IS NOT NULL)",
  },
  {
    sql_expression:
      "COUNT(case when custom_fields->>'ctype__support_priority' = 'Low' then  id end)",
  },
  {
    sql_expression: 'AVG(duration)',
  },
  {
    sql_expression: 'count(distinct ticket_id)',
  },
  {
    sql_expression:
      "Count(SELECT DISTINCT ON (id)\n    id,\nFROM\n    support_insights_ticket_metrics_summary\nWHERE\n    state != 'closed' AND\n    state IS NOT NULL AND\n    sla_stage !='' AND\n    sla_stage NOT NULL)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/ogNAgdmp:metric_set/1FNrpudPE' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'SUM(clicked_count)',
  },
  {
    sql_expression:
      'LAG(max(curr_p95_latency)) OVER (ORDER BY max(curr_timestamp) asc) / 1000',
  },
  {
    sql_expression:
      "COUNT(distinct CASE WHEN JSON_EXTRACT_STRING(stage_json, '$.name') = 'duplicate' THEN id END)",
  },
  {
    sql_expression:
      'PERCENTILE_DISC(0.95) WITHIN GROUP ( ORDER BY completed_in)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE      WHEN metric_status = 'in_progress'      THEN id  END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN ((product_effort_planned IS NULL )                  OR (target_start_date IS NULL OR target_start_date = '')                  OR (target_close_date IS NULL OR target_close_date = '')) THEN 1 ELSE NULL END) ",
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='awaiting_product_assist' and state != 'closed'  AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "format('${:t,}', (SUM(amount) FILTER(direction = 'Income'))::INTEGER)",
  },
  {
    sql_expression: 'AVG(article_creation_to_mitigated)',
  },
  {
    sql_expression: 'SUM(actual_effort)',
  },
  {
    sql_expression:
      "(SELECT CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(first_resp_time_arr) > 0) AND (total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 / (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(first_resp_time_arr) > 0) AND (total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL) THEN id END))) ELSE NULL END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/16bjTrSu7' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      'COALESCE((select value from sms_sent cc2 where cc2.account_id = account_metrics__account and cast(cast(cc2.timestamp_nsecs as timestamp) as date) = cast(current_date - 1 as date) order by timestamp_nsecs desc limit 1),0)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'AI Services' THEN id END)  || ''",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN dim_ticket.stage == 'warning' THEN ticket_id END))",
  },
  {
    sql_expression:
      "SUM(planned_dev_effort) FILTER (state = 'open' or state='in_progress')",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN dim_ticket.status == 'hit' THEN ticket_id END)/COUNT(DISTINCT ticket_id) * 100)",
  },
  {
    sql_expression: 'array_distinct(flatten(ARRAY_AGG(customers)))',
  },
  {
    sql_expression: 'any_value(primary_part_id)',
  },
  {
    sql_expression: "COUNT(Distinct CASE WHEN status = 'miss' then id end)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/5WLaGLPW' THEN metric_set_value END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/Ds2FRuuX' THEN metric_set_value END)",
  },
  {
    sql_expression: 'SUM(new_users)',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN (dim_ticket.status == 'hit' OR dim_ticket.status == 'in_progress') THEN ticket_id END)/COUNT(DISTINCT ticket_id) * 100)",
  },
  {
    sql_expression: 'avg(curr_score_value)*100',
  },
  {
    sql_expression:
      "SUM(case when nodes = 'nan' then 0 else CAST(nodes AS INTEGER) end)",
  },
  {
    sql_expression: 'COUNT(dev_oid_su)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state = 'in_progress' THEN id END)",
  },
  {
    sql_expression: 'COALESCE(MEDIAN(first_resp_time), 0)',
  },
  {
    sql_expression: 'SUM(CAST(total_nodes AS INTEGER))',
  },
  {
    sql_expression:
      'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_resolution_time)',
  },
  {
    sql_expression:
      'CASE WHEN COALESCE(SUM(api_calls_count),0) = 0 THEN 100 ELSE ROUND(((SUM(api_calls_count) - COALESCE(SUM(api_server_error_count),0)) / SUM(api_calls_count)) * 100.0, 3) END',
  },
  {
    sql_expression:
      "count(CASE WHEN state='closed' AND te is true THEN ticket END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'U-Assist' THEN ticket END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Interact (Legacy)' THEN id END)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'tnt__product' = 'Q for Sales' THEN id END)  || ''",
  },
  {
    sql_expression: 'AVG(total_daily_active_users)',
  },
  {
    sql_expression: 'avg(weekly_distinct_revuser_logins)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN source_name = 'Inbound: Paid' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Code Review' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'completed' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'triage' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Problem Statement review' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Short Term Backlog' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Long Term Backlog' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/6cqTR14h' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "ANY_VALUE(datediff('day', support_insights_ticket_metrics_summary.modified_date, current_date))",
  },
  {
    sql_expression: 'ROUND(SUM(total_bytes_billed) * (6.25 / POWER(10, 12)),2)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN target_close_quarter = '2025-Q2' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/2' AND json_extract_string(metric, '$.status') == 'hit' THEN ticket_id END)*100/COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/2' THEN ticket_id END))",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-global:metric-set/1' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/1EYSCQzBT' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'PERCENTILE_DISC(0.90) WITHIN GROUP ( ORDER BY res_time)',
  },
  {
    sql_expression: 'median(res_time)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_JIRA' THEN duration END)/SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_JIRA' THEN count_sync END)",
  },
  {
    sql_expression:
      'ROUND(AVG(SUM(daily_active_users)) OVER (ORDER BY {MEERKAT}.created_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0)',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT(CASE WHEN metric_status = 'hit' THEN id END))* 100.0 / COUNT(DISTINCT(CASE WHEN metric_status = 'hit' OR metric_status = 'miss' THEN id END)))",
  },
  {
    sql_expression: 'MAX(owner_id)',
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER(WHERE stage_name in ('Internal UAT',   'In UAT',   'Live',   'QA on production',   'Test cases automated',   'Completed'))",
  },
  {
    sql_expression:
      "SUM(DISTINCT CASE WHEN custom_fields->>'ctype__total_alerts' IS NOT NULL THEN CAST(custom_fields->>'ctype__total_alerts' AS INT) ELSE 0 END)",
  },
  {
    sql_expression: 'COUNT(gng)',
  },
  {
    sql_expression: 'sum(cast(fleet_count as integer))',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Capture NG' THEN id END)  || ''",
  },
  {
    sql_expression: 'min(min_latency) / 1000',
  },
  {
    sql_expression: 'COUNT(*)',
  },
  {
    sql_expression: "COUNT(DISTINCT CASE WHEN state = 'closed' THEN id END)",
  },
  {
    sql_expression: "(COUNT(CASE WHEN status == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression: "COUNT(id)  || ''",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state!='closed' AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN current_date - CASE WHEN custom_fields ->> 'ctype__created_at_cfid' IS NOT NULL THEN Cast( custom_fields ->> 'ctype__created_at_cfid' AS TIMESTAMP) ELSE created_date END > interval '5' day AND state != 'closed' AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN JSON_EXTRACT(surveys_aggregation_json, '$[0].average') >= 4 THEN id END)/COUNT(id)*100",
  },
  {
    sql_expression: 'any_value(group_id)',
  },
  {
    sql_expression:
      "(count(CASE WHEN made_sla THEN id WHEN status='hit' THEN id END)/count(*))*100",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, '$[0].average') AS INT)) = '5' THEN id END) || ''",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/1BSaeBgMNN:metric_set/18aD8RPm' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'abs(avg(score_value))',
  },
  {
    sql_expression: 'COUNT(ID)',
  },
  {
    sql_expression: 'SUM("Deactivated")',
  },
  {
    sql_expression: 'SUM(total_session_length)',
  },
  {
    sql_expression: 'SUM(count_call)',
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN status = 'hit' THEN id END)/count(id)) * 100",
  },
  {
    sql_expression: 'any_value(csat_score)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN source_name = 'Outbound' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN source_name = 'Events: Industry Events' THEN acv ELSE 0 END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Ready for scoping' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Bad Quality Ticket' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'prioritized' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Ready for Merge' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Client dependency' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'High Level Solutioning' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Tech Doc Inprogress' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "printf('%.2f%%',SUM(CAST(total_prs_with_jest_tests as DOUBLE)) / SUM(CAST(total_prs as DOUBLE)) * 100)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/LzS6CEY7' THEN metric_set_value END)",
  },
  {
    sql_expression: "SUM(CASE WHEN retry_count = '1' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression:
      "COUNT(distinct case when custom_fields->>'ctype__support_priority' = 'High' then  id end)",
  },
  {
    sql_expression:
      'CASE WHEN COUNT(DISTINCT CASE WHEN total_responses > 0 THEN id END) > 0 THEN COUNT(DISTINCT CASE WHEN total_responses > 0 AND floor(sum_rating / NULLIF(total_responses, 0)) IN (4, 5) THEN id END) * 100.0 / COUNT(DISTINCT CASE WHEN total_responses > 0 THEN id END) ELSE NULL END',
  },
  {
    sql_expression: 'ROUND(AVG(mttr_hours))',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/ogNAgdmp:metric_set/8jZGgeBH' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'ANY_VALUE(worked_date)',
  },
  {
    sql_expression:
      "count(CASE WHEN state = 'in_progress' AND te is true THEN ticket END)",
  },
  {
    sql_expression:
      "   COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', created_date) = month THEN id END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/2JxwhYm6' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'AVG(completion_rate)',
  },
  {
    sql_expression:
      '(SUM(weighted_contribution) FILTER(ARRAY_LENGTH(tickets) > 0)) / SUM(weighted_contribution) * 100',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/tmD9r4ID:metric_set/15qMdjAza' THEN metric_set_value*100 END)",
  },
  {
    sql_expression: 'any_value(value)',
  },
  {
    sql_expression: "sum(case when state = 'closed' THEN 1 else 0 END)",
  },
  {
    sql_expression: 'sum(amount)',
  },
  {
    sql_expression: ' COUNT(id)',
  },
  {
    sql_expression: 'array_distinct(type)',
  },
  {
    sql_expression: 'SUM(total_comment_clicks)',
  },
  {
    sql_expression: 'SUM(unique_views)',
  },
  {
    sql_expression: 'AVG(merge_time)',
  },
  {
    sql_expression: 'sum(cast(point as integer))',
  },
  {
    sql_expression: 'sum(cast(arr as integer))',
  },
  {
    sql_expression: 'COUNT(distinct id_sh)',
  },
  {
    sql_expression:
      "COALESCE((SELECT value FROM (SELECT 'Orders with CN Value' as name, account_id as account, timestamp_nsecs, value FROM orders_with_cn_value UNION ALL SELECT 'SMS Sent' as name, account_id as account, timestamp_nsecs, value FROM sms_sent UNION ALL SELECT 'COD Orders Count' as name, account_id as account, timestamp_nsecs, value FROM cod_orders_count UNION ALL SELECT 'Reconciliation' as name, account_id as account, timestamp_nsecs, value FROM cod_reconciliation UNION ALL SELECT 'Rider Payout Count' as name, account_id as account, timestamp_nsecs, value FROM rider_payout_count UNION ALL SELECT 'Trip Count' as name, account_id as account, timestamp_nsecs, value FROM trip_count UNION ALL SELECT 'Consignment Count' as name, account_id as account, timestamp_nsecs, value FROM consignment_count) AS cc2 WHERE cc2.name = account_metrics__name AND CAST(CAST(cc2.timestamp_nsecs AS timestamp) AS date) = CAST(CURRENT_DATE - 1 AS date) ORDER BY timestamp_nsecs DESC LIMIT 1), 0)",
  },
  {
    sql_expression: 'count(distinct display_id)',
  },
  {
    sql_expression:
      '(SUM(total_second_resp_breaches) + SUM(total_first_resp_breaches)) * 100.0 / NULLIF(COUNT(total_resolution_breaches), 0)',
  },
  {
    sql_expression:
      'PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY time_to_approve)',
  },
  {
    sql_expression: 'count(distinct conversation)',
  },
  {
    sql_expression: 'any_value(severity_name)',
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/1' AND json_extract_string(metric, '$.status') == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN te is true AND state='closed' THEN ticket END)",
  },
  {
    sql_expression:
      "count(DISTINCT CASE WHEN DATE_TRUNC('day', last_message_timestamp) == DATE_TRUNC('day', record_date) AND state == 'closed' THEN conv END)",
  },
  {
    sql_expression: 'avg(response_time)',
  },
  {
    sql_expression: 'MAX(total_licenses)',
  },
  {
    sql_expression: '(sum(mtbf_days) / (count(*) - 1))',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/lG0eqLha' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/FhAaKJlu' THEN metric_set_value END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/IY5SGEDE' THEN metric_set_value END)",
  },
  {
    sql_expression: 'SUM("Open")',
  },
  {
    sql_expression: 'sum(daily_unique_conversations)',
  },
  {
    sql_expression:
      'CASE WHEN COUNT(id) > 1 THEN (SUM(mtbf_hours) / (COUNT(id) - 1)) ELSE null END',
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'completed' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'In UAT' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'QA on production' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/10qRijQBr' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/xXjPo9nF:metric_set/10qRijQBr' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      'SUM(total_resolution_breaches) + SUM(total_first_resp_breaches) + SUM(total_second_resp_breaches)',
  },
  {
    sql_expression:
      "CASE WHEN severity = 5 THEN 'Blocker' WHEN severity = 6 THEN 'High' WHEN severity = 7 THEN 'Medium' WHEN severity = 8 THEN 'Low' ELSE 'No Severity' END",
  },
  {
    sql_expression: 'sum(allocation)/count(distinct(timestamp))',
  },
  {
    sql_expression: 'median(completed_in)',
  },
  {
    sql_expression: '(COUNT(hour_of_day) * 100.0 / SUM(COUNT(id)) OVER ())',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/tmD9r4ID:metric_set/REgRIsII' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT closed_id) FILTER (closed_id IS NOT NULL)',
  },
  {
    sql_expression:
      "(CASE WHEN (opp_close_date <= (MAX(CASE WHEN date_diff('day', current_date(), opp_close_date) < 0 THEN opp_close_date END) OVER ())) THEN (SUM(SUM(actual_amount)) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING)) END)",
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='awaiting_product_assist' and 'don:core:dvrv-us-1:devo/0:tag/1606'=ANY(tag_ids) and severity_name='High' and state != 'closed'  AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: "format('${:t,}', SUM(amount)::INTEGER)",
  },
  {
    sql_expression:
      'ARRAY_AGG(DISTINCT id) FILTER (WHERE target_close_date::DATE < CURRENT_DATE )',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN metric_status='hit' THEN id END)/Count(DISTINCT CASE WHEN metric_status='hit' OR metric_status='miss' THEN id END) * 100",
  },
  {
    sql_expression:
      "ARRAY_AGG(DISTINCT id) FILTER ((product_effort_planned IS NULL )                  OR (target_start_date IS NULL OR target_start_date = '')                  OR (target_close_date IS NULL OR target_close_date = ''))",
  },
  {
    sql_expression:
      "printf('%.2f (%+.2f%%)', new_final_val, ((new_final_val - old_final_val) / old_final_val) * 100)",
  },
  {
    sql_expression:
      'CASE WHEN SUM(forecast_amount) IS NOT NULL THEN (SUM(SUM(forecast_amount)) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING) + (SUM(CASE WHEN SUM(actual_amount) IS NOT NULL THEN SUM(actual_amount) ELSE 0 END) OVER (ORDER BY opp_close_date ROWS UNBOUNDED PRECEDING))) END',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END)",
  },
  {
    sql_expression: 'ROUND(MEDIAN (resolution_time_hours),1)',
  },
  {
    sql_expression:
      "((COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '29 day' AND CURRENT_DATE THEN rev_oid ELSE NULL END)- COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '59 day' AND CURRENT_DATE - INTERVAL '29 day'THEN rev_oid ELSE NULL END))/COUNT(DISTINCT CASE WHEN day_start_date BETWEEN CURRENT_DATE - INTERVAL '29 day' AND CURRENT_DATE THEN rev_oid ELSE NULL END))*100",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'Q Sales' THEN id END)  || ''",
  },
  {
    sql_expression:
      "(COUNT(DISTINCT CASE WHEN dim_ticket.status == 'hit' OR dim_ticket.status == 'in_progress' THEN ticket_id END)/COUNT(DISTINCT ticket_id) * 100)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: 'COUNT(DISTINCT ticketId)',
  },
  {
    sql_expression:
      "COUNT(CASE WHEN custom_fields->>'ctype__x1260822517949_cfid' IS NULL and state !='closed' THEN id END)",
  },
  {
    sql_expression:
      "COUNT(Distinct CASE WHEN metric_status = 'miss' then id end)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN metric_status = 'miss' THEN id END)* 100.0 / COUNT(*))",
  },
  {
    sql_expression: 'COUNT(DISTINCT revu_id)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN id = 'don:identity:dvrv-us-1:devo/0:account/1F5aezyGo' THEN total_licenses ELSE 0 END)",
  },
  {
    sql_expression: 'any_value(account_id)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed' AND 'don:core:dvrv-us-1:devo/0:tag/1606' = ANY(tag_ids) AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression:
      "100.0 * (\nCOUNT(DISTINCT CASE WHEN state != 'closed' AND 'don:core:dvrv-us-1:devo/0:tag/508' = ANY(tag_ids) AND state IS NOT NULL THEN id END)\n/ NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0)\n)",
  },
  {
    sql_expression:
      "count(DISTINCT CASE WHEN stage_json->>'$.name' = 'triage' then id end)",
  },
  {
    sql_expression: 'SUM(CASE WHEN priority = 2 THEN 1 ELSE 0 END)',
  },
  {
    sql_expression: 'avg(completed_in)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE when state_distribution IS NOT NULL or state_distribution !='Slice' THEN id END)",
  },
  {
    sql_expression: 'SUM(count_def)',
  },
  {
    sql_expression: 'SUM(count_linkedin)',
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'In Dev' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'PROBLEM STATEMENT DEFINED' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Live' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression: 'MAX(affected_area)',
  },
  {
    sql_expression:
      "Count(DISTINCT CASE WHEN \nstage ='accepted' and 'don:core:dvrv-us-1:devo/0:tag/508'=ANY(tag_ids) and severity_name='High' AND state IS NOT NULL THEN id END)",
  },
  {
    sql_expression: 'count(1)',
  },
  {
    sql_expression:
      'SUM(CAST(lines_covered as DOUBLE)) / SUM(CAST(lines_total as DOUBLE)) * 100',
  },
  {
    sql_expression: "avg(date_diff('days',created_date,target_time))",
  },
  {
    sql_expression: "COUNT(distinct CASE WHEN state != 'closed' THEN id END)",
  },
  {
    sql_expression:
      "COUNT(case when custom_fields->>'ctype__support_priority' = 'Critical' then  id end)",
  },
  {
    sql_expression: 'count(sr_number)',
  },
  {
    sql_expression: "COUNT(*) FILTER(where state = 'closed')",
  },
  {
    sql_expression: 'SUM(annual_recurring_revenue)',
  },
  {
    sql_expression:
      'AVG(AVG(NULLIF(time_to_merge, 0))) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
  },
  {
    sql_expression:
      "CAST(AVG(datediff('day', dim_ticket.created_date, current_date)) AS INT) || ' days'",
  },
  {
    sql_expression:
      "SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_ADAAS' THEN duration END)/SUM(CASE WHEN external_system_type = 'ExternalSystemTypeEnum_ADAAS' THEN count_sync END)",
  },
  {
    sql_expression:
      "SUM(contributions) FILTER (type = 'github.pull_request.opened' OR type = 'bitbucket.pull_request.created')",
  },
  {
    sql_expression:
      'AVG(AVG(NULLIF(time_to_approve, 0))) OVER (ORDER BY {MEERKAT}.record_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/3irx4dJ6' THEN metric_set_value END)",
  },
  {
    sql_expression: 'SUM(total_queries)',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN sla_stage = 'breached' AND state != 'closed' THEN id END)",
  },
  {
    sql_expression: 'COUNT(negotiate)',
  },
  {
    sql_expression:
      "100.0 * (\n    COUNT(DISTINCT CASE \n        WHEN state != 'closed' AND \n           ( 'don:core:dvrv-us-1:devo/0:tag/2124' = ANY(tag_ids) OR 'don:core:dvrv-us-1:devo/0:tag/13375' = ANY(tag_ids)) AND \n            state IS NOT NULL  THEN id  END) \n    / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0))\n\n",
  },
  {
    sql_expression: 'sum(measure2)',
  },
  {
    sql_expression: 'AVG(avg_time_to_deploy)',
  },
  {
    sql_expression: 'AVG(time_to_deploy)',
  },
  {
    sql_expression: "ARRAY_AGG(id) FILTER(state = 'closed')",
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER(state IN ('open','in_progress') AND CURRENT_DATE()>target_close_date)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN state = 'closed' AND actual_start_date!='1970-01-01T00:00:00.000Z' AND actual_close_date != '1970-01-01T00: 00: 00.000Z'  THEN DATEDIFF('minute', actual_start_date, actual_close_date) END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'U-Analyze on X-Plat' THEN id END)  || ''",
  },
  {
    sql_expression:
      "SUM(CASE WHEN type not like '%attachments%' THEN created_in_devrev+created_in_external+updated_in_devrev+updated_in_external END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN array_length(responses, 1) < 2 AND stage = 'resolved' THEN 1 END) * 100.0 / COUNT(DISTINCT id)",
  },
  {
    sql_expression: 'max(max_latency) / 1000',
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'open' THEN datediff('day',created_date,current_date) END)",
  },
  {
    sql_expression: "(COUNT(CASE WHEN status == 'miss' THEN id END))",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') LIKE '%:metric_definition/2' AND json_extract_string(metric, '$.status') == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN state != 'closed'  AND state IS NOT NULL  THEN id END )",
  },
  {
    sql_expression: 'sum(rage_clicks) / count(*)',
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/IY5SGEDE' THEN metric_set_value*100 END)",
  },
  {
    sql_expression:
      "MAX(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/3fAHEC:metric_set/IY5SGEDE' THEN curr_metric_set_value*100 END)",
  },
  {
    sql_expression:
      "(COUNT(CASE WHEN json_extract_string(metric, '$.metric_definition_id') = 'don:core:dvrv-us-1:devo/0:metric_definition/1' AND json_extract_string(metric, '$.status') == 'miss' THEN ticket_id END))",
  },
  {
    sql_expression: 'COUNT(stage)',
  },
  {
    sql_expression:
      "count(distinct case when state = 'in_progress' then id when stage = 'resolved' then id end)",
  },
  {
    sql_expression:
      "count(distinct CASE WHEN te is true AND state='in_progress' THEN ticket END)",
  },
  {
    sql_expression:
      "count(distinct CASE WHEN te is true AND state='closed' THEN ticket END)",
  },
  {
    sql_expression: 'any_value(title)',
  },
  {
    sql_expression:
      "MAX(CASE WHEN stage_json ->> 'name' = 'Tech Approval Pending' THEN modified_date ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Archived' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'Scope review pending' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression:
      "Count(CASE WHEN stage_json ->> 'name' = 'QA on dev' THEN 1 ELSE NULL END)",
  },
  {
    sql_expression: 'SUM(CAST(physical_loc as INT))',
  },
  {
    sql_expression:
      "100.0 * (\n    COUNT(DISTINCT CASE \n        WHEN state != 'closed' AND \n            'don:core:dvrv-us-1:devo/0:tag/1606' = ANY(tag_ids) AND \n            state IS NOT NULL  THEN id  END) \n    / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0))\n\n",
  },
  {
    sql_expression:
      "AVG(CASE WHEN state = 'closed' THEN DATEDIFF('minute', created_date, actual_close_date) END)",
  },
  {
    sql_expression:
      "COUNT(DISTINCT CASE WHEN stage = 'completed' then id end)/Count(id) *100",
  },
  {
    sql_expression:
      "DAYNAME(CASE WHEN custom_fields ->> 'ctype__created_at_cfid' IS NOT NULL THEN Cast( custom_fields ->> 'ctype__created_at_cfid' AS TIMESTAMP) ELSE created_date END)",
  },
  {
    sql_expression:
      "STRING_SPLIT(TRANSLATE(JSON_EXTRACT(tags_json, '$[*].tag_id'), '\"[]', '' ), ',')",
  },
  {
    sql_expression: 'avg(licenses)',
  },
  {
    sql_expression: 'sum(deleted)',
  },
  {
    sql_expression:
      "SUM(CASE WHEN final_component = 'DevRev Loader' THEN duration END)/SUM(CASE WHEN final_component = 'DevRev Loader' THEN count_sync END)",
  },
  {
    sql_expression:
      "SUM(CASE WHEN final_component = 'RecipeManager' THEN duration END)/SUM(CASE WHEN final_component = 'RecipeManager' THEN count_sync END)",
  },
  {
    sql_expression:
      'ROUND(AVG(EXTRACT(EPOCH FROM (CAST(ticket_time_updated AS TIMESTAMP) - CAST(ticket_time_created AS TIMESTAMP))) / 3600), 2)',
  },
  {
    sql_expression: "sum(case when state = 'closed' THEN 1 ELSE 0 END)",
  },
  {
    sql_expression:
      'ARRAY_AGG( DISTINCT id) FILTER(WHERE target_close_date::DATE >= CURRENT_DATE AND target_close_date < sprint_end_date)',
  },
  {
    sql_expression:
      "ROUND(100.0 * COUNT( DISTINCT CASE WHEN stage_name in (  'Live',  'Ready For Deployment', 'QA on production',  'Completed') THEN id ELSE NULL END) / NULLIF(COUNT( DISTINCT id), 0), 2)",
  },
  {
    sql_expression: "count(case when visibility='EXTERNAL' then 1 end)",
  },
  {
    sql_expression:
      "format('${:t,}', (SUM(amount) FILTER(direction = 'Expense'))::INTEGER)",
  },
  {
    sql_expression: 'SUM(article_upvotes_change)',
  },
  {
    sql_expression: 'MEDIAN(duration_ms)/1000',
  },
  {
    sql_expression: 'ROUND(SUM(acv),2)',
  },
  {
    sql_expression: 'AVG(article_upvotes)',
  },
  {
    sql_expression:
      "ARRAY_AGG(id) FILTER( where state = 'closed' AND actual_close_date>target_close_date)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/5WLaGLPW' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      "AVG(CASE WHEN metric_set_id = 'don:core:dvrv-us-1:devo/0:metric_set/3irx4dJ6' THEN curr_metric_set_value END)",
  },
  {
    sql_expression:
      "COUNT(CASE WHEN issue_custom_fields->>'ctype__customfield_11497_cfid' = 'AI Services' THEN id END)  || ''",
  },
  {
    sql_expression: 'count(category)',
  },
]
  .filter((query) => query.sql_expression !== null)
  .map((query) => {
    return {
      sql_expression: query.sql_expression
        .replace(/\{/g, '(')
        .replace(/\}/g, ')'),
    };
  });
