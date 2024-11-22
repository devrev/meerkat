export const queries = [
  {
    sql_expression: 'ROUND(median_resolution_time_days,1)',
  },
];
// [
//   {
//     sql_expression: 'avg_avg_time_spent_on_dashboard_val',
//   },
//   {
//     sql_expression: 'actual_close_date',
//   },
//   {
//     sql_expression: 'response_status',
//   },
//   {
//     sql_expression: 'final_answer',
//   },
//   {
//     sql_expression: 'custom_category',
//   },
//   {
//     sql_expression: 'rating',
//   },
//   {
//     sql_expression: 'total_bytes_processed',
//   },
//   {
//     sql_expression: 'Q3_Customers',
//   },
//   {
//     sql_expression: 'component',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''day'', CAST(date as TIMESTAMP))",
//   },
//   {
//     sql_expression: 'project_type',
//   },
//   {
//     sql_expression:
//       "CONCAT( UPPER(SUBSTRING(state, 1, 1)), LOWER(REPLACE(SUBSTRING(state, 2), ''_'', '' '')) )",
//   },
//   {
//     sql_expression: 'ctype__x1260822517229_cfid',
//   },
//   {
//     sql_expression: 'activity_date',
//   },
//   {
//     sql_expression: 'acc_owned_by_ids',
//   },
//   {
//     sql_expression: 'actor',
//   },
//   {
//     sql_expression: 'page',
//   },
//   {
//     sql_expression: 'watchers',
//   },
//   {
//     sql_expression: 'focus_filter',
//   },
//   {
//     sql_expression:
//       'CASE WHEN actual_close_date > created_date THEN actual_close_date ELSE null END',
//   },
//   {
//     sql_expression: "DATETRUNC(''week'', record_date)",
//   },
//   {
//     sql_expression: 'customers_element',
//   },
//   {
//     sql_expression:
//       "case when status = ''hit'' then ''Hits'' when status = ''miss'' then ''Miss'' end",
//   },
//   {
//     sql_expression: 'issue_close_count',
//   },
//   {
//     sql_expression: 'CAST(partition_ts AS DATE)',
//   },
//   {
//     sql_expression:
//       "STRING_SPLIT(TRANSLATE(CAST(json_extract(tags_json, ''$[*].tag_id'') AS STRING), CHR(34) || ''[]'', ''''), '','')",
//   },
//   {
//     sql_expression: 'mean_unhealthy_days',
//   },
//   {
//     sql_expression: 'p90_latency',
//   },
//   {
//     sql_expression: 'severity',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state = ''open'' THEN ''Open'' WHEN state = ''in_progress'' THEN ''In Progress'' WHEN state = ''closed'' THEN ''Closed'' END",
//   },
//   {
//     sql_expression: 'share_count_val',
//   },
//   {
//     sql_expression: 'vista_report_dashboard_created_count_val',
//   },
//   {
//     sql_expression: 'completing_for',
//   },
//   {
//     sql_expression: 'dim_ticket_group',
//   },
//   {
//     sql_expression: 'product_manager',
//   },
//   {
//     sql_expression: 'addition_type',
//   },
//   {
//     sql_expression: 'valid_objects',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''day'',close_date)",
//   },
//   {
//     sql_expression: 'end_time',
//   },
//   {
//     sql_expression: "IF(region_name IS NULL ,''Other'',region_name)",
//   },
//   {
//     sql_expression: 'flag',
//   },
//   {
//     sql_expression: 'issue_product',
//   },
//   {
//     sql_expression: "custom_fields->>''tnt__closed_by''",
//   },
//   {
//     sql_expression: 'NULLIF(avg_time_to_deploy, 0)',
//   },
//   {
//     sql_expression:
//       "concat(monthname(activity_date),''-'',year(activity_date))",
//   },
//   {
//     sql_expression: 'applies_to_version_ids',
//   },
//   {
//     sql_expression: "date_trunc(''day'', last_internal_comment_date)",
//   },
//   {
//     sql_expression: 'watcher',
//   },
//   {
//     sql_expression: "cast(replace(partition_ts, ''0024'', ''2024'') as date)",
//   },
//   {
//     sql_expression: 'valid_qnas',
//   },
//   {
//     sql_expression: 'inbound_organic_total_acv',
//   },
//   {
//     sql_expression:
//       "(CASE WHEN ''issue'' == ANY(json_extract_string(dim_ticket.links_json,''$[*].target_object_type'')) THEN ''yes'' ELSE ''no'' END)",
//   },
//   {
//     sql_expression: 'sdr',
//   },
//   {
//     sql_expression: 'pod_Element',
//   },
//   {
//     sql_expression: 'job_id',
//   },
//   {
//     sql_expression: 'week_start_date',
//   },
//   {
//     sql_expression: 'cast(ticket_time_created as date)',
//   },
//   {
//     sql_expression: 'test',
//   },
//   {
//     sql_expression: 'enhancement',
//   },
//   {
//     sql_expression: 'formatted_created_date',
//   },
//   {
//     sql_expression: 'widget_created_count_val',
//   },
//   {
//     sql_expression: 'avg_median_time_spent_on_dashboard_val',
//   },
//   {
//     sql_expression: 'avg_max_time_spent_on_dashboard_val',
//   },
//   {
//     sql_expression: 'retrieved_sources',
//   },
//   {
//     sql_expression: 'stage_id',
//   },
//   {
//     sql_expression: 'event_name',
//   },
//   {
//     sql_expression: "date_trunc(''day'',close_date)",
//   },
//   {
//     sql_expression: 'project',
//   },
//   {
//     sql_expression: 'meeting_organizer',
//   },
//   {
//     sql_expression: "STRING_TO_ARRAY(members_attended,'','')",
//   },
//   {
//     sql_expression: 'CAST(ticket_time_updated AS TIMESTAMP)',
//   },
//   {
//     sql_expression: 'CAST(ticket_time_updated AS DATE)',
//   },
//   {
//     sql_expression: "DATE_DIFF(''minute'', created_date, first_response_time)",
//   },
//   {
//     sql_expression: 'cumulative_user_count',
//   },
//   {
//     sql_expression: 'opp_type',
//   },
//   {
//     sql_expression: 'devu_id',
//   },
//   {
//     sql_expression: 'account_id_dr',
//   },
//   {
//     sql_expression: 'sales_data_dr.customer',
//   },
//   {
//     sql_expression: 'sales_data_dr.start_time',
//   },
//   {
//     sql_expression: 'issue',
//   },
//   {
//     sql_expression: 'gpu_type',
//   },
//   {
//     sql_expression: 'dev_oid',
//   },
//   {
//     sql_expression: 'is_customer_account',
//   },
//   {
//     sql_expression: 'COALESCE(day(ttimestamp), day(pv_timestamp))',
//   },
//   {
//     sql_expression:
//       "case when state = ''in_progress'' then ''Active'' when stage = ''resolved'' then ''Closed'' else null end",
//   },
//   {
//     sql_expression: 'filter_part',
//   },
//   {
//     sql_expression: 'slug',
//   },
//   {
//     sql_expression: 'owners',
//   },
//   {
//     sql_expression: 'AD Time',
//   },
//   {
//     sql_expression: 'account_tags',
//   },
//   {
//     sql_expression: 'time_spent_mins',
//   },
//   {
//     sql_expression:
//       "json_extract_string(dim_ticket.stage_json, ''$.stage_id'')",
//   },
//   {
//     sql_expression:
//       "CAST(CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN custom_fields ->> ''ctype__created_at_cfid'' WHEN custom_fields ->> ''ctype__opened_at_cfid'' IS NOT NULL THEN custom_fields ->> ''ctype__opened_at_cfid'' ELSE created_date END AS TIMESTAMP)",
//   },
//   {
//     sql_expression: 'health_state_duration_max',
//   },
//   {
//     sql_expression: 'pipeline_arr',
//   },
//   {
//     sql_expression: 'May_Customers',
//   },
//   {
//     sql_expression: 'August_ID',
//   },
//   {
//     sql_expression: 'issue_creation_count',
//   },
//   {
//     sql_expression: 'issue_created_date',
//   },
//   {
//     sql_expression: 'hour(timestamp)',
//   },
//   {
//     sql_expression: 'percent_unhealthy_days',
//   },
//   {
//     sql_expression: 'min_time_taken_to_create_widget',
//   },
//   {
//     sql_expression: 'min_time_taken_to_update_widget',
//   },
//   {
//     sql_expression: 'article_count',
//   },
//   {
//     sql_expression:
//       "translate(JSON_EXTRACT(stage_json, ''$.stage_id''), CHR(34), '''')",
//   },
//   {
//     sql_expression: 'total_survey_dispatched',
//   },
//   {
//     sql_expression: "''Total''",
//   },
//   {
//     sql_expression: 'issue_closed_date',
//   },
//   {
//     sql_expression: "json_extract_string(labels, ''$[1].value'')",
//   },
//   {
//     sql_expression: 'total_bytes_billed',
//   },
//   {
//     sql_expression: 'tnt__bmc',
//   },
//   {
//     sql_expression: 'avg_fleet_time',
//   },
//   {
//     sql_expression: 'groups',
//   },
//   {
//     sql_expression: 'created_date_quarter',
//   },
//   {
//     sql_expression:
//       "CASE WHEN actual_close_date > created_date THEN DATEDIFF(''minutes'',actual_close_date, created_date) END",
//   },
//   {
//     sql_expression: 'cast(applies_to_part_ids as varchar)',
//   },
//   {
//     sql_expression:
//       "DATE_TRUNC(''day'',CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END)",
//   },
//   {
//     sql_expression: 'api',
//   },
//   {
//     sql_expression: "CONCAT(MONTHNAME(date),''-'',YEAR(date))",
//   },
//   {
//     sql_expression: 'ROUND(median_resolution_time_days,1)',
//   },
//   {
//     sql_expression: 'part_summary_filter',
//   },
//   {
//     sql_expression:
//       "regexp_split_to_array(custom_fields ->> ''tnt__customer_segment'', ''\"|\\[\"|\"\\]'')",
//   },
//   {
//     sql_expression: 'commenter',
//   },
//   {
//     sql_expression: 'ticket_prioritized',
//   },
//   {
//     sql_expression: 'secondary_owner_devu',
//   },
//   {
//     sql_expression: 'test_name',
//   },
//   {
//     sql_expression: 'api_server_error_count',
//   },
//   {
//     sql_expression: 'avg_members',
//   },
//   {
//     sql_expression: 'UNNEST(impacted_customers)',
//   },
//   {
//     sql_expression:
//       "COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP",
//   },
//   {
//     sql_expression: 'tag_ids',
//   },
//   {
//     sql_expression: 'primary_part_id',
//   },
//   {
//     sql_expression: 'sprint_id',
//   },
//   {
//     sql_expression: 'external_ticket_type',
//   },
//   {
//     sql_expression: 'dim_issue.subtype',
//   },
//   {
//     sql_expression: 'creation_time',
//   },
//   {
//     sql_expression:
//       "CASE WHEN primary_part_id LIKE ''%enhancement%'' OR is_issue_linked = ''yes'' THEN ''yes'' ELSE ''no'' END",
//   },
//   {
//     sql_expression: 'acv',
//   },
//   {
//     sql_expression: 'Percentage_Delivered',
//   },
//   {
//     sql_expression: 'role_name',
//   },
//   {
//     sql_expression: 'scheduled_quarter',
//   },
//   {
//     sql_expression: 'cast(cast(timestamp_nsecs as TIMESTAMP) as DATE)',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''day'',created_date)",
//   },
//   {
//     sql_expression: 'submission_date',
//   },
//   {
//     sql_expression: 'ctype__x1260822517949_cfid::varchar',
//   },
//   {
//     sql_expression: 'reported_by',
//   },
//   {
//     sql_expression: 'family_name',
//   },
//   {
//     sql_expression: 'destination',
//   },
//   {
//     sql_expression: 'csat_rating',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state = ''closed'' THEN (issue_closed_date)::TIMESTAMP ELSE issue_date::TIMESTAMP END",
//   },
//   {
//     sql_expression: 'daily_distinct_revuser_logins',
//   },
//   {
//     sql_expression: 'health_state_duration_count',
//   },
//   {
//     sql_expression: 'monthly_distinct_devuser_logins',
//   },
//   {
//     sql_expression: 'July_Customers',
//   },
//   {
//     sql_expression: 'category_order',
//   },
//   {
//     sql_expression: 'app_launch_type',
//   },
//   {
//     sql_expression: 'p50_latency',
//   },
//   {
//     sql_expression:
//       "COALESCE(custom_fields->>''ctype__closed_at_cfid'',actual_close_date)",
//   },
//   {
//     sql_expression: 'owned_by_id',
//   },
//   {
//     sql_expression: 'widget_updated_count_val',
//   },
//   {
//     sql_expression: 'avg_min_time_spent_on_dashboard_val',
//   },
//   {
//     sql_expression: 'account_ids',
//   },
//   {
//     sql_expression: 'stage_name',
//   },
//   {
//     sql_expression: 'stage',
//   },
//   {
//     sql_expression: 'custom_company_lookup',
//   },
//   {
//     sql_expression: 'ae_name',
//   },
//   {
//     sql_expression: "''issue''",
//   },
//   {
//     sql_expression: 'api_response_time',
//   },
//   {
//     sql_expression: 'value',
//   },
//   {
//     sql_expression: 'tnt__actual_product_release',
//   },
//   {
//     sql_expression: 'opp_id',
//   },
//   {
//     sql_expression: "CASE WHEN is_internal THEN ''Yes'' ELSE ''No'' END",
//   },
//   {
//     sql_expression:
//       "STRING_SPLIT(TRANSLATE(JSON_EXTRACT(tags_json, ''$[*].tag_id''), ''\"[]'', '''' ), '','')",
//   },
//   {
//     sql_expression: 'ticket_group',
//   },
//   {
//     sql_expression: 'user_percentage*100',
//   },
//   {
//     sql_expression: 'tkt_id',
//   },
//   {
//     sql_expression:
//       "DAYNAME(CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END)",
//   },
//   {
//     sql_expression: " CAST(strftime(''%H'', created_date) AS VARCHAR)",
//   },
//   {
//     sql_expression: 'started_at',
//   },
//   {
//     sql_expression: 'sales_data_dr.Tracking_',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state_su = ''SyncUnit_StateEnumCompleted'' THEN ''Completed'' WHEN state_su = ''SyncUnit_StateEnumFailed'' THEN ''Failed'' WHEN state_su = ''SyncUnit_StateEnumRecipeDiscoveryWaitingForUserInput'' THEN ''WaitingForUserInput'' WHEN state_su = ''SyncUnit_StateEnumInProgress'' THEN ''InProgress'' WHEN state_su = ''SyncUnit_StateEnumSyncInProgress'' THEN ''SyncInProgress'' WHEN state_su = ''SyncUnit_StateEnumDeleteInProgress'' THEN ''DeleteInProgress'' WHEN state_su = ''SyncUnit_StateEnumDeletePending'' THEN ''DeletePending'' END",
//   },
//   {
//     sql_expression: 'product',
//   },
//   {
//     sql_expression: 'trial_start_time',
//   },
//   {
//     sql_expression: 'user_group',
//   },
//   {
//     sql_expression: 'overall_time',
//   },
//   {
//     sql_expression: 'bookings_goal',
//   },
//   {
//     sql_expression: 'capability',
//   },
//   {
//     sql_expression: 'CAST(SUBSTRING(partition_ts_30m, 1, 10) AS TIMESTAMP)',
//   },
//   {
//     sql_expression: 'sprint_end_date',
//   },
//   {
//     sql_expression: 'total_periods',
//   },
//   {
//     sql_expression: 'max_time_repair',
//   },
//   {
//     sql_expression: 'subtype',
//   },
//   {
//     sql_expression: 'COALESCE(created_at, opened_at, created_date)::TIMESTAMP',
//   },
//   {
//     sql_expression: 'CAST (category_name_age AS VARCHAR)',
//   },
//   {
//     sql_expression: 'close_date',
//   },
//   {
//     sql_expression: 'api_hits',
//   },
//   {
//     sql_expression: 'impacted_customers',
//   },
//   {
//     sql_expression: 'skills',
//   },
//   {
//     sql_expression: 'priority',
//   },
//   {
//     sql_expression: 'domains',
//   },
//   {
//     sql_expression: 'BMC_Element',
//   },
//   {
//     sql_expression: 'hardware_shape',
//   },
//   {
//     sql_expression: 'cast(cast(record_date as TIMESTAMP) as DATE)',
//   },
//   {
//     sql_expression: 'pr_reviewed_time',
//   },
//   {
//     sql_expression:
//       "CASE WHEN issue_subtype = ''njuvewk7oxvgs4din5zgkltborygc43tnfqy4ltomx2f6mjrgmwtav3jonzvkzltfzss243xobwg64tu'' THEN ''E-Support'' WHEN issue_subtype = ''jira_uniphore.atlassian.net_engineering bug intake_issues.bug'' THEN ''E-Bug'' END",
//   },
//   {
//     sql_expression: 'owned_by_ids::varchar',
//   },
//   {
//     sql_expression: 'dayofname',
//   },
//   {
//     sql_expression: 'previous_stage_id',
//   },
//   {
//     sql_expression: 'tnt__csp_region',
//   },
//   {
//     sql_expression:
//       "CAST(json_extract_string(tags_json, ''$[*].tag_id'') AS VARCHAR[])",
//   },
//   {
//     sql_expression: "cast(replace(date_broken, ''0024'', ''2024'') as date)",
//   },
//   {
//     sql_expression: 'approved_date',
//   },
//   {
//     sql_expression: 'events_effortless_total_acv',
//   },
//   {
//     sql_expression: 'group_conversation',
//   },
//   {
//     sql_expression:
//       "regexp_split_to_array(tnt__sub_regions, ''\"|\\[\"|\"\\]'')",
//   },
//   {
//     sql_expression:
//       "DATE_TRUNC(''week'', COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP)",
//   },
//   {
//     sql_expression: 'mao',
//   },
//   {
//     sql_expression: 'user_activity_date',
//   },
//   {
//     sql_expression: 'acc.domain',
//   },
//   {
//     sql_expression:
//       "CAST(JSON_EXTRACT(custom_fields, ''$.ctype__amount'') AS DOUBLE)",
//   },
//   {
//     sql_expression: 'fiscal_time',
//   },
//   {
//     sql_expression:
//       "CASE WHEN severity = ''5'' THEN ''Sev 1'' WHEN severity = ''6'' THEN ''Sev 2'' WHEN severity = ''7'' THEN ''Sev 3'' WHEN severity = ''8'' THEN ''Sev 4'' END",
//   },
//   {
//     sql_expression: 'engagement_count',
//   },
//   {
//     sql_expression: 'exception_type',
//   },
//   {
//     sql_expression: 'is_rage',
//   },
//   {
//     sql_expression: 'num_rev_users',
//   },
//   {
//     sql_expression: "date_trunc(''week'', created_date)",
//   },
//   {
//     sql_expression: 'custom_region',
//   },
//   {
//     sql_expression: 'ticket',
//   },
//   {
//     sql_expression: 'avg_min_time_taken_to_create_dashboard_val',
//   },
//   {
//     sql_expression: 'surface',
//   },
//   {
//     sql_expression: 'created_dayname',
//   },
//   {
//     sql_expression: 'qnas_retrieved',
//   },
//   {
//     sql_expression: 'page_title',
//   },
//   {
//     sql_expression:
//       "CASE WHEN links_json like ''%\"target_object_type\":\"issue\"%'' THEN ''Yes'' ELSE ''No'' END",
//   },
//   {
//     sql_expression: 'customer_ref',
//   },
//   {
//     sql_expression: 'statement_type',
//   },
//   {
//     sql_expression: "json_extract_string(labels, ''$[0].value'')",
//   },
//   {
//     sql_expression: 'elapsed_times',
//   },
//   {
//     sql_expression: 'created_quarter',
//   },
//   {
//     sql_expression: 'Features_Delivered',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state = ''SyncProgress_StateEnumExtractAttachments'' THEN ''ExtractAttachments'' WHEN state = ''SyncProgress_StateEnumExtractAttachmentsPending'' THEN ''ExtractAttachmentsPending'' WHEN state = ''SyncProgress_StateEnumExtraction'' THEN ''Extraction'' WHEN state = ''SyncProgress_StateEnumExtractionPending'' THEN ''ExtractionPending'' WHEN state = ''SyncProgress_StateEnumLoading'' THEN ''Loading'' WHEN state = ''SyncProgress_StateEnumLoadingPending'' THEN ''LoadingPending'' WHEN state = ''SyncProgress_StateEnumLoadingAttachments'' THEN ''LoadingAttachments'' WHEN state = ''SyncProgress_StateEnumLoadingAttachmentsPending'' THEN ''LoadingAttachmentsPending'' WHEN state = ''SyncProgress_StateEnumLoadingPending'' THEN ''LoadingPending'' WHEN state = ''SyncProgress_StateEnumRecipeDiscovery'' THEN ''RecipeDiscovery'' WHEN state = ''SyncProgress_StateEnumRecipeDiscoveryPending'' THEN ''RecipeDiscoveryPending'' WHEN state = ''SyncProgress_StateEnumStartingPending'' THEN ''StartingPending'' WHEN state = ''SyncProgress_StateEnumTransformation'' THEN ''Transformation'' WHEN state = ''SyncProgress_StateEnumTransformationPending'' THEN ''TransformationPending'' END",
//   },
//   {
//     sql_expression: 'breached_days',
//   },
//   {
//     sql_expression:
//       "STRING_SPLIT(TRANSLATE(json_extract(tags_json, ''$[*].tag_id''), CHR(34) || ''[]'' || '' '', ''''), '','')",
//   },
//   {
//     sql_expression: 'total_code_contributions',
//   },
//   {
//     sql_expression: 'node_status',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''month'', started_at_sh)",
//   },
//   {
//     sql_expression: 'user_type',
//   },
//   {
//     sql_expression: 'billing_cycle',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state = ''closed'' THEN actual_close_date::TIMESTAMP END",
//   },
//   {
//     sql_expression: "DATE_TRUNC(''day'', record_date)",
//   },
//   {
//     sql_expression: "DATE_TRUNC(''day'', created_date)",
//   },
//   {
//     sql_expression: 'utm_medium',
//   },
//   {
//     sql_expression: 'min_upstream_service_time',
//   },
//   {
//     sql_expression: "JSON_EXTRACT(custom_fields, ''$.ctype__currency'')",
//   },
//   {
//     sql_expression: 'April_Customers',
//   },
//   {
//     sql_expression: 'May_ID',
//   },
//   {
//     sql_expression: 'csp',
//   },
//   {
//     sql_expression: 'gpu_id',
//   },
//   {
//     sql_expression: 'event',
//   },
//   {
//     sql_expression: 'mtbf_hours',
//   },
//   {
//     sql_expression: 'fact_issue.applies_to_part_id',
//   },
//   {
//     sql_expression: 'appVersion',
//   },
//   {
//     sql_expression: 'max_latency',
//   },
//   {
//     sql_expression: 'id',
//   },
//   {
//     sql_expression: 'session_id',
//   },
//   {
//     sql_expression: 'tnt__regions',
//   },
//   {
//     sql_expression: 'access_level',
//   },
//   {
//     sql_expression: 'request_id',
//   },
//   {
//     sql_expression: 'opp_full_id',
//   },
//   {
//     sql_expression: 'work_type',
//   },
//   {
//     sql_expression: 'department',
//   },
//   {
//     sql_expression: 'display_id',
//   },
//   {
//     sql_expression: 'browser_name',
//   },
//   {
//     sql_expression: 'sprint_board_name',
//   },
//   {
//     sql_expression: 'latest_part_id',
//   },
//   {
//     sql_expression: 'member_id',
//   },
//   {
//     sql_expression: 'Q1_ID',
//   },
//   {
//     sql_expression: 'CAST(ticket_time_created AS DATE)',
//   },
//   {
//     sql_expression: 'actual_close_quarter',
//   },
//   {
//     sql_expression: 'issue_id',
//   },
//   {
//     sql_expression: 'pr_url',
//   },
//   {
//     sql_expression:
//       "CASE WHEN issue_state = ''closed'' THEN (issue_actual_close_date)::TIMESTAMP ELSE issue_created_date::TIMESTAMP END",
//   },
//   {
//     sql_expression:
//       "CASE WHEN (range_state=''closed'') THEN ''E-bug Closed'' ELSE ''E-bug Opened'' END",
//   },
//   {
//     sql_expression:
//       " CASE WHEN external_system_type_su = ''ExternalSystemTypeEnum_ZENDESK'' THEN ''ZENDESK'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_JIRA'' THEN ''JIRA'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_HUBSPOT'' THEN ''HUBSPOT'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_GITHUB'' THEN ''GITHUB'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_SERVICENOW'' THEN ''SERVICENOW'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_SALESFORCE_SERVICE'' THEN ''SALESFORCE'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_ADAAS'' THEN ''ADAAS'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_LINEAR'' THEN ''LINEAR'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_ROCKETLANE'' THEN ''ROCKETLANE'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_CONFLUENCE'' THEN ''CONFLUENCE'' END",
//   },
//   {
//     sql_expression: 'country',
//   },
//   {
//     sql_expression: 'CAST(partition_ts AS TIMESTAMP)',
//   },
//   {
//     sql_expression: 'part_version_ids',
//   },
//   {
//     sql_expression: 'outbound_total_acv',
//   },
//   {
//     sql_expression:
//       "(CASE WHEN links_json != ''[]'' THEN ''yes'' ELSE ''no'' END)",
//   },
//   {
//     sql_expression: '"range"',
//   },
//   {
//     sql_expression: 'forecast_enum',
//   },
//   {
//     sql_expression:
//       "CASE WHEN actual_close_date> created_date THEN DATE_TRUNC(''week'', actual_close_date) ELSE null END",
//   },
//   {
//     sql_expression: 'awaiting_product_assist_duration_days',
//   },
//   {
//     sql_expression: 'participant_oid',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state = ''open'' THEN ''Open'' WHEN state = ''in_progress'' THEN ''In Progress'' END",
//   },
//   {
//     sql_expression: 'total_new_revusers',
//   },
//   {
//     sql_expression: 'importance',
//   },
//   {
//     sql_expression: 'pipeline_arr/bookings_goal',
//   },
//   {
//     sql_expression: 'unnest(impacted_customers)',
//   },
//   {
//     sql_expression: 'network_operator',
//   },
//   {
//     sql_expression:
//       "TRIM(CHR(34) FROM JSON_EXTRACT(custom_fields, ''$.tnt__sales_engineer''))",
//   },
//   {
//     sql_expression: 'widget_edit_clicked_count_val',
//   },
//   {
//     sql_expression: 'tnt__sub_regions',
//   },
//   {
//     sql_expression:
//       "CASE WHEN metric_status = ''hit'' THEN ''Completed'' WHEN metric_status = ''miss'' THEN ''Breached'' END",
//   },
//   {
//     sql_expression: 'week_start',
//   },
//   {
//     sql_expression: 'modified_by_id',
//   },
//   {
//     sql_expression: 'hits',
//   },
//   {
//     sql_expression: 'transition_end_date',
//   },
//   {
//     sql_expression: 'priority_stage',
//   },
//   {
//     sql_expression: 'tnt__planned_product_release',
//   },
//   {
//     sql_expression: 'Q4_Customers',
//   },
//   {
//     sql_expression: 'date_group',
//   },
//   {
//     sql_expression: 'trip_count',
//   },
//   {
//     sql_expression: 'bot_resolution_rate',
//   },
//   {
//     sql_expression: 'is_ticket_linked',
//   },
//   {
//     sql_expression:
//       "CAST( strftime(''%H'', CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END) AS VARCHAR)",
//   },
//   {
//     sql_expression: 'sales_data_dr.qty',
//   },
//   {
//     sql_expression: 'sales_data_dr.Ship_Date',
//   },
//   {
//     sql_expression: 'total_lines_deleted',
//   },
//   {
//     sql_expression: 'is_devrev_app_activity',
//   },
//   {
//     sql_expression: "date_trunc(''day'', created_date)",
//   },
//   {
//     sql_expression: 'source_name',
//   },
//   {
//     sql_expression: 'created_day',
//   },
//   {
//     sql_expression: 'in_development_duration_days',
//   },
//   {
//     sql_expression: 'ID Time',
//   },
//   {
//     sql_expression: 'utm_content',
//   },
//   {
//     sql_expression: 'signup_count',
//   },
//   {
//     sql_expression: 'mao_change_percentage',
//   },
//   {
//     sql_expression: 'cast(date_in_state as int)',
//   },
//   {
//     sql_expression: 'week_of_year',
//   },
//   {
//     sql_expression: 'breach_percentage',
//   },
//   {
//     sql_expression: 'total_tickets',
//   },
//   {
//     sql_expression: 'old_stage_name',
//   },
//   {
//     sql_expression: 'closed_date',
//   },
//   {
//     sql_expression: 'CAST(unhealthy_since AS TIMESTAMP)',
//   },
//   {
//     sql_expression: "JSON_EXTRACT(custom_fields, ''$.ctype__vendor'')",
//   },
//   {
//     sql_expression: 'deviceType',
//   },
//   {
//     sql_expression:
//       "DATETRUNC(''month'', COALESCE(custom_fields ->> ''ctype__created_at_cfid'', created_date)::TIMESTAMP )+ interval ''1 day''",
//   },
//   {
//     sql_expression:
//       "CAST(json_extract(fact_issue.priority_uenum_json, ''$.id'') as integer)",
//   },
//   {
//     sql_expression: "datediff(''day'', formatted_created_date, current_date)",
//   },
//   {
//     sql_expression: 'first_user_interaction',
//   },
//   {
//     sql_expression: 'error_count',
//   },
//   {
//     sql_expression: 'widget_load_count_val',
//   },
//   {
//     sql_expression: 'needs_response',
//   },
//   {
//     sql_expression: 'custom_country',
//   },
//   {
//     sql_expression: 'end_of_week',
//   },
//   {
//     sql_expression: 'start_time',
//   },
//   {
//     sql_expression: 'opp_close_date',
//   },
//   {
//     sql_expression: 'Q1_Customers',
//   },
//   {
//     sql_expression:
//       "CASE WHEN customer_type = ''paying'' THEN ''Paying'' ELSE ''Not Paying'' END",
//   },
//   {
//     sql_expression: 'first_account_id',
//   },
//   {
//     sql_expression:
//       " CASE WHEN mode_sh = ''SyncRun_ModeEnumInitial'' THEN ''Initial'' WHEN mode_sh = ''SyncRun_ModeEnumSyncToDevrev'' THEN ''SyncToDevRev'' WHEN mode_sh = ''SyncRun_ModeEnumSyncFromDevrev'' THEN ''SyncFromDevRev'' END",
//   },
//   {
//     sql_expression: "DATE_TRUNC(''month'',started_at)",
//   },
//   {
//     sql_expression: 'article_id',
//   },
//   {
//     sql_expression: "CONCAT(monthname(date),''-'',year(date))",
//   },
//   {
//     sql_expression: 'custom_schema_fragment_ids',
//   },
//   {
//     sql_expression: 'sla_tracker_id',
//   },
//   {
//     sql_expression:
//       "list_aggregate((CAST(json_extract_string(surveys_aggregation_json, ''$[*].average'') as integer[])), ''sum'')/len((CAST(json_extract_string(surveys_aggregation_json, ''$[*].average'') as integer[])))",
//   },
//   {
//     sql_expression:
//       "CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN custom_fields ->> ''ctype__created_at_cfid'' WHEN custom_fields ->> ''ctype__opened_at_cfid'' IS NOT NULL THEN custom_fields ->> ''ctype__opened_at_cfid'' ELSE created_date END",
//   },
//   {
//     sql_expression: 'Total_Client_Features_Planned',
//   },
//   {
//     sql_expression: 'latest_contribution_date',
//   },
//   {
//     sql_expression: 'created_tickets',
//   },
//   {
//     sql_expression: "concat(breach_percentage, ''%'')",
//   },
//   {
//     sql_expression: 'utm_term',
//   },
//   {
//     sql_expression: 'CAST(date_broken AS DATE)',
//   },
//   {
//     sql_expression:
//       " CASE WHEN external_system_type = ''ExternalSystemTypeEnum_ZENDESK'' THEN ''ZENDESK'' WHEN external_system_type = ''ExternalSystemTypeEnum_JIRA'' THEN ''JIRA'' WHEN external_system_type = ''ExternalSystemTypeEnum_HUBSPOT'' THEN ''HUBSPOT'' WHEN external_system_type = ''ExternalSystemTypeEnum_GITHUB'' THEN ''GITHUB'' WHEN external_system_type = ''ExternalSystemTypeEnum_SERVICENOW'' THEN ''SERVICENOW'' WHEN external_system_type = ''ExternalSystemTypeEnum_SALESFORCE_SERVICE'' THEN ''SALESFORCE'' WHEN external_system_type = ''ExternalSystemTypeEnum_ADAAS'' THEN ''ADAAS'' WHEN external_system_type = ''ExternalSystemTypeEnum_LINEAR'' THEN ''LINEAR'' WHEN external_system_type = ''ExternalSystemTypeEnum_ROCKETLANE'' THEN ''ROCKETLANE'' WHEN external_system_type = ''ExternalSystemTypeEnum_CONFLUENCE'' THEN ''CONFLUENCE'' END",
//   },
//   {
//     sql_expression: 'arr_booked',
//   },
//   {
//     sql_expression: 'fact_issue.subtype',
//   },
//   {
//     sql_expression: 'max_unhealthy_days',
//   },
//   {
//     sql_expression: 'CAST(record_time AS DATE)',
//   },
//   {
//     sql_expression: 'funnel_platform',
//   },
//   {
//     sql_expression: 'object_type',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state = ''closed'' THEN COALESCE(custom_fields->>''ctype__closed_at_cfid'',actual_close_date)::TIMESTAMP ELSE COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP END",
//   },
//   {
//     sql_expression: 'latam_sum_acv',
//   },
//   {
//     sql_expression:
//       "REPLACE(UPPER(SUBSTR(state, 1, 1)) || LOWER(SUBSTR(state, 2)), ''_'', '' '')",
//   },
//   {
//     sql_expression: 'api_url',
//   },
//   {
//     sql_expression: 'amount',
//   },
//   {
//     sql_expression: 'description',
//   },
//   {
//     sql_expression: "DATETRUNC(''month'', actual_close_date)",
//   },
//   {
//     sql_expression: 'workspace_name',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''day'', actual_close_date)",
//   },
//   {
//     sql_expression: 'trend_record_date',
//   },
//   {
//     sql_expression: 'sales_data_dr.record_id',
//   },
//   {
//     sql_expression: 'curr_timestamp',
//   },
//   {
//     sql_expression: 'NULLIF(time_to_review, 0)',
//   },
//   {
//     sql_expression:
//       "regexp_split_to_array(tag->>''tag_id'', ''\"|\\[\"|\"\\]'')",
//   },
//   {
//     sql_expression: 'other_total_acv',
//   },
//   {
//     sql_expression: 'avg_issues',
//   },
//   {
//     sql_expression: 'fy_quarter',
//   },
//   {
//     sql_expression: 'acv/bookings_goal',
//   },
//   {
//     sql_expression: 'article_ids',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state = ''closed'' THEN actual_close_date ELSE COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP END",
//   },
//   {
//     sql_expression: 'app_version',
//   },
//   {
//     sql_expression: 'funnel_app_id',
//   },
//   {
//     sql_expression: 'ticket_count',
//   },
//   {
//     sql_expression: 'rage_clicks',
//   },
//   {
//     sql_expression: 'owned_by',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''month'', creation_time)",
//   },
//   {
//     sql_expression: 'ROUND(total_bytes_processed/(1024 * 1024),2)',
//   },
//   {
//     sql_expression: 'ROUND(total_bytes_billed/(1024 * 1024),2)',
//   },
//   {
//     sql_expression:
//       " CASE WHEN mode = ''SyncRun_ModeEnumSyncFromDevrev'' THEN ''FromDevRev'' WHEN mode = ''SyncRun_ModeEnumSyncToDevrev'' THEN ''ToDevRev'' WHEN mode = ''SyncRun_ModeEnumInitial'' THEN ''Initial'' END",
//   },
//   {
//     sql_expression: 'NULLIF(completed_in, 0)',
//   },
//   {
//     sql_expression: 'has_eta',
//   },
//   {
//     sql_expression: 'sales_data_dr.material',
//   },
//   {
//     sql_expression: 'client',
//   },
//   {
//     sql_expression: 'pop',
//   },
//   {
//     sql_expression:
//       "list_distinct(CAST(json_extract_string(surveys_aggregation_json, ''$[*].minimum'') AS VARCHAR[]))",
//   },
//   {
//     sql_expression: "custom_fields->>''ctype__support_priority''",
//   },
//   {
//     sql_expression: 'currency',
//   },
//   {
//     sql_expression: 'snap_in',
//   },
//   {
//     sql_expression:
//       "CASE WHEN severity_name = ''Blocker'' THEN ''Sev 1'' WHEN severity_name = ''High'' THEN ''Sev 2'' WHEN severity_name = ''Medium'' THEN ''Sev 3'' ELSE ''Sev 4'' END",
//   },
//   {
//     sql_expression: 'tags',
//   },
//   {
//     sql_expression:
//       "CASE WHEN created_by_id LIKE ''%:devu/%'' THEN ''dev_user'' WHEN created_by_id LIKE ''%:revu/%'' THEN ''rev_user'' ELSE ''unknown''END",
//   },
//   {
//     sql_expression:
//       "STRING_SPLIT(TRANSLATE(json_extract(tags_json, ''$[*].tag_id'')::TEXT, CHR(34) || ''[]'', ''''), '','')",
//   },
//   {
//     sql_expression: 'close_quarter',
//   },
//   {
//     sql_expression:
//       "CASE WHEN actual_close_date > created_date THEN DATEDIFF(''minutes'',actual_closed_date, created_date) END",
//   },
//   {
//     sql_expression: "IF(account_type IS NULL, ''All'',account_type)",
//   },
//   {
//     sql_expression: 'created_by_fullname',
//   },
//   {
//     sql_expression: 'p50_upstream_service_time',
//   },
//   {
//     sql_expression: 'March_Customers',
//   },
//   {
//     sql_expression: 'count',
//   },
//   {
//     sql_expression: 'applies_to_part_id',
//   },
//   {
//     sql_expression: 'part_id',
//   },
//   {
//     sql_expression: 'answer',
//   },
//   {
//     sql_expression: 'COALESCE(total_next_resp_time, 0)',
//   },
//   {
//     sql_expression: 'due_date',
//   },
//   {
//     sql_expression:
//       "CAST(json_extract(dim_issue.priority_uenum_json, ''$.id'') as integer)",
//   },
//   {
//     sql_expression: 'forecast_category_str',
//   },
//   {
//     sql_expression:
//       "(COUNT(DISTINCT CASE WHEN turing_interacted = ''yes'' AND is_undeflected = ''no'' THEN id END)/COUNT(DISTINCT CASE WHEN is_resolved_today = ''yes'' THEN id END))",
//   },
//   {
//     sql_expression: 'full_name',
//   },
//   {
//     sql_expression: "datediff(''day'', created_date)",
//   },
//   {
//     sql_expression: 'identified_at',
//   },
//   {
//     sql_expression: 'event_date',
//   },
//   {
//     sql_expression: 'NULLIF(time_to_open, 0)',
//   },
//   {
//     sql_expression:
//       "TRIM(''\"'' from UNNEST(json_extract(custom_fields->>''ctype__event_type'', ''$[*]'')))",
//   },
//   {
//     sql_expression: 'dev',
//   },
//   {
//     sql_expression: 'current_opp_stage',
//   },
//   {
//     sql_expression:
//       "DATETRUNC(''month'', COALESCE((COALESCE(custom_fields ->> ''ctype__created_at_cfid'', NULL)::TIMESTAMP), created_date::TIMESTAMP)) + INTERVAL ''1 day''",
//   },
//   {
//     sql_expression: 'total_revusers',
//   },
//   {
//     sql_expression: 'daily_tickets_created',
//   },
//   {
//     sql_expression: 'opportunity_count',
//   },
//   {
//     sql_expression: 'feature',
//   },
//   {
//     sql_expression:
//       "CASE WHEN range_state = ''closed'' then actual_close_date::TIMESTAMP ELSE   created_date::TIMESTAMP END",
//   },
//   {
//     sql_expression: 'custom_fields',
//   },
//   {
//     sql_expression: 'new_stage_name',
//   },
//   {
//     sql_expression:
//       "case when state = ''in_progress'' then ''Active'' when state = ''closed'' and stage = ''resolved'' then ''Closed'' end",
//   },
//   {
//     sql_expression:
//       "STRING_SPLIT(TRANSLATE(cast(json_extract(tags_json, ''$[*].tag_id'') as string), CHR(34) || ''[]'', ''''), '','')",
//   },
//   {
//     sql_expression: "custom_fields->>''tnt__region''",
//   },
//   {
//     sql_expression: 'dim_ticket.subtype',
//   },
//   {
//     sql_expression: 'platform',
//   },
//   {
//     sql_expression: 'days_difference',
//   },
//   {
//     sql_expression:
//       "STRING_SPLIT(TRANSLATE(json_extract(tags_json, ''$[*].tag_id''), CHR(34) || ''[]'', ''''), '','')",
//   },
//   {
//     sql_expression: 'avg_time_taken_to_download_data_for_dashboard',
//   },
//   {
//     sql_expression: 'max_time_taken_to_update_widget',
//   },
//   {
//     sql_expression: 'service_improvement_status',
//   },
//   {
//     sql_expression: 'applies_to_part_ids',
//   },
//   {
//     sql_expression: 'target_close_date',
//   },
//   {
//     sql_expression: 'page_url',
//   },
//   {
//     sql_expression: 'part_ids',
//   },
//   {
//     sql_expression: 'mitigated_date',
//   },
//   {
//     sql_expression: 'curr_category',
//   },
//   {
//     sql_expression: 'query_id',
//   },
//   {
//     sql_expression: 'engagement_type_str',
//   },
//   {
//     sql_expression: 'Delivered_On_Time',
//   },
//   {
//     sql_expression: 'Q2_Customers',
//   },
//   {
//     sql_expression: 'Q4_ID',
//   },
//   {
//     sql_expression: 'user',
//   },
//   {
//     sql_expression: 'ticket_stage',
//   },
//   {
//     sql_expression:
//       "CASE WHEN source = ''issues''  THEN ''Issue Opened Ticket Linked'' ELSE ''Ticket Opened'' END",
//   },
//   {
//     sql_expression:
//       "CASE WHEN range_state = ''closed'' THEN (actual_close_date)::TIMESTAMP ELSE COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP END",
//   },
//   {
//     sql_expression: 'percentage_completion',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state = ''closed'' THEN actual_close_date::TIMESTAMP ELSE COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP END",
//   },
//   {
//     sql_expression: 'subtype2',
//   },
//   {
//     sql_expression:
//       "DATE_TRUNC(''week'', CASE WHEN range_state = ''closed'' THEN (actual_close_date)::TIMESTAMP ELSE COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP END)",
//   },
//   {
//     sql_expression: 'dayofweek(created_date)',
//   },
//   {
//     sql_expression:
//       " CASE WHEN mode = ''SyncRun_ModeEnumInitial'' THEN ''Initial'' WHEN mode = ''SyncRun_ModeEnumSyncToDevrev'' THEN ''SyncToDevRev'' WHEN mode = ''SyncRun_ModeEnumSyncFromDevrev'' THEN ''SyncFromDevRev'' END",
//   },
//   {
//     sql_expression: 'sub_region',
//   },
//   {
//     sql_expression: 'previous_target_close_quarter',
//   },
//   {
//     sql_expression: 'sales_data_dr.Ship_via',
//   },
//   {
//     sql_expression: 'sales_data_dr.finish_time',
//   },
//   {
//     sql_expression:
//       " SUBSTRING(state_sh, POSITION(''SyncProgress_StateEnum'' IN state_sh) + LENGTH(''SyncProgress_StateEnum'')) ",
//   },
//   {
//     sql_expression: 'coder_status',
//   },
//   {
//     sql_expression: "CONCAT(monthname(end_of_month),''-'',year(end_of_month))",
//   },
//   {
//     sql_expression: 'dayname(created_date)',
//   },
//   {
//     sql_expression: 'SUM(SUM(amount)) OVER (PARTITION BY {MEERKAT}.owner)',
//   },
//   {
//     sql_expression: 'transition_start_date',
//   },
//   {
//     sql_expression: "date_trunc(''months'', created_date)",
//   },
//   {
//     sql_expression: "date_trunc(''week'', actual_close_date)",
//   },
//   {
//     sql_expression: 'utm_source',
//   },
//   {
//     sql_expression: 'mau',
//   },
//   {
//     sql_expression: 'account_type',
//   },
//   {
//     sql_expression: 'average',
//   },
//   {
//     sql_expression: 'hpc_island',
//   },
//   {
//     sql_expression: 'order_of_states',
//   },
//   {
//     sql_expression: 'time',
//   },
//   {
//     sql_expression: 'browser',
//   },
//   {
//     sql_expression: 'aggr_part_id',
//   },
//   {
//     sql_expression: 'funnel_id',
//   },
//   {
//     sql_expression: 'p95_latency',
//   },
//   {
//     sql_expression:
//       "CASE WHEN actual_close_date > created_date THEN DATEDIFF(''day'',actual_closed_date, current_date) END",
//   },
//   {
//     sql_expression: "custom_fields->>''tnt__product''",
//   },
//   {
//     sql_expression: 'rev_uid',
//   },
//   {
//     sql_expression: 'drilldown_count_val',
//   },
//   {
//     sql_expression: "DATETRUNC(''day'',actual_close_date)",
//   },
//   {
//     sql_expression: 'COALESCE(total_first_resp_time, 0)',
//   },
//   {
//     sql_expression:
//       "translate(unnest(json_extract(tags_json, ''$[*].tag_id'')), CHR(34), '''')",
//   },
//   {
//     sql_expression: 'developer',
//   },
//   {
//     sql_expression: 'contact_ids',
//   },
//   {
//     sql_expression:
//       "''platform=WEB&vk='' || version_key || ''&si='' || session_id",
//   },
//   {
//     sql_expression: 'response_time',
//   },
//   {
//     sql_expression: "issue_custom_fields->>''ctype__customfield_11497_cfid''",
//   },
//   {
//     sql_expression: 'Features_Planned',
//   },
//   {
//     sql_expression: 'cod_orders_count',
//   },
//   {
//     sql_expression:
//       "CASE WHEN turing_interacted = ''yes'' AND is_undeflected = ''no'' THEN ''yes'' ELSE ''no'' END",
//   },
//   {
//     sql_expression: 'created_at_ts',
//   },
//   {
//     sql_expression: 'cluster',
//   },
//   {
//     sql_expression: 'pr_approved_time',
//   },
//   {
//     sql_expression: 'pr_merged_time',
//   },
//   {
//     sql_expression: 'issue_date::TIMESTAMP',
//   },
//   {
//     sql_expression: 'issue_linked',
//   },
//   {
//     sql_expression: 'actual_close_date::TIMESTAMP',
//   },
//   {
//     sql_expression: 'request_type',
//   },
//   {
//     sql_expression: 'timestamp_nsecs',
//   },
//   {
//     sql_expression: 'first_response_time',
//   },
//   {
//     sql_expression: "stage_json->>''name''",
//   },
//   {
//     sql_expression:
//       "COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP ",
//   },
//   {
//     sql_expression: 'id_su',
//   },
//   {
//     sql_expression:
//       " CASE WHEN mode_su = ''SyncRun_ModeEnumInitial'' THEN ''Initial'' WHEN mode_su = ''SyncRun_ModeEnumSyncToDevrev'' THEN ''SyncToDevRev'' WHEN mode_su = ''SyncRun_ModeEnumSyncFromDevrev'' THEN ''SyncFromDevRev'' END",
//   },
//   {
//     sql_expression: 'dim_ticket.id',
//   },
//   {
//     sql_expression: 'UNNEST(t.owned_by_ids)',
//   },
//   {
//     sql_expression: 'stock_schema_fragment_id',
//   },
//   {
//     sql_expression: "date_trunc(''day'', target_close_date)",
//   },
//   {
//     sql_expression: 'host_serial',
//   },
//   {
//     sql_expression: 'summary',
//   },
//   {
//     sql_expression: 'opportunity_id',
//   },
//   {
//     sql_expression: 'Total_Product_Features_Planned',
//   },
//   {
//     sql_expression: "JSON_EXTRACT(custom_fields, ''$.ctype__linkedaccount'')",
//   },
//   {
//     sql_expression: 'job_timestamp',
//   },
//   {
//     sql_expression: 'total_new_devusers',
//   },
//   {
//     sql_expression: 'desciption',
//   },
//   {
//     sql_expression:
//       "CASE WHEN array_length(responses, 1) > 2 AND stage = ''resolved'' THEN ''Single touch resolved'' ELSE ''Not resolved in single touch'' END",
//   },
//   {
//     sql_expression: "DATETRUNC(''month'', record_date)",
//   },
//   {
//     sql_expression: 'health_state',
//   },
//   {
//     sql_expression: 'won_arr',
//   },
//   {
//     sql_expression: "CASE WHEN escalated THEN ''Yes'' ELSE ''No'' END",
//   },
//   {
//     sql_expression: 'October_Customers',
//   },
//   {
//     sql_expression: 'December_Customers',
//   },
//   {
//     sql_expression: 'CAST(partition_ts_30m AS TIMESTAMP)',
//   },
//   {
//     sql_expression: 'num_contacts',
//   },
//   {
//     sql_expression: 'devu_count',
//   },
//   {
//     sql_expression: 'unhealthy_days',
//   },
//   {
//     sql_expression: 'apiName',
//   },
//   {
//     sql_expression:
//       "UNNEST(STRING_TO_ARRAY(REGEXP_REPLACE(custom_fields ->> ''ctype__x1260822517949_cfid'', ''[\\[\\]\"]'', '''', ''g''), '',''))",
//   },
//   {
//     sql_expression: 'filtered_query',
//   },
//   {
//     sql_expression: 'sla_stage',
//   },
//   {
//     sql_expression: 'issueId',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''week'',  issue_date)",
//   },
//   {
//     sql_expression: 'customer_type',
//   },
//   {
//     sql_expression: 'consignment_count',
//   },
//   {
//     sql_expression: "STRING_TO_ARRAY(members_rsvped,'','')",
//   },
//   {
//     sql_expression: 'module',
//   },
//   {
//     sql_expression: 'work_status',
//   },
//   {
//     sql_expression: 'timestamp',
//   },
//   {
//     sql_expression:
//       "CAST(CAST(FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, ''$[0].average'') AS INT)) AS INT)AS STRING)",
//   },
//   {
//     sql_expression: 'part_percentage*100',
//   },
//   {
//     sql_expression: 'artifact_ids',
//   },
//   {
//     sql_expression: 'cee_groups',
//   },
//   {
//     sql_expression: 'forecast_category_enum',
//   },
//   {
//     sql_expression: 'partner',
//   },
//   {
//     sql_expression: "IF(tnt__type IS NULL,''All'',tnt__type)",
//   },
//   {
//     sql_expression: 'job_time',
//   },
//   {
//     sql_expression: 'unnest(part_ids)',
//   },
//   {
//     sql_expression: 'daily_revuser_logins',
//   },
//   {
//     sql_expression: 'ticket_age_day',
//   },
//   {
//     sql_expression:
//       'CASE WHEN COUNT(CASE WHEN total_responses > 0 THEN 1 END) > 0 THEN COUNT(CASE WHEN total_responses > 0 AND floor(sum_rating / NULLIF(total_responses, 0)) IN (4, 5) THEN 1 END) * 100.0 / COUNT(CASE WHEN total_responses > 0 THEN 1 END) ELSE NULL END',
//   },
//   {
//     sql_expression: 'pipeline_arr/booking_goal',
//   },
//   {
//     sql_expression: 'repair_count',
//   },
//   {
//     sql_expression: 'time_group_order',
//   },
//   {
//     sql_expression: 'sprint_name',
//   },
//   {
//     sql_expression: 'custom_issue_product',
//   },
//   {
//     sql_expression: 'ticketId',
//   },
//   {
//     sql_expression: 'ticket_id',
//   },
//   {
//     sql_expression: "JSON_EXTRACT(surveys_aggregation_json, ''$[0].average'')",
//   },
//   {
//     sql_expression: 'qna_count',
//   },
//   {
//     sql_expression: 'forecast_category',
//   },
//   {
//     sql_expression: 'metric_status',
//   },
//   {
//     sql_expression: 'customer_budget',
//   },
//   {
//     sql_expression: 'Percentage_On_Time',
//   },
//   {
//     sql_expression: 'Q2_ID',
//   },
//   {
//     sql_expression: 'fleet_count',
//   },
//   {
//     sql_expression:
//       'sales_data_dr.tnt__salesforceservice_accounts_industry_cfid',
//   },
//   {
//     sql_expression:
//       "CASE WHEN DATEDIFF(''day'', created_date, CURRENT_DATE) >= 1 THEN ''1 day'' WHEN DATEDIFF(''day'', created_date, CURRENT_DATE) >= 2 THEN ''1-2 days'' WHEN DATEDIFF(''day'', created_date, CURRENT_DATE) >= 7 THEN ''2-7 days'' WHEN DATEDIFF(''day'', created_date, CURRENT_DATE) >= 30 THEN ''7-30 days'' ELSE ''>30 days'' END",
//   },
//   {
//     sql_expression: 'health_status',
//   },
//   {
//     sql_expression: 'NULLIF(time_to_merge, 0)',
//   },
//   {
//     sql_expression: 'COALESCE(closed_at, actual_close_date)',
//   },
//   {
//     sql_expression: 'released_date',
//   },
//   {
//     sql_expression: 'inbound_paid_total_acv',
//   },
//   {
//     sql_expression: 'total_issues',
//   },
//   {
//     sql_expression: 'closed_tickets',
//   },
//   {
//     sql_expression: 'cast(actual_date_of_delivery_by_service_provider as date)',
//   },
//   {
//     sql_expression:
//       "case when state != ''closed'' then ''Open'' else ''Closed'' ",
//   },
//   {
//     sql_expression:
//       "CASE WHEN issue_category = ''incident'' THEN ''bug'' ELSE issue_category END",
//   },
//   {
//     sql_expression: "DATE_TRUNC(''week'', record_date)",
//   },
//   {
//     sql_expression: 'planning_status',
//   },
//   {
//     sql_expression: 'last_updated_date',
//   },
//   {
//     sql_expression:
//       "CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END",
//   },
//   {
//     sql_expression: "DATE_TRUNC(''week'', created_date)",
//   },
//   {
//     sql_expression: 'status',
//   },
//   {
//     sql_expression: 'record_hour',
//   },
//   {
//     sql_expression: 'group',
//   },
//   {
//     sql_expression: 'articles_retrieved',
//   },
//   {
//     sql_expression: 'dim_issue.applies_to_part_id',
//   },
//   {
//     sql_expression: 'EXTRACT(HOUR FROM created_date)',
//   },
//   {
//     sql_expression: 'experience_start_date',
//   },
//   {
//     sql_expression: 'cod_reconciliation',
//   },
//   {
//     sql_expression: 'date_broken',
//   },
//   {
//     sql_expression: 'ticket_linked',
//   },
//   {
//     sql_expression: 'ticket_subtype',
//   },
//   {
//     sql_expression: 'reported_by_ids',
//   },
//   {
//     sql_expression: 'pr_opened_time',
//   },
//   {
//     sql_expression: "DATETRUNC(''day'', created_date)",
//   },
//   {
//     sql_expression:
//       "CASE WHEN (range_state=''closed'') THEN ''Closed'' ELSE ''Opened'' END",
//   },
//   {
//     sql_expression: 'on_hold_reason',
//   },
//   {
//     sql_expression: 'tnt__cluster',
//   },
//   {
//     sql_expression: 'resolution_time_hours',
//   },
//   {
//     sql_expression:
//       "CASE WHEN tnt__on_hold_reasons IS NOT NULL THEN tnt__on_hold_reasons ELSE ''-'' END",
//   },
//   {
//     sql_expression: 'released_by_id',
//   },
//   {
//     sql_expression: 'sprint_board',
//   },
//   {
//     sql_expression: 'month',
//   },
//   {
//     sql_expression: 'fordate',
//   },
//   {
//     sql_expression: 'APA Time',
//   },
//   {
//     sql_expression: 'incident',
//   },
//   {
//     sql_expression: 'job_link',
//   },
//   {
//     sql_expression: 'source_part_id',
//   },
//   {
//     sql_expression: 'weekly_distinct_revuser_logins',
//   },
//   {
//     sql_expression:
//       "CASE WHEN subtype = ''zendesk_uniphore_tickets.incident'' THEN CAST(custom_fields ->> ''ctype__created_at_cfid'' AS DATE) WHEN subtype = ''servicenow_redbox_cases'' THEN CAST(custom_fields ->> ''ctype__opened_at_cfid'' AS DATE) ELSE created_date END",
//   },
//   {
//     sql_expression: 'sprint_date',
//   },
//   {
//     sql_expression: 'ROUND(min_ttr,2)',
//   },
//   {
//     sql_expression: 'ROUND(max_ttr,2)',
//   },
//   {
//     sql_expression: "UNNEST(custom_fields ->> ''$.tnt__countries[*]'')",
//   },
//   {
//     sql_expression:
//       "CASE             WHEN days_open >= 1 THEN ''1 day''             WHEN days_open >= 2 THEN ''1-2 days''             WHEN days_open >= 7 THEN ''2-7 days''             WHEN days_open >= 30 THEN ''7-30 days''             ELSE ''>30 days''         END",
//   },
//   {
//     sql_expression: 'CAST(created_date as DATE)',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''month'', record_date) + INTERVAL 1 DAY",
//   },
//   {
//     sql_expression: 'resolved_date',
//   },
//   {
//     sql_expression: 'theatre',
//   },
//   {
//     sql_expression: 'emea_sum_acv',
//   },
//   {
//     sql_expression: 'devrev_id',
//   },
//   {
//     sql_expression:
//       "CASE WHEN status = ''hit'' THEN ''Achieved'' WHEN status = ''miss'' THEN ''Breached'' END",
//   },
//   {
//     sql_expression: 'user_email',
//   },
//   {
//     sql_expression: 'rev_attendees',
//   },
//   {
//     sql_expression: 'gpu_model',
//   },
//   {
//     sql_expression: 'CAST(SUBSTRING(partition_ts, 1, 10) AS TIMESTAMP)',
//   },
//   {
//     sql_expression: 'segment',
//   },
//   {
//     sql_expression: 'sla',
//   },
//   {
//     sql_expression:
//       "CASE WHEN range_state = ''closed'' THEN (issue_actual_close_date)::TIMESTAMP ELSE issue_created_date::TIMESTAMP END",
//   },
//   {
//     sql_expression: "json_extract_string(stage_json, ''$.name'')",
//   },
//   {
//     sql_expression: 'Cluster',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state_sh = ''SyncProgress_StateEnumCompleted''  THEN ''Completed''   WHEN state_sh = ''SyncProgress_StateEnumExtractionError'' THEN ''Failed''   WHEN state_sh = ''SyncProgress_StateEnumRecipeDiscoveryError'' THEN ''Failed''   WHEN state_sh = ''SyncProgress_StateEnumLoadingError'' THEN ''Failed''   WHEN state_sh = ''SyncProgress_StateEnumExtractAttachmentsError'' THEN ''Failed''   WHEN state_sh = ''SyncProgress_StateEnumDeletionError'' THEN ''Failed''   WHEN state_sh = ''SyncProgress_StateEnumLoadingAttachmentsError'' THEN ''Failed''   WHEN state_sh = ''SyncProgress_StateEnumExtractAttachments'' THEN ''ExtractAttachments'' END",
//   },
//   {
//     sql_expression: "date_trunc(''day'', actual_close_date)",
//   },
//   {
//     sql_expression: 'articles_post_mfz_check',
//   },
//   {
//     sql_expression:
//       "CASE WHEN metric_status = ''hit'' THEN ''Achieved'' WHEN metric_status = ''miss'' THEN ''Missed'' ELSE ''In Progress'' END",
//   },
//   {
//     sql_expression: 'next_mod_timestamp',
//   },
//   {
//     sql_expression: 'mao_percent_change',
//   },
//   {
//     sql_expression:
//       "DATETRUNC(''month'', COALESCE(NULLIF(custom_fields ->> ''ctype__created_at_cfid'', '''')::TIMESTAMP, created_date::TIMESTAMP)) + INTERVAL ''1 day''",
//   },
//   {
//     sql_expression: 'compare_date',
//   },
//   {
//     sql_expression: "regexp_split_to_array(owned_by_id, ''\"|\\[\"|\"\\]'')",
//   },
//   {
//     sql_expression: 'June_ID',
//   },
//   {
//     sql_expression: 'any_value(customer_type)',
//   },
//   {
//     sql_expression: 'tier',
//   },
//   {
//     sql_expression: 'create_vista_report_click_count_val',
//   },
//   {
//     sql_expression: 'avg_max_time_taken_to_create_dashboard_val',
//   },
//   {
//     sql_expression: 'title',
//   },
//   {
//     sql_expression: 'group_id',
//   },
//   {
//     sql_expression: 'sum_rating',
//   },
//   {
//     sql_expression: 'sources',
//   },
//   {
//     sql_expression: 'severity_stage',
//   },
//   {
//     sql_expression: 'valid_sources',
//   },
//   {
//     sql_expression: 'object',
//   },
//   {
//     sql_expression: 'location',
//   },
//   {
//     sql_expression: 'owner_id',
//   },
//   {
//     sql_expression: "JSON_EXTRACT_STRING(value, ''$.amount'')::FLOAT",
//   },
//   {
//     sql_expression: 'env',
//   },
//   {
//     sql_expression: 'version_key',
//   },
//   {
//     sql_expression: 'sprint_board_id',
//   },
//   {
//     sql_expression: 'severity_id',
//   },
//   {
//     sql_expression: 'prev_category',
//   },
//   {
//     sql_expression: 'total_objects',
//   },
//   {
//     sql_expression: 'tnt__customers',
//   },
//   {
//     sql_expression: 'rider_payout_count',
//   },
//   {
//     sql_expression: 'lifecycle_details',
//   },
//   {
//     sql_expression: 'issues',
//   },
//   {
//     sql_expression: 'forecast_category_strg',
//   },
//   {
//     sql_expression: 'ticket_owned_by_ids',
//   },
//   {
//     sql_expression:
//       "CAST(TRUNC(CAST(FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, ''$[0].average'') AS INT)) AS INT)) AS VARCHAR)",
//   },
//   {
//     sql_expression: "DATE_TRUNC(''week'',created_date)",
//   },
//   {
//     sql_expression:
//       "DATE_TRUNC(''week'', CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END)",
//   },
//   {
//     sql_expression: 'ctype__x20331512708123_cfid',
//   },
//   {
//     sql_expression:
//       " CASE WHEN external_system_type = ''ExternalSystemTypeEnum_ZENDESK'' THEN ''ZENDESK'' WHEN external_system_type = ''ExternalSystemTypeEnum_JIRA'' THEN ''JIRA'' WHEN external_system_type = ''ExternalSystemTypeEnum_HUBSPOT'' THEN ''HUBSPOT'' WHEN external_system_type = ''ExternalSystemTypeEnum_GITHUB'' THEN ''GITHUB'' WHEN external_system_type = ''ExternalSystemTypeEnum_SERVICENOW'' THEN ''SERVICENOW'' WHEN external_system_type = ''ExternalSystemTypeEnum_SALESFORCE_SERVICE'' THEN ''SALESFORCE'' WHEN external_system_type = ''ExternalSystemTypeEnum_ADAAS'' THEN ''ADAAS'' WHEN external_system_type = ''ExternalSystemTypeEnum_LINEAR'' THEN ''LINEAR'' WHEN external_system_type = ''ExternalSystemTypeEnum_ROCKETLANE'' THEN ''ROCKETLANE'' WHEN external_system_type = ''ExternalSystemTypeEnum_CONFLUENCE'' THEN ''CONFLUENCE''END",
//   },
//   {
//     sql_expression: 'ordinal',
//   },
//   {
//     sql_expression: 'is_spam',
//   },
//   {
//     sql_expression: 'industry_events_total_acv',
//   },
//   {
//     sql_expression: 'is_sprint_linked',
//   },
//   {
//     sql_expression: 'UNNEST(owned_by_ids)',
//   },
//   {
//     sql_expression: 'org_activity_date',
//   },
//   {
//     sql_expression: 'account_owner',
//   },
//   {
//     sql_expression: 'tnt__account_executive_v2',
//   },
//   {
//     sql_expression: 'capacity_block_id',
//   },
//   {
//     sql_expression: 'cluster_name',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state=''open'' THEN ''Open'' WHEN state=''in_progress'' THEN ''In Progress'' WHEN state=''closed'' THEN ''Closed'' END",
//   },
//   {
//     sql_expression: 'geography',
//   },
//   {
//     sql_expression: 'health_state_duration_min',
//   },
//   {
//     sql_expression:
//       "CASE WHEN component = ''Loader'' then ''Loader'' WHEN component = ''Extractor'' then ''Extractor'' WHEN component = ''Orchestrator'' THEN ''Orchestrator'' WHEN component = ''RecipeManager'' THEN ''RecipeManager'' WHEN component = ''Transformer'' THEN ''Transformer'' END",
//   },
//   {
//     sql_expression: 'session_url',
//   },
//   {
//     sql_expression:
//       'CASE WHEN actual_close_date > created_date THEN actual_close_date END',
//   },
//   {
//     sql_expression: 'vista_name',
//   },
//   {
//     sql_expression:
//       'CONCAT( UPPER(SUBSTRING(first_time_fix, 1, 1)), LOWER(SUBSTRING(first_time_fix, 2)) )',
//   },
//   {
//     sql_expression: 'created_at',
//   },
//   {
//     sql_expression: 'edit_mode_count_val',
//   },
//   {
//     sql_expression: '"group"',
//   },
//   {
//     sql_expression: 'account_id',
//   },
//   {
//     sql_expression: 'severity_name',
//   },
//   {
//     sql_expression: 'csat_score',
//   },
//   {
//     sql_expression: 'forecasted_close_date',
//   },
//   {
//     sql_expression: "COUNT(CASE WHEN state != ''closed'' THEN id END)",
//   },
//   {
//     sql_expression: 'external_client',
//   },
//   {
//     sql_expression: 'total_first_resp_time',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''month'', record_date)",
//   },
//   {
//     sql_expression: 'tnt__opportunity_type',
//   },
//   {
//     sql_expression: "IF(region_name IS NULL,''Other'',region_name)",
//   },
//   {
//     sql_expression: 'ticket_created_date',
//   },
//   {
//     sql_expression: 'forecast',
//   },
//   {
//     sql_expression: "DATE_DIFF(''minute'', created_date, actual_close_date)",
//   },
//   {
//     sql_expression:
//       " CASE WHEN json_extract_string(metric, ''$.metric_definition_id'') LIKE ''%:metric_definition/1'' THEN ''first_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') LIKE ''%:metric_definition/2'' THEN ''next_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') LIKE ''%:metric_definition/3'' THEN ''resolution''END ",
//   },
//   {
//     sql_expression:
//       "CASE WHEN subtype IS NOT  NULL THEN subtype WHEN subtype IS NULL THEN ''Unassigned'' END",
//   },
//   {
//     sql_expression: 'tnt__developer',
//   },
//   {
//     sql_expression: 'verified_enum',
//   },
//   {
//     sql_expression:
//       "CASE WHEN stage_json->>''name'' in (''On Hold'', ''On-hold'', ''On hold'') THEN ''On Hold'' WHEN state = ''in_progress'' THEN ''In Progress''  END",
//   },
//   {
//     sql_expression: 'first_commit_time',
//   },
//   {
//     sql_expression: 'started_at_sh',
//   },
//   {
//     sql_expression: 'tnt__geo',
//   },
//   {
//     sql_expression: 'dev_slug',
//   },
//   {
//     sql_expression: 'subscribers.group',
//   },
//   {
//     sql_expression:
//       "TRIM(''\"'' from UNNEST(json_extract(custom_fields->>''ctype__alert_name'', ''$[*]'')))",
//   },
//   {
//     sql_expression: 'environment',
//   },
//   {
//     sql_expression: 'event_meetups_total_acv',
//   },
//   {
//     sql_expression: 'org_ref',
//   },
//   {
//     sql_expression:
//       "CASE WHEN DATE_TRUNC(''day'', record_hour) = DATE_TRUNC(''day'', created_date) THEN ''Created'' WHEN DATE_TRUNC(''day'', created_date) != DATE_TRUNC(''day'', record_hour) AND DATE_TRUNC(''day'', actual_close_date) = DATE_TRUNC(''day'', record_hour) THEN ''Closed'' END",
//   },
//   {
//     sql_expression: 'days_in_current_stage',
//   },
//   {
//     sql_expression: 'opp_owner',
//   },
//   {
//     sql_expression: 'first_touch_timestamp',
//   },
//   {
//     sql_expression: 'primary_owner_devu',
//   },
//   {
//     sql_expression: 'health_state_duration_median',
//   },
//   {
//     sql_expression: 'count_of_active_snapins',
//   },
//   {
//     sql_expression: 'CAST(last_healthy_or_first_seen_time AS TIMESTAMP)',
//   },
//   {
//     sql_expression: 'CAST(last_healthy_or_first_seen_time as TIMESTAMP)',
//   },
//   {
//     sql_expression: 'October_ID',
//   },
//   {
//     sql_expression: 'ROUND(mean_ttr,2)',
//   },
//   {
//     sql_expression:
//       "json_extract_string(fact_issue.stage_json, ''$.stage_id'')",
//   },
//   {
//     sql_expression: 'opp_stage_id',
//   },
//   {
//     sql_expression:
//       "CASE WHEN json_extract_string(metric, ''$.metric_definition_id'') LIKE ''%:metric_definition/1'' THEN ''first_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') LIKE ''%:metric_definition/2'' THEN ''next_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') LIKE ''%:metric_definition/3'' THEN ''resolution''END",
//   },
//   {
//     sql_expression: 'turing_effective',
//   },
//   {
//     sql_expression: 'group_ticket',
//   },
//   {
//     sql_expression: 'add_widget_clicked_count_val',
//   },
//   {
//     sql_expression: 'pipeline_created_date',
//   },
//   {
//     sql_expression: 'owner',
//   },
//   {
//     sql_expression: 'device_type',
//   },
//   {
//     sql_expression: 'group_ids',
//   },
//   {
//     sql_expression: 'total_elapsed_seconds',
//   },
//   {
//     sql_expression: 'Feature_Group',
//   },
//   {
//     sql_expression: 'turing_deflected',
//   },
//   {
//     sql_expression: 'CAST(ticket_time_created AS TIMESTAMP)',
//   },
//   {
//     sql_expression: 'part',
//   },
//   {
//     sql_expression: 'sales_data_dr.date',
//   },
//   {
//     sql_expression: 'sprint_start_date',
//   },
//   {
//     sql_expression: 'email',
//   },
//   {
//     sql_expression: 'revu_id',
//   },
//   {
//     sql_expression: 'sales_data_dr.num',
//   },
//   {
//     sql_expression:
//       " CASE WHEN sync_type_su = ''SyncUnit_SyncTypeEnumManual'' THEN ''Manual'' WHEN sync_type_su = ''SyncUnit_SyncTypeEnumPeriodic'' THEN ''Periodic'' END",
//   },
//   {
//     sql_expression: 'ended_at_sh',
//   },
//   {
//     sql_expression: 'total_file_count',
//   },
//   {
//     sql_expression: 'modified_date_su',
//   },
//   {
//     sql_expression: 'avg_sales_cycle',
//   },
//   {
//     sql_expression: 'metric_stage',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''week'', actual_close_date)",
//   },
//   {
//     sql_expression: 'previous_mod_timestamp',
//   },
//   {
//     sql_expression: "JSON_EXTRACT(custom_fields, ''$.ctype__date'')",
//   },
//   {
//     sql_expression: 'traversed_stage',
//   },
//   {
//     sql_expression:
//       "case when state = ''in_progress'' then ''active'' when stage = ''resolved'' then ''closed'' else null end",
//   },
//   {
//     sql_expression: 'last_health_state',
//   },
//   {
//     sql_expression: 'total_devusers',
//   },
//   {
//     sql_expression: 'weekly_distinct_devuser_logins',
//   },
//   {
//     sql_expression:
//       "CASE WHEN source = ''issues'' and state = ''closed''  THEN ''Issue Closed Ticket Linked'' ELSE ''Issue Opened Linked To Ticket'' END",
//   },
//   {
//     sql_expression: 'booking_goal',
//   },
//   {
//     sql_expression: 'p90_upstream_service_time',
//   },
//   {
//     sql_expression: 'issue_count',
//   },
//   {
//     sql_expression: 'September_Customers',
//   },
//   {
//     sql_expression: 'source_channel',
//   },
//   {
//     sql_expression: 'max_time_taken_to_execute_query_for_dashboard_val',
//   },
//   {
//     sql_expression: 'gen_answer',
//   },
//   {
//     sql_expression: 'question',
//   },
//   {
//     sql_expression: 'reason',
//   },
//   {
//     sql_expression: 'opp_stage',
//   },
//   {
//     sql_expression: "json_extract_string(dim_issue.stage_json, ''$.stage_id'')",
//   },
//   {
//     sql_expression: 'user_id',
//   },
//   {
//     sql_expression: "DATETRUNC(''month'',created_date)",
//   },
//   {
//     sql_expression: 'Q3_ID',
//   },
//   {
//     sql_expression: 'display_name',
//   },
//   {
//     sql_expression: 'orders_with_cn_value',
//   },
//   {
//     sql_expression: 'meeting_type',
//   },
//   {
//     sql_expression: 'CAST(CAST(record_date AS TIMESTAMP) AS DATE)',
//   },
//   {
//     sql_expression: "DATETRUNC(''month'', created_date)",
//   },
//   {
//     sql_expression: "DATETRUNC(''day'',created_date)",
//   },
//   {
//     sql_expression:
//       "CASE WHEN json_extract_string(metric, ''$.metric_definition_id'') == ''don:core:dvrv-us-1:devo/0:metric_definition/1'' THEN ''first_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') == ''don:core:dvrv-us-1:devo/0:metric_definition/2'' THEN ''next_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') = ''don:core:dvrv-us-1:devo/0:metric_definition/3'' THEN ''resolution''END",
//   },
//   {
//     sql_expression: "CAST(strftime(''%H'', created_date) AS VARCHAR)",
//   },
//   {
//     sql_expression: 'previous_target_close_date',
//   },
//   {
//     sql_expression: 'sales_data_dr.volume',
//   },
//   {
//     sql_expression: 'resolved_at',
//   },
//   {
//     sql_expression: 'monthname(end_of_month)',
//   },
//   {
//     sql_expression: "date_trunc(''day'', modified_date)",
//   },
//   {
//     sql_expression: 'COALESCE(created_at, opened_at, created_date)::TIMESTAMP ',
//   },
//   {
//     sql_expression: 'partners_total_acv',
//   },
//   {
//     sql_expression: 'sla_policy_id',
//   },
//   {
//     sql_expression:
//       "CASE WHEN is_issue_linked = ''yes'' THEN ''Escalated to issue'' ELSE ''Not escalated to issue'' END",
//   },
//   {
//     sql_expression: 'repair_type',
//   },
//   {
//     sql_expression: 'DAYNAME(formatted_created_date)',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''week'', formatted_created_date)",
//   },
//   {
//     sql_expression:
//       "dayofweek(CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END)",
//   },
//   {
//     sql_expression: 'app_launch_time',
//   },
//   {
//     sql_expression: 'statusCode',
//   },
//   {
//     sql_expression: 'type',
//   },
//   {
//     sql_expression: 'save_layout_count_val',
//   },
//   {
//     sql_expression: 'min_time_taken_to_execute_query_for_dashboard_val',
//   },
//   {
//     sql_expression: 'age',
//   },
//   {
//     sql_expression: 'employment_status',
//   },
//   {
//     sql_expression: 'console_value',
//   },
//   {
//     sql_expression: 'os',
//   },
//   {
//     sql_expression: 'ttimestamp',
//   },
//   {
//     sql_expression: 'persona',
//   },
//   {
//     sql_expression: 'scheduled_date',
//   },
//   {
//     sql_expression: 'parent_acc_id',
//   },
//   {
//     sql_expression:
//       "CASE when rev_oid = ''don:identity:dvrv-us-1:devo/NJ5yrCDA:revo/ddmwB92c'' THEN ''Proactive'' ELSE ''Reactive'' END",
//   },
//   {
//     sql_expression: 'modified_date_quarter',
//   },
//   {
//     sql_expression: 'sales_data_dr.sku',
//   },
//   {
//     sql_expression: 'total_lines_added',
//   },
//   {
//     sql_expression:
//       "CASE WHEN state = ''closed'' THEN (actual_close_date)::TIMESTAMP ELSE COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP END",
//   },
//   {
//     sql_expression: 'qnas_post_mfz_check',
//   },
//   {
//     sql_expression: 'other_sum_acv',
//   },
//   {
//     sql_expression: 'item',
//   },
//   {
//     sql_expression: 'cloud',
//   },
//   {
//     sql_expression: 'created_month',
//   },
//   {
//     sql_expression: 'sla_achivements',
//   },
//   {
//     sql_expression: 'tnt__type',
//   },
//   {
//     sql_expression: 'health_state_duration_mean',
//   },
//   {
//     sql_expression: 'customer_tier',
//   },
//   {
//     sql_expression: 'owned_by_fullname',
//   },
//   {
//     sql_expression: 'January_Customers',
//   },
//   {
//     sql_expression: 'November_Customers',
//   },
//   {
//     sql_expression: 'tkt_ct',
//   },
//   {
//     sql_expression: 'issue_stage',
//   },
//   {
//     sql_expression:
//       "IF(tnt__opportunity_type IS NULL,''All'',tnt__opportunity_type)",
//   },
//   {
//     sql_expression: 'min_unhealthy_days',
//   },
//   {
//     sql_expression: 'rev_oid',
//   },
//   {
//     sql_expression: 'COALESCE(servicenow_closed_at, actual_close_date)',
//   },
//   {
//     sql_expression: 'dead_clicks',
//   },
//   {
//     sql_expression: 'api_errors',
//   },
//   {
//     sql_expression: 'repo_name',
//   },
//   {
//     sql_expression: "''ticket''",
//   },
//   {
//     sql_expression: 'round(duration_hours)',
//   },
//   {
//     sql_expression: 'dimension_part_id',
//   },
//   {
//     sql_expression: 'region_name',
//   },
//   {
//     sql_expression: 'replacement_count',
//   },
//   {
//     sql_expression: 'ticket_tags',
//   },
//   {
//     sql_expression: 'event_week_start',
//   },
//   {
//     sql_expression: 'full_opp_id',
//   },
//   {
//     sql_expression:
//       "CASE WHEN DATEDIFF(''day'', modified_date, CURRENT_DATE) >= 1 THEN ''1 day'' WHEN DATEDIFF(''day'', modified_date, CURRENT_DATE) >= 2 THEN ''1-2 days'' WHEN DATEDIFF(''day'', modified_date, CURRENT_DATE) >= 7 THEN ''2-7 days'' WHEN DATEDIFF(''day'', modified_date, CURRENT_DATE) >= 30 THEN ''7-30 days'' ELSE ''>30 days'' END",
//   },
//   {
//     sql_expression: 'display_name_devo',
//   },
//   {
//     sql_expression: 'method',
//   },
//   {
//     sql_expression: 'NULLIF(time_to_approve, 0)',
//   },
//   {
//     sql_expression: 'reported_by_id',
//   },
//   {
//     sql_expression: 'sku_type',
//   },
//   {
//     sql_expression: 'opportunity',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''week'', issue_date)",
//   },
//   {
//     sql_expression: "DATE_TRUNC(''days'', created_date)",
//   },
//   {
//     sql_expression: 'most_recent_timestamp',
//   },
//   {
//     sql_expression:
//       "COALESCE(custom_fields ->> ''ctype__created_at_cfid'', created_date)::TIMESTAMP",
//   },
//   {
//     sql_expression: 'commit_id',
//   },
//   {
//     sql_expression: 'subfeature',
//   },
//   {
//     sql_expression: 'se',
//   },
//   {
//     sql_expression: 'revu_ct',
//   },
//   {
//     sql_expression: 'issue_close_date',
//   },
//   {
//     sql_expression: 'partition_ts',
//   },
//   {
//     sql_expression: 'opp_ct',
//   },
//   {
//     sql_expression: 'latest_issue_contribution_time',
//   },
//   {
//     sql_expression: 'median_unhealthy_days',
//   },
//   {
//     sql_expression: 'minimum_time_repair',
//   },
//   {
//     sql_expression: "json_extract_string(stage_json, ''$.stage_id'')",
//   },
//   {
//     sql_expression: 'add_section_clicked_count_val',
//   },
//   {
//     sql_expression: 'query',
//   },
//   {
//     sql_expression: 'participant_oids',
//   },
//   {
//     sql_expression: 'resolution_time',
//   },
//   {
//     sql_expression: 'CAST(EXTRACT(HOUR FROM created_date) as VARCHAR)',
//   },
//   {
//     sql_expression: 'CAST (unsolved_age AS VARCHAR)',
//   },
//   {
//     sql_expression: 'fullname',
//   },
//   {
//     sql_expression: 'external_pod',
//   },
//   {
//     sql_expression: 'errors',
//   },
//   {
//     sql_expression: 'sr_number',
//   },
//   {
//     sql_expression: 'opp.created_date',
//   },
//   {
//     sql_expression: 'pipeline_created_quarter',
//   },
//   {
//     sql_expression:
//       "DATE_TRUNC(''week'', CASE WHEN state = ''closed'' THEN (actual_close_date)::TIMESTAMP ELSE COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP END)",
//   },
//   {
//     sql_expression:
//       "CASE WHEN on_hold_reason IS NOT NULL THEN on_hold_reason ELSE ''-'' END",
//   },
//   {
//     sql_expression: 'customer',
//   },
//   {
//     sql_expression:
//       " CASE WHEN external_system_type_su = ''ExternalSystemTypeEnum_ZENDESK'' THEN ''ZENDESK'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_JIRA'' THEN ''JIRA'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_HUBSPOT'' THEN ''HUBSPOT'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_GITHUB'' THEN ''GITHUB'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_SERVICENOW'' THEN ''SERVICENOW'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_SALESFORCE_SERVICE'' THEN ''SALESFORCE'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_ADAAS'' THEN ''ADAAS'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_LINEAR'' THEN ''LINEAR'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_ROCKETLANE'' THEN ''ROCKETLANE'' WHEN external_system_type_su = ''ExternalSystemTypeEnum_CONFLUENCE'' THEN ''CONFLUENCE''END",
//   },
//   {
//     sql_expression: 'end_of_month',
//   },
//   {
//     sql_expression: 'subscribers.events',
//   },
//   {
//     sql_expression:
//       "list_distinct(CAST(json_extract_string(links_json, ''$[*].target_object_type'') AS VARCHAR[]))",
//   },
//   {
//     sql_expression: 'tnt__closed_by',
//   },
//   {
//     sql_expression: 'revo_id',
//   },
//   {
//     sql_expression: 'is_contracted',
//   },
//   {
//     sql_expression: 'metric_timestamp',
//   },
//   {
//     sql_expression: "regexp_split_to_array(tnt__regions, ''\"|\\[\"|\"\\]'')",
//   },
//   {
//     sql_expression:
//       " CASE WHEN external_system_type = ''ExternalSystemTypeEnum_ZENDESK'' THEN ''ZENDESK'' WHEN external_system_type = ''ExternalSystemTypeEnum_JIRA'' THEN ''JIRA'' WHEN external_system_type = ''ExternalSystemTypeEnum_HUBSPOT'' THEN ''HUBSPOT'' WHEN external_system_type = ''ExternalSystemTypeEnum_GITHUB'' THEN ''GITHUB'' WHEN external_system_type = ''ExternalSystemTypeEnum_SERVICENOW'' THEN ''SERVICENOW'' WHEN external_system_type = ''ExternalSystemTypeEnum_SALESFORCE_SERVICE'' THEN ''SALESFORCE'' WHEN external_system_type = ''ExternalSystemTypeEnum_ADAAS'' THEN ''ADAAS'' WHEN external_system_type = ''ExternalSystemTypeEnum_LINEAR'' THEN ''LINEAR'' WHEN external_system_type = ''ExternalSystemTypeEnum_ROCKETLANE'' THEN ''ROCKETLANE'' END",
//   },
//   {
//     sql_expression: 'move_to_next_stage_days',
//   },
//   {
//     sql_expression: 'sprint_board_count',
//   },
//   {
//     sql_expression: 'ended_quarter',
//   },
//   {
//     sql_expression: 'dev_part_id',
//   },
//   {
//     sql_expression: "DATETRUNC(''week'', created_date)",
//   },
//   {
//     sql_expression:
//       "CASE WHEN DAYNAME(CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END) = ''Monday'' THEN 1 WHEN DAYNAME(CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END) = ''Tuesday'' THEN 2 WHEN DAYNAME(CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END) = ''Wednesday'' THEN 3 WHEN DAYNAME(CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END) = ''Thursday'' THEN 4 WHEN DAYNAME(CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END) = ''Friday'' THEN 5 WHEN DAYNAME(CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END) = ''Saturday'' THEN 6 ELSE 7 END",
//   },
//   {
//     sql_expression:
//       "(CASE WHEN dim_ticket.conv IS NOT NULL THEN ''yes'' ELSE ''no'' END)",
//   },
//   {
//     sql_expression: 'daily_unique_conversations',
//   },
//   {
//     sql_expression: 'triage_enum',
//   },
//   {
//     sql_expression: 'March_ID',
//   },
//   {
//     sql_expression: 'April_ID',
//   },
//   {
//     sql_expression: 'week',
//   },
//   {
//     sql_expression: 'ct',
//   },
//   {
//     sql_expression: 'tcd_status',
//   },
//   {
//     sql_expression: 'total_calls',
//   },
//   {
//     sql_expression: "IF(region_name IS NULL,''Other'', region_name)",
//   },
//   {
//     sql_expression: 'account',
//   },
//   {
//     sql_expression: 'record_date',
//   },
//   {
//     sql_expression: 'avg_time_taken_to_execute_query_for_dashboard_val',
//   },
//   {
//     sql_expression: 'min_time_taken_to_download_data_for_dashboard',
//   },
//   {
//     sql_expression: 'max_time_taken_to_create_widget',
//   },
//   {
//     sql_expression: 'avg_time_taken_to_create_widget',
//   },
//   {
//     sql_expression: 'avg_time_taken_to_update_widget',
//   },
//   {
//     sql_expression: 'tnt__region',
//   },
//   {
//     sql_expression: 'created_date',
//   },
//   {
//     sql_expression: 'apac_sum_acv',
//   },
//   {
//     sql_expression: 'category',
//   },
//   {
//     sql_expression: 'name',
//   },
//   {
//     sql_expression: 'region',
//   },
//   {
//     sql_expression: "IF(segment IS NULL,''None'',segment)",
//   },
//   {
//     sql_expression: 'date',
//   },
//   {
//     sql_expression: 'sales_data_dr.category',
//   },
//   {
//     sql_expression:
//       "CASE WHEN (range_state=''closed'') THEN ''E-support Closed'' ELSE ''E-support Opened'' END",
//   },
//   {
//     sql_expression: 'revo_don',
//   },
//   {
//     sql_expression: "datediff(''day'', created_date, current_date)",
//   },
//   {
//     sql_expression: 'avg_pr_deployed_time',
//   },
//   {
//     sql_expression: 'target_close_quarter',
//   },
//   {
//     sql_expression: 'sales_data_dr.created_at',
//   },
//   {
//     sql_expression: 'modified_date_hour_su',
//   },
//   {
//     sql_expression:
//       " CASE WHEN json_extract_string(metric, ''$.metric_definition_id'') == ''don:core:dvrv-us-1:devo/0:metric_definition/1'' THEN ''first_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') == ''don:core:dvrv-us-1:devo/0:metric_definition/2'' THEN ''next_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') = ''don:core:dvrv-us-1:devo/0:metric_definition/3'' THEN ''resolution''END ",
//   },
//   {
//     sql_expression: 'parent_id',
//   },
//   {
//     sql_expression: 'tnt__country',
//   },
//   {
//     sql_expression: '(actual_close_date)',
//   },
//   {
//     sql_expression: 'average_floor',
//   },
//   {
//     sql_expression: 'mau_percent_change',
//   },
//   {
//     sql_expression:
//       "CASE WHEN metric_status = ''hit'' THEN ''Achieved'' WHEN metric_status = ''miss'' THEN ''Missed'' END",
//   },
//   {
//     sql_expression:
//       "COALESCE(NULLIF(custom_fields ->> ''ctype__created_at_cfid'', '''')::TIMESTAMP, created_date::TIMESTAMP)",
//   },
//   {
//     sql_expression: 'retry_count',
//   },
//   {
//     sql_expression: 'comment_count',
//   },
//   {
//     sql_expression: 'daily_parts_created',
//   },
//   {
//     sql_expression: 'sprint_attachment',
//   },
//   {
//     sql_expression: 'source',
//   },
//   {
//     sql_expression: 'issue_owned_by_ids',
//   },
//   {
//     sql_expression: 'issue_tags',
//   },
//   {
//     sql_expression: 'CAST(date_broken AS TIMESTAMP)',
//   },
//   {
//     sql_expression: 'block',
//   },
//   {
//     sql_expression: 'issue_category',
//   },
//   {
//     sql_expression: 'signup_date',
//   },
//   {
//     sql_expression: 'CAST(ticket_created_date as date)',
//   },
//   {
//     sql_expression: 'device_manufacturer',
//   },
//   {
//     sql_expression:
//       "CASE WHEN json_extract_string(metric, ''$.metric_definition_id'') LIKE ''%:metric_definition/1'' THEN ''first_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') LIKE ''%:metric_definition/2'' THEN ''next_response'' WHEN json_extract_string(metric, ''$.metric_definition_id'') LIKE ''%:metric_definition/3'' THEN ''resolution'' END",
//   },
//   {
//     sql_expression: 'modified_date',
//   },
//   {
//     sql_expression: 'share_count_total_val',
//   },
//   {
//     sql_expression: 'state',
//   },
//   {
//     sql_expression: 'applies_to_id',
//   },
//   {
//     sql_expression: 'stage_ordinal',
//   },
//   {
//     sql_expression: 'annual_recurring_revenue',
//   },
//   {
//     sql_expression: "probability || '' %''",
//   },
//   {
//     sql_expression: 'console_type',
//   },
//   {
//     sql_expression: 'ticketSeverity',
//   },
//   {
//     sql_expression: 'issue_closed_date::TIMESTAMP',
//   },
//   {
//     sql_expression: 'group_name',
//   },
//   {
//     sql_expression: 'sales_data_dr.tnt__salesforceservice_accounts_type_cfid',
//   },
//   {
//     sql_expression: 'part_id_secondary',
//   },
//   {
//     sql_expression: 'created_date ',
//   },
//   {
//     sql_expression:
//       "CASE WHEN source = ''tickets'' THEN (actual_close_date)::TIMESTAMP ELSE  COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP END",
//   },
//   {
//     sql_expression: 'issue_date',
//   },
//   {
//     sql_expression: 'conversation',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''day'',target_close_date)",
//   },
//   {
//     sql_expression:
//       "case when category is NULL or category = '''' then ''Missing OCI Label'' else category end",
//   },
//   {
//     sql_expression: 'channels',
//   },
//   {
//     sql_expression:
//       "cast(json_extract_string(sla_summary, ''$.target_time'') as timestamp)",
//   },
//   {
//     sql_expression:
//       "CASE WHEN sla_summary = '''' THEN '''' WHEN sla_summary IS NULL THEN '''' WHEN sla_summary =''null'' THEN '''' ELSE json_extract_string(sla_summary, ''$.target_time'') END",
//   },
//   {
//     sql_expression: 'trial_end_time',
//   },
//   {
//     sql_expression: 'is_first_render',
//   },
//   {
//     sql_expression:
//       "DATE_TRUNC(''week'', CASE WHEN state = ''closed'' THEN COALESCE(custom_fields->>''ctype__closed_at_cfid'',actual_close_date)::TIMESTAMP ELSE COALESCE(custom_fields ->> ''ctype__created_at_cfid'', custom_fields ->> ''ctype__opened_at_cfid'', created_date)::TIMESTAMP END)",
//   },
//   {
//     sql_expression: 'state_distribution',
//   },
//   {
//     sql_expression:
//       "CASE WHEN translate( translate(json_extract(links_json, ''$[*].target_object_type''), CHR(34), ''''), ''[]'', '''' )=''issue'' THEN ''Escalated to issue'' ELSE ''Not escalated to issue'' END",
//   },
//   {
//     sql_expression: 'fy_year',
//   },
//   {
//     sql_expression: 'daily_devuser_logins',
//   },
//   {
//     sql_expression: 'api_success_count',
//   },
//   {
//     sql_expression: 'p95_upstream_service_time',
//   },
//   {
//     sql_expression: 'stage_enum',
//   },
//   {
//     sql_expression: 'June_Customers',
//   },
//   {
//     sql_expression: 'December_ID',
//   },
//   {
//     sql_expression:
//       "JSON_EXTRACT_STRING(custom_fields, ''$.tnt__good_category'')",
//   },
//   {
//     sql_expression: 'percent_group',
//   },
//   {
//     sql_expression:
//       "CASE WHEN source = ''tickets'' AND range_state = ''closed'' then actual_close_date::TIMESTAMP ELSE created_date::TIMESTAMP END",
//   },
//   {
//     sql_expression: 'record_time',
//   },
//   {
//     sql_expression: 'ux_evaluation',
//   },
//   {
//     sql_expression: 'dateRange',
//   },
//   {
//     sql_expression: 'created_by_id',
//   },
//   {
//     sql_expression: 'avg_avg_time_taken_to_create_dashboard_val',
//   },
//   {
//     sql_expression: 'tnt__product',
//   },
//   {
//     sql_expression: 'owned_by_ids',
//   },
//   {
//     sql_expression: 'verified_creator',
//   },
//   {
//     sql_expression: 'total_responses',
//   },
//   {
//     sql_expression: 'identified_date',
//   },
//   {
//     sql_expression: 'updation_type',
//   },
//   {
//     sql_expression:
//       "CASE WHEN cache_hit = false THEN ''MISS'' ELSE ''HIT'' END",
//   },
//   {
//     sql_expression: 'member_ids',
//   },
//   {
//     sql_expression: 'is_conversation_linked',
//   },
//   {
//     sql_expression: 'team_name',
//   },
//   {
//     sql_expression:
//       " CASE WHEN external_system_type = ''ExternalSystemTypeEnum_ZENDESK'' THEN ''ZENDESK'' WHEN external_system_type = ''ExternalSystemTypeEnum_JIRA'' THEN ''JIRA'' WHEN external_system_type = ''ExternalSystemTypeEnum_HUBSPOT'' THEN ''HUBSPOT'' WHEN external_system_type = ''ExternalSystemTypeEnum_GITHUB'' THEN ''GITHUB'' WHEN external_system_type = ''ExternalSystemTypeEnum_SERVICENOW'' THEN ''SERVICENOW'' WHEN external_system_type = ''ExternalSystemTypeEnum_SALESFORCE_SERVICE'' THEN ''SALESFORCE'' WHEN external_system_type = ''ExternalSystemTypeEnum_ADAAS'' THEN ''ADAAS'' WHEN external_system_type = ''ExternalSystemTypeEnum_LINEAR'' THEN ''LINEAR'' WHEN external_system_type = ''ExternalSystemTypeEnum_ROCKETLANE'' THEN ''ROCKETLANE''  WHEN external_system_type = ''ExternalSystemTypeEnum_CONFLUENCE'' THEN ''CONFLUENCE'' END",
//   },
//   {
//     sql_expression: 'dev_attendees',
//   },
//   {
//     sql_expression: 'tickets',
//   },
//   {
//     sql_expression: 'ticket_close_date',
//   },
//   {
//     sql_expression: 'split_tags',
//   },
//   {
//     sql_expression: 'branch_name',
//   },
//   {
//     sql_expression: 'DAYNAME(created_date)',
//   },
//   {
//     sql_expression:
//       "CASE WHEN stage_json->>''name'' in (''Work In Progress'', ''Support Work In Progress'') THEN ''In Progress'' WHEN stage_json->>''name'' IN (''On Hold'',''On-hold'') THEN ''On Hold'' ELSE stage_json->>''name'' END",
//   },
//   {
//     sql_expression: '(actual_close_date)::TIMESTAMP',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''month'',created_date)",
//   },
//   {
//     sql_expression: 'closure_code',
//   },
//   {
//     sql_expression:
//       "FLOOR(CAST(JSON_EXTRACT(surveys_aggregation_json, ''$[0].average'') AS INT))",
//   },
//   {
//     sql_expression: 'dim_ticket.severity_name',
//   },
//   {
//     sql_expression:
//       "CASE WHEN DATEDIFF(''day'', CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast(custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END, CURRENT_DATE) >= 1 THEN ''1 day'' WHEN DATEDIFF(''day'', CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast(custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END, CURRENT_DATE) >= 2 THEN ''1-2 days'' WHEN DATEDIFF(''day'', CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast(custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END, CURRENT_DATE) >= 7 THEN ''2-7 days'' WHEN DATEDIFF(''day'', CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast(custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END, CURRENT_DATE) >= 30 THEN ''7-30 days'' ELSE ''>30 days'' END",
//   },
//   {
//     sql_expression: 'node_id',
//   },
//   {
//     sql_expression: 'ae_region',
//   },
//   {
//     sql_expression: 'opp_owner_id',
//   },
//   {
//     sql_expression: 'average_csat_rating',
//   },
//   {
//     sql_expression: 'comment_date',
//   },
//   {
//     sql_expression: 'Total_Features_Planned',
//   },
//   {
//     sql_expression: 'turing_interacted',
//   },
//   {
//     sql_expression: 'awaiting_development_duration_days',
//   },
//   {
//     sql_expression: 'utm_campaign',
//   },
//   {
//     sql_expression: 'bare_metal_host_id',
//   },
//   {
//     sql_expression: 'health_state_duration_sum',
//   },
//   {
//     sql_expression: 'api_calls_count',
//   },
//   {
//     sql_expression: 'pod_element',
//   },
//   {
//     sql_expression: 'November_ID',
//   },
//   {
//     sql_expression: 'unnest(all_impacted_customers)',
//   },
//   {
//     sql_expression:
//       "CAST( strftime(''%H'', formatted_created_date) AS VARCHAR)",
//   },
//   {
//     sql_expression: 'network_type',
//   },
//   {
//     sql_expression: 'open_ttr',
//   },
//   {
//     sql_expression:
//       "CASE WHEN severity = 5 THEN ''Blocker'' WHEN severity = 6 THEN ''High'' WHEN severity = 7 THEN ''Medium'' WHEN severity = 8 THEN ''Low'' ELSE ''No Severity'' END",
//   },
//   {
//     sql_expression: 'na_sum_acv',
//   },
//   {
//     sql_expression: 'sentiment',
//   },
//   {
//     sql_expression: 'identifier',
//   },
//   {
//     sql_expression: 'total_arr_week',
//   },
//   {
//     sql_expression: "CAST( strftime(''%H'', created_date) AS VARCHAR)",
//   },
//   {
//     sql_expression:
//       "datediff(''day'', CASE WHEN custom_fields ->> ''ctype__created_at_cfid'' IS NOT NULL THEN Cast( custom_fields ->> ''ctype__created_at_cfid'' AS TIMESTAMP) ELSE created_date END, current_date)",
//   },
//   {
//     sql_expression: 'hardware',
//   },
//   {
//     sql_expression:
//       'IFNULL(time_to_open, 0) + IFNULL(time_to_review, 0) + IFNULL(time_to_approve, 0) + IFNULL(time_to_merge, 0)',
//   },
//   {
//     sql_expression: 'sales_data_dr.updated_at',
//   },
//   {
//     sql_expression: 'total_lines_modified',
//   },
//   {
//     sql_expression: 'sla_id',
//   },
//   {
//     sql_expression:
//       "cast(json_extract_string(staged_info, ''$.is_staged'') as boolean)",
//   },
//   {
//     sql_expression: 'valid_articles',
//   },
//   {
//     sql_expression:
//       "COALESCE(custom_fields->>''ctype__closed_at_cfid'',actual_close_date)::TIMESTAMP",
//   },
//   {
//     sql_expression: "custom_fields ->> ''ctype__total_alerts''",
//   },
//   {
//     sql_expression: 'daily_distinct_devuser_logins',
//   },
//   {
//     sql_expression: 'arr_month',
//   },
//   {
//     sql_expression: 'contract_gpu_qty',
//   },
//   {
//     sql_expression: 'last_updated_days',
//   },
//   {
//     sql_expression: 'ROUND(pct_unhealthy_sum * 100, 3)',
//   },
//   {
//     sql_expression: 'monthly_distinct_revuser_logins',
//   },
//   {
//     sql_expression: 'rev_users_count',
//   },
//   {
//     sql_expression: 'target_close_year',
//   },
//   {
//     sql_expression: 'created_date::TIMESTAMP',
//   },
//   {
//     sql_expression: 'max_upstream_service_time',
//   },
//   {
//     sql_expression: 'February_ID',
//   },
//   {
//     sql_expression: 'February_Customers',
//   },
//   {
//     sql_expression: 'July_ID',
//   },
//   {
//     sql_expression: 'September_ID',
//   },
//   {
//     sql_expression: 'acc_ct',
//   },
//   {
//     sql_expression: 'latest_health_status',
//   },
//   {
//     sql_expression: 'pipeline_created_date_quarter',
//   },
//   {
//     sql_expression: 'days_since_last_update',
//   },
//   {
//     sql_expression: 'count_val',
//   },
//   {
//     sql_expression: 'max_time_taken_to_download_data_for_dashboard',
//   },
//   {
//     sql_expression: 'action',
//   },
//   {
//     sql_expression: 'hour_of_day',
//   },
//   {
//     sql_expression:
//       "CASE WHEN issue_subtype = ''njuvewk7oxvgs4din5zgkltborygc43tnfqy4ltomx2f6mjrgmwtav3jonzvkzltfzss243xobwg64tu'' THEN ''E-Support Resolution Days'' WHEN issue_subtype = ''jira_uniphore.atlassian.net_engineering bug intake_issues.bug'' THEN ''E-Bug Resolution Days'' END",
//   },
//   {
//     sql_expression: 'customers',
//   },
//   {
//     sql_expression: 'project_status',
//   },
//   {
//     sql_expression: 'job_title',
//   },
//   {
//     sql_expression: 'sms_sent',
//   },
//   {
//     sql_expression: 'ended_date',
//   },
//   {
//     sql_expression: 'total_estimated_effort_hours',
//   },
//   {
//     sql_expression: 'account_name',
//   },
//   {
//     sql_expression: 'is_metric_completed',
//   },
//   {
//     sql_expression: 'name_su',
//   },
//   {
//     sql_expression: 'problem_area',
//   },
//   {
//     sql_expression: 'is_verified',
//   },
//   {
//     sql_expression: 'time_group',
//   },
//   {
//     sql_expression: 'opp_created_date',
//   },
//   {
//     sql_expression: 'sprints_count',
//   },
//   {
//     sql_expression: 'resolved',
//   },
//   {
//     sql_expression: 'previous_opp_stage',
//   },
//   {
//     sql_expression: 'job_date',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''day'', created_at)",
//   },
//   {
//     sql_expression: 'daily_issues_created',
//   },
//   {
//     sql_expression: 'Month',
//   },
//   {
//     sql_expression: "DATE_TRUNC(''day'', creation_time)",
//   },
//   {
//     sql_expression: 'January_ID',
//   },
//   {
//     sql_expression: 'August_Customers',
//   },
//   {
//     sql_expression: 'issue_reported_by_ids',
//   },
//   {
//     sql_expression: 'latest_issue_id',
//   },
//   {
//     sql_expression: 'OS',
//   },
//   {
//     sql_expression: 'dw_customer_oid',
//   },
//   {
//     sql_expression: 'completed_in',
//   },
//   {
//     sql_expression: 'mean_time_repair',
//   },
//   {
//     sql_expression: 'device_name',
//   },
//   {
//     sql_expression:
//       "MONTHNAME(old_record_date) || '' '' || CAST(YEAR(old_record_date) AS VARCHAR)",
//   },
//   {
//     sql_expression: 'min_latency',
//   },
// ];
