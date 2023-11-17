const objectDataSource = {
  dimensions: [
    {
      devrev_schema: {
        field_type: 'id',
        id_type: ['work'],
        is_filterable: true,
        name: 'id',
        ui: {
          display_name: 'Id',
        },
      },
      meerkat_schema: {
        sql_expression: 'id',
        type: 'string',
      },
      reference_name: 'id',
    },
    {
      devrev_schema: {
        field_type: 'enum',
        allowed_values: ['High', 'Medium', 'Low', 'Blocker'],
        is_filterable: true,
        name: 'severity_name',
        ui: {
          display_name: 'Severity',
        },
      },
      meerkat_schema: {
        sql_expression: 'severity_name',
        type: 'string',
      },
      reference_name: 'severity_name',
    },
    {
      devrev_schema: {
        field_type: 'composite',
        composite_type: 'stage',
        is_filterable: true,
        name: 'stage_id',
        ui: {
          display_name: 'Stage',
        },
      },
      meerkat_schema: {
        sql_expression: 'stage_id',
        type: 'string',
      },
      reference_name: 'stage_id',
    },
    {
      devrev_schema: {
        field_type: 'id',
        id_type: ['account'],
        is_filterable: true,
        name: 'account_id',
        ui: {
          display_name: 'Customer',
        },
      },
      meerkat_schema: {
        sql_expression: 'account_id',
        type: 'string',
      },
      reference_name: 'account_id',
    },
    {
      devrev_schema: {
        field_type: 'timestamp',
        is_filterable: true,
        name: 'created_date',
        ui: {
          display_name: 'Created Date',
        },
      },
      meerkat_schema: {
        sql_expression: 'created_date',
        type: 'time',
      },
      reference_name: 'created_date',
    },
    {
      devrev_schema: {
        field_type: 'timestamp',
        is_filterable: true,
        name: 'record_date',
        ui: {
          display_name: 'Record Date',
        },
      },
      meerkat_schema: {
        sql_expression: 'record_date',
        type: 'time',
      },
      reference_name: 'record_date',
    },
    {
      devrev_schema: {
        field_type: 'enum',
        allowed_values: [
          'active',
          'breached',
          'completed',
          'warning',
          'paused',
        ],
        is_filterable: true,
        name: 'sla_stage',
        ui: {
          display_name: 'SLA Stage',
        },
      },
      meerkat_schema: {
        sql_expression: 'sla_stage',
        type: 'string',
      },
      reference_name: 'sla_stage',
    },
    {
      devrev_schema: {
        field_type: 'enum',
        allowed_values: ['yes', 'no'],
        is_filterable: false,
        name: 'is_conversation_linked',
        ui: {
          display_name: 'Is Conversation Linked',
        },
      },
      meerkat_schema: {
        sql_expression: 'is_conversation_linked',
        type: 'string',
      },
      reference_name: 'is_conversation_linked',
    },
    {
      devrev_schema: {
        field_type: 'id',
        id_type: ['part'],
        is_filterable: true,
        name: 'primary_part_id',
        ui: {
          display_name: 'Part',
        },
      },
      meerkat_schema: {
        sql_expression: 'primary_part_id',
        type: 'string',
      },
      reference_name: 'primary_part_id',
    },
    {
      devrev_schema: {
        field_type: 'id',
        id_type: ['tag'],
        is_filterable: true,
        name: 'tag_ids',
        ui: {
          display_name: 'Tag',
        },
      },
      meerkat_schema: {
        sql_expression: 'tag_ids',
        type: 'string_array',
      },
      reference_name: 'tag_ids',
    },
    {
      devrev_schema: {
        field_type: 'id',
        id_type: ['group'],
        is_filterable: true,
        name: 'group_id',
        ui: {
          display_name: 'Part',
        },
      },
      meerkat_schema: {
        sql_expression: 'group_id',
        type: 'string',
      },
      reference_name: 'group_id',
    },
    {
      devrev_schema: {
        field_type: 'timestamp',
        is_filterable: true,
        name: 'record_hour',
        ui: {
          display_name: 'Range',
          unit: 'MMM d',
        },
      },
      meerkat_schema: {
        sql_expression: 'record_hour',
        type: 'time',
      },
      reference_name: 'record_hour',
    },
    {
      devrev_schema: {
        field_type: 'enum',
        allowed_values: ['yes', 'no'],
        is_filterable: false,
        name: 'ticket_prioritized',
        ui: {
          display_name: 'Ticket Prioritized',
        },
      },
      meerkat_schema: {
        sql_expression:
          "CASE WHEN primary_part_id LIKE '%enhancement%' OR is_issue_linked = 'yes' THEN 'yes' ELSE 'no' END",
        type: 'string',
      },
      reference_name: 'ticket_prioritized',
    },
    {
      devrev_schema: {
        field_type: 'id',
        id_type: ['part'],
        is_filterable: true,
        name: 'primary_part_id',
        ui: {
          display_name: 'Part',
        },
      },
      meerkat_schema: {
        sql_expression: 'primary_part_id',
        type: 'string',
      },
      reference_name: 'primary_part_id',
    },
    {
      devrev_schema: {
        field_type: 'enum',
        allowed_values: [
          'active',
          'breached',
          'completed',
          'warning',
          'paused',
        ],
        is_filterable: true,
        name: 'sla_stage',
        ui: {
          display_name: 'SLA Stage',
        },
      },
      meerkat_schema: {
        sql_expression: 'sla_stage',
        type: 'string',
      },
      reference_name: 'sla_stage',
    },
    {
      devrev_schema: {
        field_type: 'id',
        id_type: ['rev_org'],
        is_filterable: false,
        name: 'rev_oid',
        ui: {
          display_name: 'Customers',
        },
      },
      meerkat_schema: {
        sql_expression: 'rev_oid',
        type: 'string',
      },
      reference_name: 'rev_oid',
    },
    {
      devrev_schema: {
        field_type: 'id',
        id_type: ['dev_user', 'rev_user'],
        is_filterable: true,
        name: 'Owner',
        ui: {
          display_name: 'Owner',
        },
      },
      meerkat_schema: {
        sql_expression: 'owned_by_ids',
        type: 'string_array',
      },
      reference_name: 'owned_by_ids',
    },
    {
      devrev_schema: {
        field_type: 'array',
        base_type: 'id',
        id_type: ['dev_user', 'rev_user'],
        is_filterable: true,
        name: 'created_by_id',
        ui: {
          display_name: 'Created By',
        },
      },
      meerkat_schema: {
        sql_expression: 'created_by_id',
        type: 'string',
      },
      reference_name: 'created_by_id',
    },
  ],
  measures: [
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: false,
        name: 'total_first_resp_breaches',
        ui: {
          display_name: 'Total First Resp Breaches',
        },
      },
      meerkat_schema: {
        sql_expression: 'SUM(COALESCE(total_first_resp_breaches, 0))',
        type: 'number',
      },
      reference_name: 'total_first_resp_breaches',
    },
    {
      devrev_schema: {
        field_type: 'double',
        is_filterable: false,
        name: 'median_first_resp_time_arr',
        ui: {
          display_name: 'Median First Resp Time Arr',
        },
      },
      meerkat_schema: {
        sql_expression: 'MEDIAN(first_resp_time_arr)',
        type: 'number',
      },
      reference_name: 'median_first_resp_time_arr',
    },
    {
      devrev_schema: {
        field_type: 'double',
        is_filterable: false,
        name: 'median_next_resp_time_arr',
        ui: {
          display_name: 'Next First Resp Time Arr',
        },
      },
      meerkat_schema: {
        sql_expression: 'MEDIAN(next_resp_time_arr)',
        type: 'number',
      },
      reference_name: 'median_next_resp_time_arr',
    },
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: false,
        name: 'total_second_resp_breaches',
        ui: {
          display_name: 'Total Second Resp Breaches',
        },
      },
      meerkat_schema: {
        sql_expression: 'SUM(COALESCE(total_second_resp_breaches, 0))',
        type: 'number',
      },
      reference_name: 'total_second_resp_breaches',
    },
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: false,
        name: 'total_resolution_breaches',
        ui: {
          display_name: 'Total Resolution Breaches',
        },
      },
      meerkat_schema: {
        sql_expression: 'SUM(COALESCE(total_resolution_breaches, 0))',
        type: 'number',
      },
      reference_name: 'total_resolution_breaches',
    },
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: true,
        name: 'count_sla_stage',
        ui: {
          display_name: 'Count SLA Stage',
        },
      },
      meerkat_schema: {
        sql_expression: 'COUNT(sla_stage)',
        type: 'number',
      },
      reference_name: 'count_sla_stage',
    },
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: true,
        name: 'count_star',
        ui: {
          display_name: 'Count Star',
        },
      },
      meerkat_schema: {
        sql_expression: 'COUNT(*)',
        type: 'number',
      },
      reference_name: 'count_star',
    },
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: true,
        name: 'sum_total_responses',
        ui: {
          display_name: 'Sum Total Responses',
        },
      },
      meerkat_schema: {
        sql_expression: 'SUM(total_responses)',
        type: 'number',
      },
      reference_name: 'sum_total_responses',
    },
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: true,
        name: 'sum_total_survey_dispatched',
        ui: {
          display_name: 'Sum Total Survey Dispatched',
        },
      },
      meerkat_schema: {
        sql_expression: 'SUM(total_survey_dispatched)',
        type: 'number',
      },
      reference_name: 'sum_total_survey_dispatched',
    },
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: true,
        name: 'average_floor',
        ui: {
          display_name: 'Average Floor',
        },
      },
      meerkat_schema: {
        sql_expression:
          'FLOOR(SUM(sum_rating) OVER (PARTITION BY id) / SUM(total_responses) OVER (PARTITION BY id))',
        type: 'number',
      },
      reference_name: 'average_floor',
    },
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: true,
        name: 'count_over_id_partition',
        ui: {
          display_name: 'Count Over Id Partition',
        },
      },
      meerkat_schema: {
        sql_expression: 'COUNT(*) OVER (PARTITION BY id)',
        type: 'number',
      },
      reference_name: 'count_over_id_partition',
    },
    {
      devrev_schema: {
        field_type: 'int',
        is_filterable: true,
        name: 'total_ids_count',
        ui: {
          display_name: 'Total Ids Count',
        },
      },
      meerkat_schema: {
        sql_expression: 'COUNT(DISTINCT id)',
        type: 'number',
      },
      reference_name: 'total_ids_count',
    },
  ],
  oasis: {
    datasets: ['support_insights_ticket_metrics_summary'],
    sql_query:
      "SELECT *, DATE_TRUNC('day', record_hour) AS record_date FROM support_insights_ticket_metrics_summary",
    column_required: ['id', 'severity_name'],
  },
  reference_name: 'support_insights_ticket_metrics_summary',
  type: 'oasis',
};
