import { sqlQueryToAST } from '../sql-to-ast';

describe('filter-param-tests', () => {
  it('1. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    SELECT SUM(COALESCE(total_first_resp_breaches, 0)) AS support_insights_ticket_metrics_summary__total_first_resp_breaches ,  SUM(COALESCE(total_resolution_breaches, 0)) AS support_insights_ticket_metrics_summary__total_resolution_breaches ,   support_insights_ticket_metrics_summary__record_date FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, record_date AS support_insights_ticket_metrics_summary__record_date FROM (select DATE_TRUNC('day', record_hour) AS record_date, * from system.support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__record_date ORDER BY support_insights_ticket_metrics_summary__record_date ASC
    `;

    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'total_first_resp_breaches',
        'total_resolution_breaches',
      ])
    );
  });

  it('2. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    select count(*) as count from (SELECT COUNT(DISTINCT CASE WHEN state != 'closed' THEN id END) AS support_insights_ticket_metrics_summary__unique_ids_count ,   support_insights_ticket_metrics_summary__owned_by_ids FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, owned_by_ids AS support_insights_ticket_metrics_summary__owned_by_ids FROM (SELECT id, stage_id, account_id, subtype ,severity_name, created_date, state,source_channel,created_by_id, primary_part_id, tag_ids, group_id, sla_stage, rev_oid, record_hour,DATE_TRUNC('day', record_hour) as record_date, UNNEST(owned_by_ids) as owned_by_ids FROM system.support_insights_ticket_metrics_summary where record_date = (select MAX(record_date) from (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where state!='closed') as support_insights_ticket_metrics_summary  where  ((((support_insights_ticket_metrics_summary.record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary.record_date <= '2024-06-02T06:29:59.999Z')))))) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__owned_by_ids ORDER BY support_insights_ticket_metrics_summary__unique_ids_count DESC)
    `;

    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'state',
        'record_date',
        'record_hour',
        'owned_by_ids',
        'stage_id',
        'account_id',
        'subtype',
        'severity_name',
        'created_date',
        'source_channel',
        'created_by_id',
        'primary_part_id',
        'tag_ids',
        'group_id',
        'sla_stage',
        'rev_oid',
      ])
    );
  });

  it('3. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    SELECT COUNT(*) AS support_insights_ticket_metrics_summary__count_star ,   support_insights_ticket_metrics_summary__csat_score FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, csat_score AS support_insights_ticket_metrics_summary__csat_score FROM (SELECT *, DATE_TRUNC('day', record_hour) AS record_date,   CAST(CAST(FLOOR(sum_rating / total_responses) AS INTEGER) AS VARCHAR) AS csat_score FROM system.support_insights_ticket_metrics_summary WHERE total_responses != 0) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__csat_score ORDER BY support_insights_ticket_metrics_summary__csat_score ASC
    `;

    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'sum_rating',
        'total_responses',
        'csat_score',
      ])
    );
  });

  it('4. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    select count(*) as count from (SELECT COUNT(*) AS support_insights_ticket_metrics_summary__unique_ids_count ,   support_insights_ticket_metrics_summary__primary_part_id FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, primary_part_id AS support_insights_ticket_metrics_summary__primary_part_id FROM (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where record_date = (select MAX(record_date) from (SELECT * FROM (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where state!='closed' and primary_part_id is not null and primary_part_id!='' ORDER BY record_date DESC) as support_insights_ticket_metrics_summary  where  ((((support_insights_ticket_metrics_summary.record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary.record_date <= '2024-06-02T06:29:59.999Z')))))) and state!='closed') AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__primary_part_id ORDER BY support_insights_ticket_metrics_summary__unique_ids_count DESC)
    `;
    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'state',
        'primary_part_id',
      ])
    );
  });

  it('5. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    SELECT 100.0 * (COUNT(DISTINCT CASE WHEN severity_name = 'Blocker' AND state != 'closed' AND state IS NOT NULL THEN id END) / NULLIF(COUNT(DISTINCT CASE WHEN state != 'closed' AND state IS NOT NULL THEN id END), 0)) AS support_insights_ticket_metrics_summary__distinct_count  FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date FROM (select DATE_TRUNC('day', record_hour) AS record_date, * from system.support_insights_ticket_metrics_summary WHERE state!='closed' and state is not NULL) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z'))))
    `;
    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'severity_name',
        'state',
        'id',
      ])
    );
  });

  it('6. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    SELECT (select CASE WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL ) AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL ) THEN id END) > 0 THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 /(COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND ( total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL ) AND ( total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL )AND ( total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL )THEN id END))) ELSE NULL END) AS support_insights_ticket_metrics_summary__compliance_rate  FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date FROM (select DATE_TRUNC('day', record_hour) AS record_date, * from system.support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z'))))
    `;

    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'sla_stage',
        'id',
        'next_resp_time_arr',
        'first_resp_time_arr',
        'resolution_time_arr',
        'total_second_resp_breaches_ever',
        'total_first_resp_breaches_ever',
        'total_resolution_breaches_ever',
      ])
    );
  });

  it('7. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    SELECT CASE WHEN COUNT(CASE WHEN total_responses > 0 THEN 1 END) > 0 THEN COUNT(CASE WHEN total_responses > 0 AND floor(sum_rating / NULLIF(total_responses, 0)) IN (4, 5) THEN 1 END) * 100.0 / COUNT(CASE WHEN total_responses > 0 THEN 1 END) ELSE NULL END AS support_insights_ticket_metrics_summary__average_rating  FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date FROM (select DATE_TRUNC('day', record_hour) AS record_date, * from system.support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z'))))
    `;
    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'total_responses',
        'sum_rating',
      ])
    );
  });

  it('8. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    SELECT MEDIAN(DATEDIFF('minute', created_date, actual_close_date)) AS support_insights_ticket_metrics_summary__median_diff_minutes  FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date FROM (select DATE_TRUNC('day', record_hour) AS record_date, * FROM system.support_insights_ticket_metrics_summary WHERE state = 'closed' and actual_close_date > created_date) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z'))))
    `;
    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'created_date',
        'actual_close_date',
        'state',
      ])
    );
  });
});
