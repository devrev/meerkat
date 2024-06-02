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

  it('9. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    SELECT COUNT(*) AS support_insights_ticket_metrics_summary__unique_ids_count ,   support_insights_ticket_metrics_summary__account_id FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, account_id AS support_insights_ticket_metrics_summary__account_id FROM (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where record_date = (select MAX(record_date) from (SELECT * FROM (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where account_id!='' and account_id is not null and state!='closed' ORDER BY record_date DESC) as support_insights_ticket_metrics_summary  where  ((((support_insights_ticket_metrics_summary.record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary.record_date <= '2024-06-02T06:29:59.999Z')))))) and state!='closed') AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__account_id ORDER BY support_insights_ticket_metrics_summary__unique_ids_count DESC LIMIT 5
    `;

    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'account_id',
        'state',
      ])
    );
  });

  it('10. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    select count(*) as count from (SELECT COUNT(*) AS support_insights_ticket_metrics_summary__unique_ids_count ,   support_insights_ticket_metrics_summary__account_id FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, account_id AS support_insights_ticket_metrics_summary__account_id FROM (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where record_date = (select MAX(record_date) from (SELECT * FROM (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where account_id!='' and account_id is not null and state!='closed' ORDER BY record_date DESC) as support_insights_ticket_metrics_summary  where  ((((support_insights_ticket_metrics_summary.record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary.record_date <= '2024-06-02T06:29:59.999Z')))))) and state!='closed') AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__account_id ORDER BY support_insights_ticket_metrics_summary__unique_ids_count DESC)
    `;

    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'account_id',
        'state',
      ])
    );
  });

  it('11. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    SELECT COUNT(*) AS support_insights_ticket_metrics_summary__unique_ids_count ,   support_insights_ticket_metrics_summary__primary_part_id FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, primary_part_id AS support_insights_ticket_metrics_summary__primary_part_id FROM (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where record_date = (select MAX(record_date) from (SELECT * FROM (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where state!='closed' and primary_part_id is not null and primary_part_id!='' ORDER BY record_date DESC) as support_insights_ticket_metrics_summary  where  ((((support_insights_ticket_metrics_summary.record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary.record_date <= '2024-06-02T06:29:59.999Z')))))) and state!='closed') AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__primary_part_id ORDER BY support_insights_ticket_metrics_summary__unique_ids_count DESC LIMIT 5
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

  it('12. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(sla_stage) AS support_insights_ticket_metrics_summary__count_sla_stage ,   support_insights_ticket_metrics_summary__record_date,  support_insights_ticket_metrics_summary__sla_stage FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, record_date AS support_insights_ticket_metrics_summary__record_date, sla_stage AS support_insights_ticket_metrics_summary__sla_stage FROM (select DATE_TRUNC('day', record_hour) AS record_date, * from system.support_insights_ticket_metrics_summary where sla_stage IN ('breached', 'warning', 'paused')) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__record_date, support_insights_ticket_metrics_summary__sla_stage ORDER BY support_insights_ticket_metrics_summary__record_date ASC
    `;

    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining(['record_hour', 'record_date', 'sla_stage'])
    );
  });

  it('13. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT SUM(total_responses) AS support_insights_ticket_metrics_summary__sum_total_responses ,  SUM(total_survey_dispatched) AS support_insights_ticket_metrics_summary__sum_total_survey_dispatched ,   support_insights_ticket_metrics_summary__record_date FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, record_date AS support_insights_ticket_metrics_summary__record_date FROM (select DATE_TRUNC('day', record_hour) AS record_date, * from system.support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__record_date ORDER BY support_insights_ticket_metrics_summary__record_date ASC
    `;

    const references = await sqlQueryToAST(sql);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'record_hour',
        'record_date',
        'total_responses',
        'total_survey_dispatched',
      ])
    );
  });

  it('14. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(DISTINCT id) AS support_insights_ticket_metrics_summary__count_star ,   support_insights_ticket_metrics_summary__is_conversation_linked FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, is_conversation_linked AS support_insights_ticket_metrics_summary__is_conversation_linked FROM (SELECT   DATE_TRUNC('day', record_hour) AS record_date,   CASE       WHEN (primary_part_id LIKE '%enhancement%' OR is_issue_linked = 'yes') THEN 'yes'       ELSE 'no'   END AS ticket_prioritized,   * FROM system.support_insights_ticket_metrics_summary where state!='closed') AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__is_conversation_linked LIMIT 2
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
        'is_issue_linked',
        'id',
        'is_conversation_linked',
      ])
    );
  });

  it('15. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(DISTINCT id) AS support_insights_ticket_metrics_summary__count_star ,   support_insights_ticket_metrics_summary__ticket_prioritized FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, CASE WHEN primary_part_id LIKE '%enhancement%' OR is_issue_linked = 'yes' THEN 'yes' ELSE 'no' END AS support_insights_ticket_metrics_summary__ticket_prioritized FROM (SELECT *, DATE_TRUNC('day', record_hour) AS record_date FROM system.support_insights_ticket_metrics_summary where state!='closed') AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__ticket_prioritized
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
        'is_issue_linked',
        'id',
      ])
    );
  });

  it('16. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(DISTINCT CASE WHEN state != 'closed' THEN id END) AS support_insights_ticket_metrics_summary__unique_ids_count ,   support_insights_ticket_metrics_summary__owned_by_ids FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, owned_by_ids AS support_insights_ticket_metrics_summary__owned_by_ids FROM (SELECT id, stage_id, account_id, subtype ,severity_name, created_date, state,source_channel,created_by_id, primary_part_id, tag_ids, group_id, sla_stage, rev_oid, record_hour,DATE_TRUNC('day', record_hour) as record_date, UNNEST(owned_by_ids) as owned_by_ids FROM system.support_insights_ticket_metrics_summary where record_date = (select MAX(record_date) from (SELECT *, DATE_TRUNC('day', record_hour) as record_date FROM system.support_insights_ticket_metrics_summary where state!='closed') as support_insights_ticket_metrics_summary  where  ((((support_insights_ticket_metrics_summary.record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary.record_date <= '2024-06-02T06:29:59.999Z')))))) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__owned_by_ids ORDER BY support_insights_ticket_metrics_summary__unique_ids_count DESC LIMIT 5
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'stage_id',
        'account_id',
        'subtype',
        'severity_name',
        'created_date',
        'state',
        'source_channel',
        'created_by_id',
        'primary_part_id',
        'tag_ids',
        'group_id',
        'sla_stage',
        'rev_oid',
        'record_hour',
        'record_date',
        'owned_by_ids',
      ])
    );
  });

  it('17. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) THEN id END) AS support_insights_ticket_metrics_summary__unique_tickets_created, 
      COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', actual_close_date) = DATE_TRUNC('day', record_hour) THEN id END) AS support_insights_ticket_metrics_summary__unique_tickets_closed,
      support_insights_ticket_metrics_summary__record_date 
      FROM (
        SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, record_date AS support_insights_ticket_metrics_summary__record_date 
        FROM (
          SELECT *, DATE_TRUNC('day', record_hour) AS record_date
          FROM system.support_insights_ticket_metrics_summary
        ) AS support_insights_ticket_metrics_summary
      ) AS support_insights_ticket_metrics_summary
      WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z'))))
      GROUP BY support_insights_ticket_metrics_summary__record_date
      ORDER BY support_insights_ticket_metrics_summary__record_date ASC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'created_date',
        'actual_close_date',
        'record_hour',
      ])
    );
  });

  it('18. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        SUM(COALESCE(total_first_resp_breaches, 0)) AS support_insights_ticket_metrics_summary__total_first_resp_breaches,
        SUM(COALESCE(total_resolution_breaches, 0)) AS support_insights_ticket_metrics_summary__total_resolution_breaches,
        support_insights_ticket_metrics_summary__record_date
      FROM (
        SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, record_date AS support_insights_ticket_metrics_summary__record_date
        FROM (
          SELECT DATE_TRUNC('day', record_hour) AS record_date, *
          FROM system.support_insights_ticket_metrics_summary
        ) AS support_insights_ticket_metrics_summary
      ) AS support_insights_ticket_metrics_summary
      WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z'))))
      GROUP BY support_insights_ticket_metrics_summary__record_date
      ORDER BY support_insights_ticket_metrics_summary__record_date ASC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'total_first_resp_breaches',
        'total_resolution_breaches',
        'record_hour',
      ])
    );
  });

  it('19. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) THEN id END) AS support_insights_ticket_metrics_summary__unique_tickets_created,
        COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', actual_close_date) = DATE_TRUNC('day', record_hour) THEN id END) AS support_insights_ticket_metrics_summary__unique_tickets_closed,
        support_insights_ticket_metrics_summary__record_date
      FROM (
        SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, record_date AS support_insights_ticket_metrics_summary__record_date
        FROM (
          SELECT *, DATE_TRUNC('day', record_hour) AS record_date
          FROM system.support_insights_ticket_metrics_summary
        ) AS support_insights_ticket_metrics_summary
      ) AS support_insights_ticket_metrics_summary
      WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z'))))
      GROUP BY support_insights_ticket_metrics_summary__record_date
      ORDER BY support_insights_ticket_metrics_summary__record_date ASC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'created_date',
        'actual_close_date',
        'record_hour',
      ])
    );
  });

  it('20. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT id) AS support_insights_ticket_metrics_summary__count_star,
        support_insights_ticket_metrics_summary__ticket_prioritized
      FROM (
        SELECT *, 
          record_date AS support_insights_ticket_metrics_summary__record_date, 
          owned_by_ids AS support_insights_ticket_metrics_summary__owned_by_ids,
          CASE WHEN primary_part_id LIKE '%enhancement%' OR is_issue_linked = 'yes' THEN 'yes' ELSE 'no' END AS support_insights_ticket_metrics_summary__ticket_prioritized
        FROM (
          SELECT *, DATE_TRUNC('day', record_hour) AS record_date
          FROM system.support_insights_ticket_metrics_summary
          WHERE state!='closed'
        ) AS support_insights_ticket_metrics_summary
      ) AS support_insights_ticket_metrics_summary
      WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))
             AND (('don:identity:dvrv-us-1:devo/0:devu/157' = ANY(SELECT unnest(support_insights_ticket_metrics_summary__owned_by_ids)))))) 
      GROUP BY support_insights_ticket_metrics_summary__ticket_prioritized
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_ticket_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'primary_part_id',
        'is_issue_linked',
        'record_hour',
        'state',
        'owned_by_ids',
      ])
    );
  });

  it('21. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        (COUNT(DISTINCT CASE WHEN is_resolved_today='yes' THEN id END)/COUNT(DISTINCT id))*100 AS support_insights_conversation_metrics_summary__resolution_rate
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date
        FROM (
          SELECT *, DATE_TRUNC('day', record_hour) AS record_date
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining(['id', 'is_resolved_today', 'record_hour'])
    );
  });

  it('22. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        (SELECT 
          CASE 
            WHEN COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND (total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL) AND (total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL) AND (total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL) THEN id END) > 0 
            THEN 100 - (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) * 100.0 / (COUNT(DISTINCT CASE WHEN sla_stage = 'breached' THEN id END) + COUNT(DISTINCT CASE WHEN sla_stage = 'completed' AND (ARRAY_LENGTH(next_resp_time_arr) > 0 OR ARRAY_LENGTH(first_resp_time_arr) > 0 OR ARRAY_LENGTH(resolution_time_arr) > 0) AND (total_second_resp_breaches_ever = 0 OR total_second_resp_breaches_ever IS NULL) AND (total_first_resp_breaches_ever = 0 OR total_first_resp_breaches_ever IS NULL) AND (total_resolution_breaches_ever = 0 OR total_resolution_breaches_ever IS NULL) THEN id END))) 
            ELSE NULL 
          END
        ) AS support_insights_conversation_metrics_summary__compliance_rate
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date
        FROM (
          SELECT DATE_TRUNC('day', record_hour) AS record_date, *
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'sla_stage',
        'next_resp_time_arr',
        'first_resp_time_arr',
        'resolution_time_arr',
        'total_second_resp_breaches_ever',
        'total_first_resp_breaches_ever',
        'total_resolution_breaches_ever',
        'record_hour',
      ])
    );
  });

  it('23. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        CASE 
          WHEN COUNT(DISTINCT CASE WHEN total_responses > 0 THEN id END) > 0 
          THEN COUNT(DISTINCT CASE WHEN total_responses > 0 AND floor(sum_rating / NULLIF(total_responses, 0)) IN (4, 5) THEN id END) * 100.0 / COUNT(DISTINCT CASE WHEN total_responses > 0 THEN id END)
          ELSE NULL
        END AS support_insights_conversation_metrics_summary__average_rating
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date
        FROM (
          SELECT DATE_TRUNC('day', record_hour) AS record_date, *
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'total_responses',
        'sum_rating',
        'record_hour',
      ])
    );
  });

  it('24. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COALESCE(MEDIAN(first_resp_time), 0) AS support_insights_conversation_metrics_summary__median_first_resp_time
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date
        FROM (
          SELECT *,
            DATE_TRUNC('day', record_hour) AS record_date,
            UNNEST(
              CASE
                WHEN ARRAY_LENGTH(first_resp_time_arr) > 0 THEN first_resp_time_arr
                ELSE ARRAY[null]
              END
            ) AS first_resp_time
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(expect.arrayContaining(['first_resp_time_arr', 'record_hour']));
  });

  it('25. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__unique_ids_count,
        support_insights_conversation_metrics_summary__account_ids
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, account_ids AS support_insights_conversation_metrics_summary__account_ids
        FROM (
          SELECT 
            id, record_hour, owned_by_ids, sla_stage, source_channel, stage_name, tag_ids, applies_to_part_ids, participant_oids, group_id, created_date, created_by_id,
            UNNEST(account_ids) AS account_ids, 
            DATE_TRUNC('day', record_hour) AS record_date
          FROM system.support_insights_conversation_metrics_summary
          WHERE account_ids IS NOT NULL AND state != 'closed'
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__account_ids
      ORDER BY support_insights_conversation_metrics_summary__unique_ids_count DESC
      LIMIT 5
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'record_hour',
        'owned_by_ids',
        'sla_stage',
        'source_channel',
        'stage_name',
        'tag_ids',
        'applies_to_part_ids',
        'participant_oids',
        'group_id',
        'created_date',
        'created_by_id',
        'account_ids',
        'state',
      ])
    );
  });

  it('26. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(*) AS count
      FROM (
        SELECT
          COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__unique_ids_count,
          support_insights_conversation_metrics_summary__account_ids
        FROM (
          SELECT *,
            record_date AS support_insights_conversation_metrics_summary__record_date,
            account_ids AS support_insights_conversation_metrics_summary__account_ids
          FROM (
            SELECT
              id, record_hour, owned_by_ids, sla_stage, source_channel, stage_name, tag_ids, applies_to_part_ids, participant_oids, group_id, created_date, created_by_id,
              UNNEST(account_ids) AS account_ids,
              DATE_TRUNC('day', record_hour) AS record_date
            FROM system.support_insights_conversation_metrics_summary
            WHERE account_ids IS NOT NULL AND state != 'closed'
          ) AS support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
        WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z')
                AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
        GROUP BY support_insights_conversation_metrics_summary__account_ids
        ORDER BY support_insights_conversation_metrics_summary__unique_ids_count DESC
      )
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'record_hour',
        'owned_by_ids',
        'sla_stage',
        'source_channel',
        'stage_name',
        'tag_ids',
        'applies_to_part_ids',
        'participant_oids',
        'group_id',
        'created_date',
        'created_by_id',
        'account_ids',
        'state',
      ])
    );
  });

  it('27. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__unique_ids_count,
        support_insights_conversation_metrics_summary__applies_to_part_ids
      FROM (
        SELECT *, 
          record_date AS support_insights_conversation_metrics_summary__record_date, 
          applies_to_part_ids AS support_insights_conversation_metrics_summary__applies_to_part_ids
        FROM (
          SELECT 
            id, sla_stage, owned_by_ids, stage_name, group_id, tag_ids, participant_oids, account_ids, created_date, source_channel, created_by_id, record_hour, 
            UNNEST(applies_to_part_ids) AS applies_to_part_ids, 
            DATE_TRUNC('day', record_hour) AS record_date
          FROM system.support_insights_conversation_metrics_summary
          WHERE applies_to_part_ids IS NOT NULL
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__applies_to_part_ids
      ORDER BY support_insights_conversation_metrics_summary__unique_ids_count DESC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'sla_stage',
        'owned_by_ids',
        'stage_name',
        'group_id',
        'tag_ids',
        'participant_oids',
        'account_ids',
        'created_date',
        'source_channel',
        'created_by_id',
        'record_hour',
        'applies_to_part_ids',
        'record_date',
      ])
    );
  });

  it('28. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(*) AS count
      FROM (
        SELECT
          COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__unique_ids_count,
          support_insights_conversation_metrics_summary__applies_to_part_ids
        FROM (
          SELECT *,
            record_date AS support_insights_conversation_metrics_summary__record_date,
            applies_to_part_ids AS support_insights_conversation_metrics_summary__applies_to_part_ids
          FROM (
            SELECT
              id, sla_stage, owned_by_ids, stage_name, group_id, tag_ids, participant_oids, account_ids, created_date, source_channel, created_by_id, record_hour,
              UNNEST(applies_to_part_ids) AS applies_to_part_ids,
              DATE_TRUNC('day', record_hour) AS record_date
            FROM system.support_insights_conversation_metrics_summary
            WHERE applies_to_part_ids IS NOT NULL AND state != 'closed'
          ) AS support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
        WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z')
                AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
        GROUP BY support_insights_conversation_metrics_summary__applies_to_part_ids
        ORDER BY support_insights_conversation_metrics_summary__unique_ids_count DESC
      )
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'sla_stage',
        'owned_by_ids',
        'stage_name',
        'group_id',
        'tag_ids',
        'participant_oids',
        'account_ids',
        'created_date',
        'source_channel',
        'created_by_id',
        'record_hour',
        'applies_to_part_ids',
        'state',
      ])
    );
  });

  it('29. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        SUM(COALESCE(total_first_resp_breaches, 0)) AS support_insights_conversation_metrics_summary__total_first_resp_breaches,
        SUM(COALESCE(total_second_resp_breaches, 0)) AS support_insights_conversation_metrics_summary__sum_total_second_resp_breaches,
        SUM(COALESCE(total_resolution_breaches, 0)) AS support_insights_conversation_metrics_summary__total_resolution_breaches,
        support_insights_conversation_metrics_summary__record_date
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, record_date AS support_insights_conversation_metrics_summary__record_date
        FROM (
          SELECT DATE_TRUNC('day', record_hour) AS record_date, *
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__record_date
      ORDER BY support_insights_conversation_metrics_summary__record_date ASC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'total_first_resp_breaches',
        'total_second_resp_breaches',
        'total_resolution_breaches',
        'record_hour',
      ])
    );
  });

  it('30. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(sla_stage) AS support_insights_conversation_metrics_summary__count_sla_stage,
        support_insights_conversation_metrics_summary__record_date,
        support_insights_conversation_metrics_summary__sla_stage
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, record_date AS support_insights_conversation_metrics_summary__record_date, sla_stage AS support_insights_conversation_metrics_summary__sla_stage
        FROM (
          SELECT DATE_TRUNC('day', record_hour) AS record_date, *
          FROM system.support_insights_conversation_metrics_summary
          WHERE sla_stage IN ('paused', 'breached', 'warning')
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__record_date, support_insights_conversation_metrics_summary__sla_stage
      ORDER BY support_insights_conversation_metrics_summary__record_date ASC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(expect.arrayContaining(['sla_stage', 'record_hour']));
  });

  it('31. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        SUM(total_responses) AS support_insights_conversation_metrics_summary__sum_total_responses,
        SUM(total_survey_dispatched) AS support_insights_conversation_metrics_summary__sum_total_survey_dispatched,
        support_insights_conversation_metrics_summary__record_date
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, record_date AS support_insights_conversation_metrics_summary__record_date
        FROM (
          SELECT DATE_TRUNC('day', record_hour) AS record_date, *
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__record_date
      ORDER BY support_insights_conversation_metrics_summary__record_date ASC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'total_responses',
        'total_survey_dispatched',
        'record_hour',
      ])
    );
  });

  it('32. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__count_star,
        support_insights_conversation_metrics_summary__csat_score
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, csat_score AS support_insights_conversation_metrics_summary__csat_score
        FROM (
          SELECT *, DATE_TRUNC('day', record_hour) AS record_date,
            CAST(CAST(FLOOR(sum_rating / total_responses) AS INTEGER) AS VARCHAR) AS csat_score
          FROM system.support_insights_conversation_metrics_summary
          WHERE total_responses != 0
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__csat_score
      ORDER BY support_insights_conversation_metrics_summary__csat_score ASC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'sum_rating',
        'total_responses',
        'record_hour',
      ])
    );
  });

  it('33. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COALESCE(MEDIAN(first_resp_time), 0) AS support_insights_conversation_metrics_summary__median_first_resp_time,
        support_insights_conversation_metrics_summary__record_date
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, record_date AS support_insights_conversation_metrics_summary__record_date
        FROM (
          SELECT *, DATE_TRUNC('day', record_hour) AS record_date, 
            UNNEST(
              CASE 
                WHEN ARRAY_LENGTH(first_resp_time_arr) > 0 THEN first_resp_time_arr
                ELSE ARRAY[null]
              END
            ) AS first_resp_time
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__record_date
      ORDER BY support_insights_conversation_metrics_summary__record_date DESC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(expect.arrayContaining(['first_resp_time_arr', 'record_hour']));
  });

  it('34. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__count_star,
        support_insights_conversation_metrics_summary__is_ticket_linked
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, is_ticket_linked AS support_insights_conversation_metrics_summary__is_ticket_linked
        FROM (
          SELECT DATE_TRUNC('day', record_hour) AS record_date, * 
          FROM system.support_insights_conversation_metrics_summary
          WHERE state != 'closed'
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__is_ticket_linked
      LIMIT 2
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining(['id', 'is_ticket_linked', 'record_hour', 'state'])
    );
  });

  it('35. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT CASE WHEN DATE_TRUNC('day', created_date) = DATE_TRUNC('day', record_hour) THEN id END) AS support_insights_conversation_metrics_summary__unique_conversations_created,
        COUNT(DISTINCT CASE WHEN is_resolved_today='yes' THEN id END) AS support_insights_conversation_metrics_summary__unique_conversations_closed,
        support_insights_conversation_metrics_summary__record_date
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, record_date AS support_insights_conversation_metrics_summary__record_date
        FROM (
          SELECT *, DATE_TRUNC('day', record_hour) AS record_date
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__record_date
      ORDER BY support_insights_conversation_metrics_summary__record_date DESC
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'created_date',
        'is_resolved_today',
        'record_hour',
      ])
    );
  });

  it('36. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__count_distinct_id,
        support_insights_conversation_metrics_summary__source_channel
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, source_channel AS support_insights_conversation_metrics_summary__source_channel
        FROM (
          SELECT DATE_TRUNC('day', record_hour) AS record_date, *
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__source_channel
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(expect.arrayContaining(['id', 'source_channel', 'record_hour']));
  });

  it('37. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__unique_ids_count,
        support_insights_conversation_metrics_summary__owned_by_ids
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, owned_by_ids AS support_insights_conversation_metrics_summary__owned_by_ids
        FROM (
          SELECT 
            DATE_TRUNC('day', record_hour) AS record_date, record_hour, UNNEST(owned_by_ids) AS owned_by_ids, id, stage_name, applies_to_part_ids, tag_ids, group_id, account_ids, participant_oids, created_date, sla_stage, created_by_id, source_channel
          FROM system.support_insights_conversation_metrics_summary
          WHERE ARRAY_LENGTH(owned_by_ids) > 0 AND state != 'closed'
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__owned_by_ids
      ORDER BY support_insights_conversation_metrics_summary__unique_ids_count DESC
      LIMIT 5
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'record_hour',
        'owned_by_ids',
        'stage_name',
        'applies_to_part_ids',
        'tag_ids',
        'group_id',
        'account_ids',
        'participant_oids',
        'created_date',
        'sla_stage',
        'created_by_id',
        'source_channel',
        'state',
      ])
    );
  });

  it('38. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(*) AS count
      FROM (
        SELECT 
          COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__unique_ids_count,
          support_insights_conversation_metrics_summary__owned_by_ids
        FROM (
          SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, owned_by_ids AS support_insights_conversation_metrics_summary__owned_by_ids
          FROM (
            SELECT 
              DATE_TRUNC('day', record_hour) AS record_date, record_hour, UNNEST(owned_by_ids) AS owned_by_ids, id, stage_name, applies_to_part_ids, tag_ids, group_id, account_ids, participant_oids, created_date, sla_stage, created_by_id, source_channel
            FROM system.support_insights_conversation_metrics_summary
            WHERE ARRAY_LENGTH(owned_by_ids) > 0 AND state != 'closed'
          ) AS support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
        WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
                AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
        GROUP BY support_insights_conversation_metrics_summary__owned_by_ids
        ORDER BY support_insights_conversation_metrics_summary__unique_ids_count DESC
      )
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'record_hour',
        'owned_by_ids',
        'stage_name',
        'applies_to_part_ids',
        'tag_ids',
        'group_id',
        'account_ids',
        'participant_oids',
        'created_date',
        'sla_stage',
        'created_by_id',
        'source_channel',
        'state',
      ])
    );
  });

  it('39. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__count_star,
        support_insights_conversation_metrics_summary__turing_deflected
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, turing_deflected AS support_insights_conversation_metrics_summary__turing_deflected
        FROM (
          SELECT DISTINCT ON(id) id, DATE_TRUNC('day', record_hour) AS record_date, *, (CASE WHEN is_undeflected = 'yes' THEN 'no' ELSE 'yes' END) AS turing_deflected
          FROM system.support_insights_conversation_metrics_summary
          WHERE turing_interacted = 'yes'
          ORDER BY record_hour DESC
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__turing_deflected
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'is_undeflected',
        'turing_interacted',
        'record_hour',
      ])
    );
  });

  it('40. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COUNT(DISTINCT id) AS support_insights_conversation_metrics_summary__count_star,
        support_insights_conversation_metrics_summary__bot_resolution_rate
      FROM (
        SELECT *, record_date AS support_insights_conversation_metrics_summary__record_date, bot_resolution_rate AS support_insights_conversation_metrics_summary__bot_resolution_rate
        FROM (
          SELECT *, DATE_TRUNC('day', record_hour) AS record_date, 
            (CASE WHEN turing_interacted = 'yes' AND is_undeflected = 'no' THEN 'yes' ELSE 'no' END) AS bot_resolution_rate
          FROM system.support_insights_conversation_metrics_summary
          WHERE is_resolved_today = 'yes' AND turing_interacted = 'yes'
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T14:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T15:29:59.999Z'))))
      GROUP BY support_insights_conversation_metrics_summary__bot_resolution_rate
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'id',
        'turing_interacted',
        'is_undeflected',
        'is_resolved_today',
        'record_hour',
      ])
    );
  });

  it('41. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT 
        COALESCE(MEDIAN(first_resp_time), 0) AS support_insights_conversation_metrics_summary__median_first_resp_time
      FROM (
        SELECT *, stage_name AS support_insights_conversation_metrics_summary__stage_name, record_date AS support_insights_conversation_metrics_summary__record_date, group_id AS support_insights_conversation_metrics_summary__group_id
        FROM (
          SELECT *, DATE_TRUNC('day', record_hour) AS record_date,
            UNNEST(
              CASE
                WHEN ARRAY_LENGTH(first_resp_time_arr) > 0 THEN first_resp_time_arr
                ELSE ARRAY[null]
              END
            ) AS first_resp_time
          FROM system.support_insights_conversation_metrics_summary
        ) AS support_insights_conversation_metrics_summary
      ) AS support_insights_conversation_metrics_summary
      WHERE ((((support_insights_conversation_metrics_summary__stage_name = 'archived')) 
              AND ((support_insights_conversation_metrics_summary__record_date >= '2024-05-03T15:30:00.000Z') 
              AND (support_insights_conversation_metrics_summary__record_date <= '2024-06-02T16:29:59.999Z')) 
              AND ((support_insights_conversation_metrics_summary__group_id = 'don:identity:dvrv-us-1:devo/0:group/38'))))
    `;

    const references = await sqlQueryToAST(sql);
    console.log(references);
    expect(
      references['system.support_insights_conversation_metrics_summary']
    ).toEqual(
      expect.arrayContaining([
        'first_resp_time_arr',
        'stage_name',
        'record_hour',
        'group_id',
      ])
    );
  });
});
