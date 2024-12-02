import { getDatasetColumnsFromSQL } from '../sql-to-ast';

describe('SQL to Referenced Columns by Dataset', () => {
  it('1. Should return the correct referenced columns from original tables', async () => {
    const sql = `
    SELECT SUM(COALESCE(total_first_resp_breaches, 0)) AS support_insights_ticket_metrics_summary__total_first_resp_breaches ,  SUM(COALESCE(total_resolution_breaches, 0)) AS support_insights_ticket_metrics_summary__total_resolution_breaches ,   support_insights_ticket_metrics_summary__record_date FROM (SELECT *, record_date AS support_insights_ticket_metrics_summary__record_date, record_date AS support_insights_ticket_metrics_summary__record_date FROM (select DATE_TRUNC('day', record_hour) AS record_date, * from system.support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary) AS support_insights_ticket_metrics_summary WHERE ((((support_insights_ticket_metrics_summary__record_date >= '2024-05-03T05:30:00.000Z') AND (support_insights_ticket_metrics_summary__record_date <= '2024-06-02T06:29:59.999Z')))) GROUP BY support_insights_ticket_metrics_summary__record_date ORDER BY support_insights_ticket_metrics_summary__record_date ASC
    `;

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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
    const references = await getDatasetColumnsFromSQL(sql);
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
    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);
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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

    const references = await getDatasetColumnsFromSQL(sql);

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

  it('42. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(*) AS dim_opportunity__active_opportunity_count
      FROM (
        SELECT *
        FROM (
          SELECT *,
          FROM (
            SELECT *,
            UNNEST(owned_by_ids) AS owned_by_id,
            FROM system.dim_opportunity
            WHERE is_deleted = FALSE
          )
          WHERE state IN ('open', 'in_progress')
        ) AS dim_opportunity
      ) AS dim_opportunity
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.dim_opportunity']).toEqual(
      expect.arrayContaining(['owned_by_ids', 'is_deleted', 'state'])
    );
  });

  it('43. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT SUM(CASE WHEN state = 'open' OR state = 'in_progress' THEN (amount*fprobability/100) ELSE 0 END) AS dim_opportunity__opportunity_total_amount
      FROM (
        SELECT *
        FROM (
          SELECT *,
          UNNEST(owned_by_ids) AS owned_by_id,
          (CASE
            WHEN probability != 0 THEN probability
            WHEN forecast_category = 1 THEN 15
            WHEN forecast_category = 2 THEN 30
            WHEN forecast_category = 5 THEN 80
            WHEN forecast_category = 6 THEN 100
            WHEN forecast_category = 7 THEN 40
            WHEN forecast_category = 8 THEN 60
            WHEN probability = 0 THEN 5
          END) AS fprobability
          FROM system.dim_opportunity
          WHERE is_deleted = FALSE
        ) AS dim_opportunity
      ) AS dim_opportunity
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.dim_opportunity']).toEqual(
      expect.arrayContaining([
        'owned_by_ids',
        'is_deleted',
        'state',
        'amount',
        'probability',
        'forecast_category',
      ])
    );
  });

  it('44. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(*) AS dim_account__count_of_rows
      FROM (
        SELECT *
        FROM (
          SELECT *,
          owned_by AS owned_by_id,
          id AS account_id
          FROM system.dim_account
          WHERE is_deleted = FALSE
        ) AS dim_account
      ) AS dim_account
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.dim_account']).toEqual(
      expect.arrayContaining(['owned_by', 'id', 'is_deleted'])
    );
  });

  it('45. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT SUM(amount) AS dim_opportunity__sum_amount,
        MAX(stage_ordinal) AS dim_opportunity__distinct_stage_ordinal,
        dim_opportunity__stage_id,
        dim_opportunity__forecast_category_str
      FROM (
        SELECT *,
          stage_id AS dim_opportunity__stage_id,
          forecast_category_str AS dim_opportunity__forecast_category_str
        FROM (
          SELECT *,
            json_extract_string(stage_json, '$.ordinal')::double as stage_ordinal,
            'opportunity' as type,
          FROM (
            SELECT *,
            UNNEST(owned_by_ids) AS owned_by_id,
            JSON_EXTRACT_STRING(stage_json, '$.stage_id') AS stage_id,
            (CASE
              WHEN forecast_category = 1 THEN 'Omitted'
              WHEN forecast_category = 2 THEN 'Pipeline'
              WHEN forecast_category = 5 THEN 'Commit'
              WHEN forecast_category = 6 THEN 'Won'
              WHEN forecast_category = 7 THEN 'Upside'
              WHEN forecast_category = 8 THEN 'Strong Upside'
              WHEN forecast_category NOT IN (1,2,5,6,7,8) THEN 'Other'
            END) AS forecast_category_str
            FROM system.dim_opportunity
            WHERE is_deleted = FALSE
          )
        ) AS dim_opportunity
      ) AS dim_opportunity
      GROUP BY dim_opportunity__stage_id, dim_opportunity__forecast_category_str
      ORDER BY dim_opportunity__distinct_stage_ordinal ASC
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.dim_opportunity']).toEqual(
      expect.arrayContaining([
        'owned_by_ids',
        'is_deleted',
        'amount',
        'stage_json',
        'forecast_category',
      ])
    );
  });

  it('46. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT SUM(amount) AS dim_opportunity__sum_amount,
        SUM(SUM(amount)) OVER (PARTITION BY dim_opportunity__owned_by_id) AS dim_opportunity__total_sum_amount,
        dim_opportunity__owned_by_id,
        dim_opportunity__stage_id
      FROM (
        SELECT *,
          owned_by_id AS dim_opportunity__owned_by_id,
          stage_id AS dim_opportunity__stage_id
        FROM (
          SELECT *
          FROM (
            SELECT *,
              UNNEST(owned_by_ids) AS owned_by_id,
              JSON_EXTRACT_STRING(stage_json, '$.name') AS stage_enum_str,
              JSON_EXTRACT_STRING(stage_json, '$.stage_id') AS stage_id,
              'opportunity' AS type,
              REPLACE(CONCAT(UPPER(LEFT(stage_enum_str, 1)), SUBSTRING(stage_enum_str, 2)), '_', ' ') AS stage,
              (CASE
                WHEN forecast_category = 1 THEN 'Omitted'
                WHEN forecast_category = 2 THEN 'Pipeline'
                WHEN forecast_category = 5 THEN 'Commit'
                WHEN forecast_category = 6 THEN 'Won'
                WHEN forecast_category = 7 THEN 'Upside'
                WHEN forecast_category = 8 THEN 'Strong Upside'
                WHEN forecast_category NOT IN (1,2,5,6,7,8) THEN 'Other'
              END) AS forecast_category_str,
              (CASE WHEN state = 'open' THEN amount ELSE 0 END) AS 'Open',
              (CASE WHEN state = 'in_progress' THEN amount ELSE 0 END) AS 'In Progress',
              (CASE WHEN state = 'closed' THEN amount ELSE 0 END) AS 'Closed',
              (CASE
                WHEN probability != 0 THEN probability
                WHEN forecast_category = 1 THEN 15
                WHEN forecast_category = 2 THEN 30
                WHEN forecast_category = 5 THEN 80
                WHEN forecast_category = 6 THEN 100
                WHEN forecast_category = 7 THEN 40
                WHEN forecast_category = 8 THEN 60
                WHEN probability = 0 THEN 5
              END) AS fprobability,
              FROM system.dim_opportunity
              WHERE is_deleted = FALSE
          )
          WHERE owned_by_id IN (
            SELECT owned_by_id
            FROM (
              SELECT UNNEST(owned_by_ids) AS owned_by_id, is_deleted, created_date, created_by_id, amount
              FROM system.dim_opportunity
            ) AS dim_opportunity
            WHERE is_deleted = FALSE
              AND TRUE
              AND TRUE
              AND TRUE
            GROUP BY owned_by_id
            ORDER BY SUM(amount) DESC
            LIMIT 10
          )
        ) AS dim_opportunity
      ) AS dim_opportunity
      GROUP BY dim_opportunity__owned_by_id, dim_opportunity__stage_id
      ORDER BY dim_opportunity__total_sum_amount DESC
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.dim_opportunity']).toEqual(
      expect.arrayContaining([
        'owned_by_ids',
        'is_deleted',
        'created_date',
        'created_by_id',
        'amount',
        'stage_json',
        'state',
        'forecast_category',
        'probability',
      ])
    );
  });

  it('47. Should return the correct referenced columns from original tables', async () => {
    const sql = `
      SELECT COUNT(*) AS account_owner_wo_stage__count_of_rows,
        account_owner_wo_stage__owned_by_id
      FROM (
        SELECT *,
          owned_by_id AS account_owner_wo_stage__owned_by_id
        FROM (
          SELECT *,
            UNNEST(owned_by) AS owned_by_id,
            id AS account_id
          FROM system.dim_account
          WHERE is_deleted = FALSE
        ) AS account_owner_wo_stage
      ) AS account_owner_wo_stage
      GROUP BY account_owner_wo_stage__owned_by_id
      ORDER BY account_owner_wo_stage__count_of_rows
    `;

    const references = await getDatasetColumnsFromSQL(sql);
    expect(references['system.dim_account']).toEqual(
      expect.arrayContaining(['owned_by', 'id', 'is_deleted'])
    );
  });

  ///////////////////////
  // it('48. Should return the correct referenced columns from original tables', async () => {
  //   const sql = `
  //     SELECT
  //       (CASE
  //         WHEN (opp_forecast__opp_close_date >= (MAX(CASE WHEN date_diff('day', current_date(), opp_forecast__opp_close_date) < 0 THEN opp_forecast__opp_close_date ELSE '1754-08-30' END) OVER ()))
  //         THEN ((SUM(CASE WHEN SUM(forecast_amount) IS NOT NULL THEN SUM(forecast_amount) ELSE 0 END) OVER (ORDER BY opp_forecast__opp_close_date ROWS UNBOUNDED PRECEDING) + (SUM(CASE WHEN SUM(actual_amount) IS NOT NULL THEN SUM(actual_amount) ELSE 0 END) OVER (ORDER BY opp_forecast__opp_close_date ROWS UNBOUNDED PRECEDING))))
  //       END) AS opp_forecast__total_forecast_amount,
  //       (CASE
  //         WHEN (opp_forecast__opp_close_date <= (MAX(CASE WHEN date_diff('day', current_date(), opp_forecast__opp_close_date) < 0 THEN opp_forecast__opp_close_date ELSE '1754-08-30' END) OVER ()))
  //         THEN (SUM(SUM(actual_amount)) OVER (ORDER BY opp_forecast__opp_close_date ROWS UNBOUNDED PRECEDING))
  //       END) AS opp_forecast__total_actual_amount,
  //       opp_forecast__opp_close_date
  //     FROM (
  //       SELECT *,
  //         opp_close_date AS opp_forecast__opp_close_date
  //       FROM (
  //         SELECT *,
  //           UNNEST(owned_by_ids) AS owned_by_id
  //         FROM (
  //           (
  //             SELECT
  //               * exclude (actual_close_date, target_close_date),
  //               DATE_TRUNC('day', actual_close_date) AS opp_close_date,
  //               (amount) AS actual_amount
  //             FROM system.dim_opportunity
  //             WHERE
  //               extract('year' FROM opp_close_date) > 1900
  //               AND date_diff('day', current_date(), opp_close_date) < 0
  //               AND state = 'closed'
  //               AND forecast_category = 6
  //               AND is_deleted = FALSE
  //           )
  //           UNION BY NAME
  //           (
  //             SELECT
  //               * exclude fprobability,
  //               (amount*fprobability/100) AS forecast_amount,
  //             FROM (
  //               (
  //                 SELECT
  //                   * exclude (actual_close_date, target_close_date),
  //                   DATE_TRUNC('day', target_close_date) AS opp_close_date,
  //                   CASE
  //                     WHEN probability != 0 THEN probability
  //                     WHEN forecast_category = 1 THEN 15
  //                     WHEN forecast_category = 2 THEN 30
  //                     WHEN forecast_category = 5 THEN 80
  //                     WHEN forecast_category = 6 THEN 100
  //                     WHEN forecast_category = 7 THEN 40
  //                     WHEN forecast_category = 8 THEN 60
  //                     WHEN probability = 0 THEN 5
  //                   END AS fprobability
  //                 FROM system.dim_opportunity
  //                 WHERE
  //                   extract('year' FROM opp_close_date) > 1900
  //                   AND date_diff('day', current_date(), opp_close_date) >= 0
  //                   AND forecast_category != 1 AND state != 'closed'
  //                   AND is_deleted = FALSE
  //               )
  //             )
  //           )
  //         )
  //       ) AS opp_forecast
  //     ) AS opp_forecast
  //     GROUP BY opp_forecast__opp_close_date
  //     ORDER BY opp_forecast__opp_close_date ASC
  //   `;

  //   const references = await sqlQueryToAST(sql);
  //   expect(references['system.dim_opportunity'].sort()).toEqual(
  //     [
  //       'owned_by_ids',
  //       'is_deleted',
  //       'amount',
  //       'actual_close_date',
  //       'target_close_date',
  //       'state',
  //       'forecast_category',
  //       'probability',
  //     ].sort()
  //   );
  // });

  it('49. Should return the correct referenced columns from the original tables', async () => {
    const sql = `
      SELECT
        SUM(amount) AS dim_opportunity__sum_amount,
        SUM(SUM(amount)) OVER (PARTITION BY dim_opportunity__owned_by_id) AS dim_opportunity__total_sum_amount,
        dim_opportunity__owned_by_id,
        dim_opportunity__stage_id
      FROM (
        SELECT *,
          owned_by_id AS dim_opportunity__owned_by_id,
          stage_id AS dim_opportunity__stage_id
        FROM (
          SELECT *
          FROM (
            SELECT
              *,
              UNNEST(owned_by_ids) AS owned_by_id,
              JSON_EXTRACT_STRING(stage_json, '$.name') AS stage_enum_str,
              JSON_EXTRACT_STRING(stage_json, '$.stage_id') AS stage_id,
              'opportunity' AS type,
              REPLACE(CONCAT(UPPER(LEFT(stage_enum_str, 1)), SUBSTRING(stage_enum_str, 2)), '_', ' ') AS stage,
              (CASE
                WHEN forecast_category = 1 THEN 'Omitted'
                WHEN forecast_category = 2 THEN 'Pipeline'
                WHEN forecast_category = 5 THEN 'Commit'
                WHEN forecast_category = 6 THEN 'Won'
                WHEN forecast_category = 7 THEN 'Upside'
                WHEN forecast_category = 8 THEN 'Strong Upside'
                WHEN forecast_category NOT IN (1,2,5,6,7,8) THEN 'Other'
              END) AS forecast_category_str,
              (CASE WHEN state = 'open' THEN amount ELSE 0 END) AS 'Open',
              (CASE WHEN state = 'in_progress' THEN amount ELSE 0 END) AS 'In Progress',
              (CASE WHEN state = 'closed' THEN amount ELSE 0 END) AS 'Closed',
              (CASE
                WHEN probability!=0 THEN probability
                WHEN forecast_category = 1 THEN 15
                WHEN forecast_category = 2 THEN 30
                WHEN forecast_category = 5 THEN 80
                WHEN forecast_category = 6 THEN 100
                WHEN forecast_category = 7 THEN 40
                WHEN forecast_category = 8 THEN 60
                WHEN probability=0 THEN 5
              END) AS fprobability,
              FROM system.dim_opportunity
              WHERE is_deleted = FALSE
          )
          WHERE owned_by_id IN (
            SELECT owned_by_id
            FROM (
              SELECT
                UNNEST(owned_by_ids) AS owned_by_id,
                is_deleted,
                created_date,
                created_by_id,
                amount
              FROM system.dim_opportunity
            ) AS dim_opportunity
            WHERE
              is_deleted = FALSE
              AND TRUE
              AND TRUE
              AND TRUE
            GROUP BY owned_by_id
            ORDER BY SUM(amount) DESC
            LIMIT 10
          )
        ) AS dim_opportunity
      ) AS dim_opportunity
      GROUP BY dim_opportunity__owned_by_id, dim_opportunity__stage_id
      ORDER BY dim_opportunity__total_sum_amount DESC
    `;

    const references = await getDatasetColumnsFromSQL(sql);
    expect(references['system.dim_opportunity']).toEqual(
      expect.arrayContaining([
        'owned_by_ids',
        'is_deleted',
        'amount',
        'stage_json',
        'forecast_category',
        'state',
        'probability',
        'created_date',
        'created_by_id',
      ])
    );
  });

  it('50. Should return the correct referenced columns from the original tables', async () => {
    const sql = `
      SELECT COUNT(*) AS dim_revu_slim__count_of_rows
      FROM (
        SELECT *
        FROM (
          SELECT
            *,
            (
              SELECT account
              FROM system.dim_revo
              WHERE dim_revo.is_deleted = FALSE
              AND dim_revo.display_id = dim_revu_slim.rev_oid
              AND dim_revo.dev_oid = dim_revu_slim.dev_oid
            ) AS account_id
          FROM system.dim_revu_slim
          WHERE is_deleted = FALSE
        ) AS dim_revu_slim
      ) AS dim_revu_slim
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.dim_revu_slim'].sort()).toEqual(
      ['is_deleted', 'rev_oid', 'dev_oid'].sort()
    );

    expect(references['system.dim_revo'].sort()).toEqual(
      ['is_deleted', 'display_id', 'dev_oid', 'account'].sort()
    );
  });

  it('51. Should return the correct columns from the original tables', async () => {
    const sql = `
      SELECT COUNT(*) AS engage_customers__count_of_rows ,   engage_customers__scheduled_date,  engage_customers__engagement_type_str 
      FROM (
        SELECT *, scheduled_date AS engage_customers__scheduled_date, scheduled_date AS engage_customers__scheduled_date, engagement_type_str AS engage_customers__engagement_type_str 
        FROM (
          select *,
            date_trunc('day', created_timestamp) as created_date,
            date_trunc('day', scheduled_timestamp) as scheduled_date,
            (
              CASE
                WHEN engagement_type = 1 THEN 'Meeting'
                WHEN engagement_type = 2 THEN 'Survey'
                WHEN engagement_type = 3 THEN 'Default'
                WHEN engagement_type = 4 THEN 'LinkedIn'
                WHEN engagement_type = 5 THEN 'Call'
                WHEN engagement_type = 6 THEN 'Offline'
                WHEN engagement_type = 7 THEN 'Email'
                ELSE 'Unknown'
              END
            ) as engagement_type_str
          from (
            select
              *
            from
              (
                select
                  dim_opportunity.account_id as parent_acc_id,
                  a.id,
                  a.display_id,
                  a.engagement_type,
                  a.created_date as created_timestamp,
                  a.parent_id,
                  a.member_ids,
                  a.title,
                  a.scheduled_date as scheduled_timestamp,
                  a.created_by_id as created_by_id
                from
                  (
                    select
                      *
                    from system.dim_engagement
                    where
                      parent_id like 'don:core%' and is_deleted = false
                  ) as a
                join system.dim_opportunity on dim_opportunity.id = a.parent_id
              )
            UNION ALL
              (
                select
                  parent_id as parent_acc_id,
                  id,
                  display_id,
                  engagement_type,
                  created_date as created_timestamp,
                  parent_id,
                  member_ids,
                  title,
                  scheduled_date as scheduled_timestamp,
                  created_by_id
                from system.dim_engagement
                where
                  parent_id like 'don:identity%' and is_deleted = false
              )
          )) AS engage_customers
        ) AS engage_customers
      WHERE (
        (
          (engage_customers__scheduled_date >= '2024-05-04T06:30:00.000Z')
          AND (engage_customers__scheduled_date <= '2024-06-03T07:29:59.999Z')
        )
      )
      GROUP BY engage_customers__scheduled_date, engage_customers__engagement_type_str
      ORDER BY engage_customers__scheduled_date ASC
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.dim_engagement'].sort()).toEqual(
      [
        'id',
        'display_id',
        'engagement_type',
        'created_date',
        'parent_id',
        'member_ids',
        'title',
        'scheduled_date',
        'created_by_id',
        'is_deleted',
      ].sort()
    );

    expect(references['system.dim_opportunity'].sort()).toEqual(
      ['id', 'account_id'].sort()
    );
  });

  it('52. Should return the correct count of rows from dim_revu_slim table', async () => {
    const sql = `
      SELECT COUNT(*) AS dim_revu_slim__count_of_rows
      FROM (
        SELECT *,
               created_date AS dim_revu_slim__created_date
        FROM (
          SELECT *
                 exclude(rev_oid),
                 CASE WHEN is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
                 'don:identity:dvrv-us-1:devo/' || SUBSTRING(dev_oid FROM POSITION('-' IN dev_oid) + 1) || ':revo/' || SUBSTRING(rev_oid FROM POSITION('-' IN rev_oid) + 1) AS rev_oid
          FROM system.dim_revu_slim
          WHERE is_deleted = FALSE
        ) AS dim_revu_slim
      ) AS dim_revu_slim
      WHERE (
        (
          (dim_revu_slim__created_date >= '2024-05-04T10:30:00.000Z')
          AND (dim_revu_slim__created_date <= '2024-06-03T11:29:59.999Z')
        )
      )
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.dim_revu_slim'].sort()).toEqual(
      ['rev_oid', 'dev_oid', 'is_verified', 'is_deleted', 'created_date'].sort()
    );
  });

  it('53. Should return the correct average daily active users from summary_grow_daily_active_users table', async () => {
    const sql = `
      SELECT ROUND(SUM(daily_active_users)/COUNT(DISTINCT(created_date)), 0) AS summary_grow_daily_active_users_grp_by_date__avg_daily_active_users
      FROM (
        SELECT *,
               created_date AS summary_grow_daily_active_users_grp_by_date__created_date
        FROM (
          SELECT *
                 exclude (rev_oid),
                 rev_oid AS prev,
                 created_at as created_date,
                 (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND summary_grow_daily_active_users.dev_oid = dim_revo.dev_oid AND summary_grow_daily_active_users.rev_oid = dim_revo.display_id) AS rev_oid,
                 CASE WHEN is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum
          FROM system.summary_grow_daily_active_users
        ) AS summary_grow_daily_active_users_grp_by_date
      ) AS summary_grow_daily_active_users_grp_by_date
      WHERE (
        (
          (summary_grow_daily_active_users_grp_by_date__created_date >= '2024-05-04T10:30:00.000Z')
          AND (summary_grow_daily_active_users_grp_by_date__created_date <= '2024-06-03T11:29:59.999Z')
        )
      )
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.summary_grow_daily_active_users'].sort()).toEqual(
      [
        'rev_oid',
        'dev_oid',
        'is_verified',
        'created_at',
        'daily_active_users',
      ].sort()
    );

    expect(references['system.dim_revo'].sort()).toEqual(
      ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
    );
  });

  it('54. Should return the correct total user sessions from user_sessions_track_events_summary table', async () => {
    const sql = `
      SELECT SUM(total_user_sessions) AS user_sessions_track_events_summary__total_user_sessions
      FROM (
        SELECT *,
               created_date AS user_sessions_track_events_summary__created_date
        FROM (
          SELECT *
                 exclude (rev_oid),
                 created_at as created_date,
                 CASE WHEN is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
                 (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND user_sessions_track_events_summary.dev_oid = dim_revo.dev_oid AND user_sessions_track_events_summary.rev_oid = dim_revo.display_id) AS rev_oid
          FROM system.user_sessions_track_events_summary
        ) AS user_sessions_track_events_summary
      ) AS user_sessions_track_events_summary
      WHERE (
        (
          (user_sessions_track_events_summary__created_date >= '2024-05-04T10:30:00.000Z')
          AND (user_sessions_track_events_summary__created_date <= '2024-06-03T11:29:59.999Z')
        )
      )
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(
      references['system.user_sessions_track_events_summary'].sort()
    ).toEqual(
      [
        'rev_oid',
        'dev_oid',
        'is_verified',
        'created_at',
        'total_user_sessions',
      ].sort()
    );

    expect(references['system.dim_revo'].sort()).toEqual(
      ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
    );
  });

  it('55. Should return the correct median session length from user_sessions_track_events_summary table', async () => {
    const sql = `
      SELECT MEDIAN(CASE WHEN total_user_sessions != 0 THEN (total_session_length/total_user_sessions) ELSE 0 END) AS user_sessions_track_events_summary__median_session_length
      FROM (
        SELECT *,
               created_date AS user_sessions_track_events_summary__created_date
        FROM (
          SELECT *
                 exclude (rev_oid),
                 created_at as created_date,
                 CASE WHEN is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
                 (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND user_sessions_track_events_summary.dev_oid = dim_revo.dev_oid AND user_sessions_track_events_summary.rev_oid = dim_revo.display_id) AS rev_oid
          FROM system.user_sessions_track_events_summary
        ) AS user_sessions_track_events_summary
      ) AS user_sessions_track_events_summary
      WHERE (
        (
          (user_sessions_track_events_summary__created_date >= '2024-05-04T10:30:00.000Z')
          AND (user_sessions_track_events_summary__created_date <= '2024-06-03T11:29:59.999Z')
        )
      )
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(
      references['system.user_sessions_track_events_summary'].sort()
    ).toEqual(
      [
        'rev_oid',
        'dev_oid',
        'is_verified',
        'created_at',
        'total_user_sessions',
        'total_session_length',
      ].sort()
    );

    expect(references['system.dim_revo'].sort()).toEqual(
      ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
    );
  });

  it('56. Should return the correct rolling average DAU, total DAU, and total returning users from summary_grow_daily_active_users table', async () => {
    const sql = `
      SELECT ROUND(AVG(SUM(daily_active_users)) OVER (ORDER BY summary_grow_daily_active_users__created_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 0) AS summary_grow_daily_active_users__rolling_avg_dau,
             SUM(daily_active_users) AS summary_grow_daily_active_users__total_daily_active_users,
             SUM(returning_users_count) AS summary_grow_daily_active_users__total_returning_users,
             summary_grow_daily_active_users__created_date
      FROM (
        SELECT *,
               created_date AS summary_grow_daily_active_users__created_date,
               created_date AS summary_grow_daily_active_users__created_date
        FROM (
          SELECT *
                 exclude (rev_oid),
                 rev_oid AS prev,
                 created_at as created_date,
                 CASE WHEN is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
                 (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND summary_grow_daily_active_users.dev_oid = dim_revo.dev_oid AND summary_grow_daily_active_users.rev_oid = dim_revo.display_id) AS rev_oid
          FROM system.summary_grow_daily_active_users
        ) AS summary_grow_daily_active_users
      ) AS summary_grow_daily_active_users
      WHERE (
        (
          (summary_grow_daily_active_users__created_date >= '2024-05-04T10:30:00.000Z')
          AND (summary_grow_daily_active_users__created_date <= '2024-06-03T11:29:59.999Z')
        )
      )
      GROUP BY summary_grow_daily_active_users__created_date
      ORDER BY summary_grow_daily_active_users__created_date ASC
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(references['system.summary_grow_daily_active_users'].sort()).toEqual(
      [
        'rev_oid',
        'dev_oid',
        'is_verified',
        'created_at',
        'daily_active_users',
        'returning_users_count',
      ].sort()
    );

    expect(references['system.dim_revo'].sort()).toEqual(
      ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
    );
  });

  it('57. Should return the correct total user sessions grouped by date from user_sessions_track_events_summary table', async () => {
    const sql = `
      SELECT SUM(total_user_sessions) AS user_sessions_track_events_summary__total_user_sessions,
             user_sessions_track_events_summary__created_date
      FROM (
        SELECT *,
               created_date AS user_sessions_track_events_summary__created_date,
               created_date AS user_sessions_track_events_summary__created_date
        FROM (
          SELECT *
                 exclude (rev_oid),
                 created_at as created_date,
                 CASE WHEN is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
                 (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND user_sessions_track_events_summary.dev_oid = dim_revo.dev_oid AND user_sessions_track_events_summary.rev_oid = dim_revo.display_id) AS rev_oid
          FROM system.user_sessions_track_events_summary
        ) AS user_sessions_track_events_summary
      ) AS user_sessions_track_events_summary
      WHERE (
        (
          (user_sessions_track_events_summary__created_date >= '2024-05-04T10:30:00.000Z')
          AND (user_sessions_track_events_summary__created_date <= '2024-06-03T11:29:59.999Z')
        )
      )
      GROUP BY user_sessions_track_events_summary__created_date
      ORDER BY user_sessions_track_events_summary__created_date ASC
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(
      references['system.user_sessions_track_events_summary'].sort()
    ).toEqual(
      [
        'rev_oid',
        'dev_oid',
        'is_verified',
        'created_at',
        'total_user_sessions',
      ].sort()
    );

    expect(references['system.dim_revo'].sort()).toEqual(
      ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
    );
  });

  it('58. Should return the correct average session length grouped by date from user_sessions_track_events_summary table', async () => {
    const sql = `
      SELECT CASE WHEN SUM(total_user_sessions) != 0 THEN (SUM(total_session_length) / SUM(total_user_sessions)) ELSE 0 END AS user_sessions_track_events_summary__avg_session_length,
             user_sessions_track_events_summary__created_date
      FROM (
        SELECT *,
               created_date AS user_sessions_track_events_summary__created_date,
               created_date AS user_sessions_track_events_summary__created_date
        FROM (
          SELECT *
                 exclude (rev_oid),
                 created_at as created_date,
                 CASE WHEN is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
                 (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND user_sessions_track_events_summary.dev_oid = dim_revo.dev_oid AND user_sessions_track_events_summary.rev_oid = dim_revo.display_id) AS rev_oid
          FROM system.user_sessions_track_events_summary
        ) AS user_sessions_track_events_summary
      ) AS user_sessions_track_events_summary
      WHERE (
        (
          (user_sessions_track_events_summary__created_date >= '2024-05-04T10:30:00.000Z')
          AND (user_sessions_track_events_summary__created_date <= '2024-06-03T11:29:59.999Z')
        )
      )
      GROUP BY user_sessions_track_events_summary__created_date
      ORDER BY user_sessions_track_events_summary__created_date ASC
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(
      references['system.user_sessions_track_events_summary'].sort()
    ).toEqual(
      [
        'rev_oid',
        'dev_oid',
        'is_verified',
        'created_at',
        'total_user_sessions',
        'total_session_length',
      ].sort()
    );

    expect(references['system.dim_revo'].sort()).toEqual(
      ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
    );
  });

  it('59. Should return the correct cumulative user count by date from dim_revu_slim and dim_revo tables', async () => {
    const sql = `
      SELECT total_users_data__created_date,
             total_users_data__cumulative_user_count
      FROM (
        SELECT *,
               created_date AS total_users_data__created_date,
               created_date AS total_users_data__created_date,
               cumulative_user_count AS total_users_data__cumulative_user_count
        FROM (
          SELECT created_date_day AS created_date,
                 COALESCE(cumulative_user_count, LAST_VALUE(cumulative_user_count IGNORE NULLS) OVER (ORDER BY created_date_day)) AS cumulative_user_count,
                 COALESCE(rev_oid, LAST_VALUE(rev_oid IGNORE NULLS) OVER (ORDER BY created_date_day)) AS rev_oid,
                 verified_enum
          FROM ((
            SELECT *
            FROM (
              SELECT unnest(generate_series(MIN(DATE_TRUNC('day', created_date)), MAX(DATE_TRUNC('day', created_date)), INTERVAL '1 DAY')) AS created_date_day
              FROM system.dim_revu_slim
            ) AS date_series
            LEFT JOIN (
              SELECT (SUM(COUNT(*)) OVER (ORDER BY created_date_day ROWS UNBOUNDED PRECEDING)) AS cumulative_user_count,
                     created_date_day,
                     ANY_VALUE(rev_oid) AS rev_oid,
                     ANY_VALUE(verified_enum) AS verified_enum
              FROM (
                WITH filtered_revu AS (
                  SELECT *
                  FROM system.dim_revu_slim
                  WHERE is_deleted = FALSE
                ),
                filtered_revo AS (
                  SELECT *,
                         id as rev_oid
                  FROM system.dim_revo
                  WHERE is_deleted = FALSE
                )
                SELECT filtered_revu.id AS id,
                       filtered_revo.id AS rev_oid,
                       DATE_TRUNC('day', filtered_revu.created_date) AS created_date_day,
                       CASE WHEN filtered_revu.is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum
                FROM filtered_revu
                LEFT JOIN filtered_revo ON filtered_revu.rev_oid = filtered_revo.display_id
              ) AS total_users_data
              WHERE TRUE AND TRUE
              GROUP BY created_date_day
            ) AS data_table ON data_table.created_date_day = date_series.created_date_day
          )) AS total_users_data
        ) AS total_users_data
      ) AS total_users_data
      WHERE (
        (
          (total_users_data__created_date >= '2024-05-04T10:30:00.000Z')
          AND (total_users_data__created_date <= '2024-06-03T11:29:59.999Z')
        )
      )
      GROUP BY total_users_data__created_date, total_users_data__cumulative_user_count
      ORDER BY total_users_data__created_date ASC
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    // Only columns which can be figured out without the original table schema are expected
    expect(references['system.dim_revu_slim'].sort()).toEqual(
      ['created_date', 'is_deleted'].sort()
    );

    // Only columns which can be figured out without the original table schema are expected
    expect(references['system.dim_revo'].sort()).toEqual(
      ['id', 'is_deleted'].sort()
    );
  });

  it('60. Should return the correct top 5 rev_uids from summary_grow_users_event_count table', async () => {
    const sql = `
      SELECT SUM(total_events) AS summary_grow_users_event_count__total_usage_events,
             summary_grow_users_event_count__rev_uid
      FROM (
        SELECT *,
               created_date AS summary_grow_users_event_count__created_date,
               rev_uid AS summary_grow_users_event_count__rev_uid
        FROM (
          SELECT summary_grow_users_event_count.created_at AS created_date,
                 summary_grow_users_event_count.total_events AS total_events,
                 revu.id AS rev_uid,
                 CASE WHEN revu.is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
                 (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND summary_grow_users_event_count.dev_oid = dim_revo.dev_oid AND summary_grow_users_event_count.rev_oid = dim_revo.display_id) AS rev_oid
          FROM system.summary_grow_users_event_count
          INNER JOIN (
            SELECT *,
                   'REVU-' || REVERSE(SPLIT_PART(REVERSE(id), '/', 1)) AS display_id
            FROM system.dim_revu_slim
            WHERE is_deleted = FALSE
          ) AS revu ON summary_grow_users_event_count.rev_uid = revu.display_id
                      AND summary_grow_users_event_count.rev_oid = revu.rev_oid
                      AND summary_grow_users_event_count.dev_oid = revu.dev_oid
        ) AS summary_grow_users_event_count
      ) AS summary_grow_users_event_count
      WHERE (
        (
          (summary_grow_users_event_count__created_date >= '2024-05-04T10:30:00.000Z')
          AND (summary_grow_users_event_count__created_date <= '2024-06-03T11:29:59.999Z')
        )
      )
      GROUP BY summary_grow_users_event_count__rev_uid
      ORDER BY summary_grow_users_event_count__total_usage_events DESC
      LIMIT 5
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    // Only columns which can be figured out without the original table schema are expected
    expect(references['system.summary_grow_users_event_count'].sort()).toEqual(
      ['created_at', 'total_events', 'rev_uid', 'dev_oid', 'rev_oid'].sort()
    );

    // Only columns which can be figured out without the original table schema are expected
    expect(references['system.dim_revu_slim'].sort()).toEqual(
      ['id', 'is_deleted'].sort()
    );

    // Only columns which can be figured out without the original table schema are expected
    expect(references['system.dim_revo'].sort()).toEqual(
      ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
    );
  });

  // it('61. Should return the correct count of rev_uids from summary_grow_users_event_count table', async () => {
  //   const sql = `
  //     SELECT COUNT(*) AS count
  //     FROM (
  //       SELECT SUM(total_events) AS summary_grow_users_event_count__total_usage_events,
  //              summary_grow_users_event_count__rev_uid
  //       FROM (
  //         SELECT *,
  //                created_date AS summary_grow_users_event_count__created_date,
  //                rev_uid AS summary_grow_users_event_count__rev_uid
  //         FROM (
  //           SELECT summary_grow_users_event_count.created_at AS created_date,
  //                  summary_grow_users_event_count.total_events AS total_events,
  //                  revu.id AS rev_uid,
  //                  CASE WHEN revu.is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
  //                  (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND summary_grow_users_event_count.dev_oid = dim_revo.dev_oid AND summary_grow_users_event_count.rev_oid = dim_revo.display_id) AS rev_oid
  //           FROM system.summary_grow_users_event_count
  //           INNER JOIN (
  //             SELECT *,
  //                    'REVU-' || REVERSE(SPLIT_PART(REVERSE(id), '/', 1)) AS display_id
  //             FROM system.dim_revu_slim
  //             WHERE is_deleted = FALSE
  //           ) AS revu ON summary_grow_users_event_count.rev_uid = revu.display_id
  //                       AND summary_grow_users_event_count.rev_oid = revu.rev_oid
  //                       AND summary_grow_users_event_count.dev_oid = revu.dev_oid
  //         ) AS summary_grow_users_event_count
  //       ) AS summary_grow_users_event_count
  //       WHERE (
  //         (
  //           (summary_grow_users_event_count__created_date >= '2024-05-04T10:30:00.000Z')
  //           AND (summary_grow_users_event_count__created_date <= '2024-06-03T11:29:59.999Z')
  //         )
  //       )
  //       GROUP BY summary_grow_users_event_count__rev_uid
  //       ORDER BY summary_grow_users_event_count__total_usage_events DESC
  //     )
  //   `;

  //   const references = await sqlQueryToAST(sql);

  //

  //   // Only columns which can be figured out without the original table schema are expected
  //   expect(references['system.summary_grow_users_event_count'].sort()).toEqual(
  //     ['created_at', 'total_events', 'rev_uid', 'dev_oid', 'rev_oid'].sort()
  //   );

  //   // Only columns which can be figured out without the original table schema are expected
  //   expect(references['system.dim_revu_slim'].sort()).toEqual(
  //     ['id', 'is_deleted', 'rev_oid', 'dev_oid', 'is_verified'].sort()
  //   );

  //   // // Only columns which can be figured out without the original table schema are expected
  //   expect(references['system.dim_revo'].sort()).toEqual(
  //     ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
  //   );
  // });

  // it('62. Should return the bottom 5 rev_uids with total usage events from summary_grow_users_event_count table', async () => {
  //   const sql = `
  //     SELECT SUM(total_events) AS summary_grow_users_event_count__total_usage_events,
  //            summary_grow_users_event_count__rev_uid
  //     FROM (
  //       SELECT *,
  //              created_date AS summary_grow_users_event_count__created_date,
  //              rev_uid AS summary_grow_users_event_count__rev_uid
  //       FROM (
  //         SELECT summary_grow_users_event_count.created_at AS created_date,
  //                summary_grow_users_event_count.total_events AS total_events,
  //                revu.id AS rev_uid,
  //                CASE WHEN revu.is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
  //                (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND summary_grow_users_event_count.dev_oid = dim_revo.dev_oid AND summary_grow_users_event_count.rev_oid = dim_revo.display_id) AS rev_oid
  //         FROM system.summary_grow_users_event_count
  //         INNER JOIN (
  //           SELECT *,
  //                  'REVU-' || REVERSE(SPLIT_PART(REVERSE(id), '/', 1)) AS display_id
  //           FROM system.dim_revu_slim
  //           WHERE is_deleted = FALSE
  //         ) AS revu ON summary_grow_users_event_count.rev_uid = revu.display_id
  //                     AND summary_grow_users_event_count.rev_oid = revu.rev_oid
  //                     AND summary_grow_users_event_count.dev_oid = revu.dev_oid
  //       ) AS summary_grow_users_event_count
  //     ) AS summary_grow_users_event_count
  //     WHERE (
  //       (
  //         (summary_grow_users_event_count__created_date >= '2024-05-04T10:30:00.000Z')
  //         AND (summary_grow_users_event_count__created_date <= '2024-06-03T11:29:59.999Z')
  //       )
  //     )
  //     GROUP BY summary_grow_users_event_count__rev_uid
  //     ORDER BY summary_grow_users_event_count__total_usage_events ASC
  //     LIMIT 5
  //   `;

  //   const references = await sqlQueryToAST(sql);

  //

  //   expect(references['system.summary_grow_users_event_count'].sort()).toEqual(
  //     ['created_at', 'total_events', 'rev_uid', 'rev_oid', 'dev_oid'].sort()
  //   );

  //   expect(references['system.dim_revu_slim'].sort()).toEqual(
  //     ['id', 'rev_oid', 'dev_oid', 'is_verified', 'is_deleted'].sort()
  //   );

  //   expect(references['system.dim_revo'].sort()).toEqual(
  //     ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
  //   );
  // });

  // it('63. Should return the correct count of rev_uids from summary_grow_users_event_count table', async () => {
  //   const sql = `
  //     SELECT COUNT(*) AS count
  //     FROM (
  //       SELECT SUM(total_events) AS summary_grow_users_event_count__total_usage_events,
  //              summary_grow_users_event_count__rev_uid
  //       FROM (
  //         SELECT *,
  //                created_date AS summary_grow_users_event_count__created_date,
  //                rev_uid AS summary_grow_users_event_count__rev_uid
  //         FROM (
  //           SELECT summary_grow_users_event_count.created_at AS created_date,
  //                  summary_grow_users_event_count.total_events AS total_events,
  //                  revu.id AS rev_uid,
  //                  CASE WHEN revu.is_verified = true THEN 'Yes' ELSE 'No' END AS verified_enum,
  //                  (SELECT id FROM system.dim_revo WHERE is_deleted = FALSE AND summary_grow_users_event_count.dev_oid = dim_revo.dev_oid AND summary_grow_users_event_count.rev_oid = dim_revo.display_id) AS rev_oid
  //           FROM system.summary_grow_users_event_count
  //           INNER JOIN (
  //             SELECT *,
  //                    'REVU-' || REVERSE(SPLIT_PART(REVERSE(id), '/', 1)) AS display_id
  //             FROM system.dim_revu_slim
  //             WHERE is_deleted = FALSE
  //           ) AS revu ON summary_grow_users_event_count.rev_uid = revu.display_id
  //                       AND summary_grow_users_event_count.rev_oid = revu.rev_oid
  //                       AND summary_grow_users_event_count.dev_oid = revu.dev_oid
  //         ) AS summary_grow_users_event_count
  //       ) AS summary_grow_users_event_count
  //       WHERE (
  //         (
  //           (summary_grow_users_event_count__created_date >= '2024-05-04T10:30:00.000Z')
  //           AND (summary_grow_users_event_count__created_date <= '2024-06-03T11:29:59.999Z')
  //         )
  //       )
  //       GROUP BY summary_grow_users_event_count__rev_uid
  //       ORDER BY summary_grow_users_event_count__total_usage_events ASC
  //     )
  //   `;

  //   const references = await sqlQueryToAST(sql);

  //

  //   expect(references['system.summary_grow_users_event_count'].sort()).toEqual(
  //     ['created_at', 'total_events', 'rev_uid', 'rev_oid', 'dev_oid'].sort()
  //   );

  //   expect(references['system.dim_revu_slim'].sort()).toEqual(
  //     ['id', 'rev_oid', 'dev_oid', 'is_verified', 'is_deleted'].sort()
  //   );

  //   expect(references['system.dim_revo'].sort()).toEqual(
  //     ['id', 'is_deleted', 'dev_oid', 'display_id'].sort()
  //   );
  // });

  it('64. Should return the average customer health scores daily summary for accounts with forecast category "Closed Won"', async () => {
    const sql = `
      SELECT
        avg(curr_score_value) * 100 AS customer_health_scores_daily_summary__curr_score_value,
        avg(prev_score_value) * 100 AS customer_health_scores_daily_summary__prev_score_value,
        avg(delta_percent) AS customer_health_scores_daily_summary__delta_percent,
        customer_health_scores_daily_summary__account_id,
        customer_health_scores_daily_summary__curr_category,
        customer_health_scores_daily_summary__prev_category,
        customer_health_scores_daily_summary__transition_end_date
      FROM (
        SELECT
          *,
          forecast_category AS customer_health_scores_daily_summary__forecast_category,
          account_id AS customer_health_scores_daily_summary__account_id,
          curr_category AS customer_health_scores_daily_summary__curr_category,
          prev_category AS customer_health_scores_daily_summary__prev_category,
          transition_end_date AS customer_health_scores_daily_summary__transition_end_date
        FROM (
          WITH ranked_scores AS (
            SELECT
              score_id,
              metric_set_id,
              account_id,
              score_value,
              category,
              record_date,
              LAG(category) OVER (PARTITION BY score_id, metric_set_id, account_id ORDER BY record_date) AS prev_category,
              LAG(score_value) OVER (PARTITION BY score_id, metric_set_id, account_id ORDER BY record_date) AS prev_score_value,
              LAG(record_date) OVER (PARTITION BY score_id, metric_set_id, account_id ORDER BY record_date) AS prev_record_date
            FROM system.customer_health_scores_daily_summary
            WHERE score_id = 'don:core:dvrv-us-1:devo/0:score/l1VEjGTl' AND TRUE
          ),
          transitions AS (
            SELECT
              score_id,
              metric_set_id,
              account_id,
              record_date,
              prev_category,
              category AS curr_category,
              prev_score_value,
              score_value AS curr_score_value,
              prev_record_date AS transition_start_date,
              record_date AS transition_end_date
            FROM ranked_scores
            WHERE prev_category IS NOT NULL AND curr_category != prev_category
          ),
          final_results AS (
            SELECT
              distinct on (account_id)
              account_id,
              record_date,
              prev_category,
              curr_category,
              prev_score_value,
              curr_score_value,
              ROUND(((curr_score_value - prev_score_value) / NULLIF(ABS(prev_score_value), 0)) * 100, 2) AS delta_percent,
              transition_start_date,
              transition_end_date
            FROM transitions
            ORDER BY record_date desc
          )
          SELECT
            final_results.*,
            dim_account.tier,
            dim_account.owned_by,
            json_extract_string(dim_account.custom_fields, '$.tnt__forecast_category') AS forecast_category
          FROM final_results
          JOIN system.dim_account ON dim_account.id = final_results.account_id
        ) AS customer_health_scores_daily_summary
      ) AS customer_health_scores_daily_summary
      WHERE (customer_health_scores_daily_summary__forecast_category = 'Closed Won')
      GROUP BY
        customer_health_scores_daily_summary__account_id,
        customer_health_scores_daily_summary__curr_category,
        customer_health_scores_daily_summary__prev_category,
        customer_health_scores_daily_summary__transition_end_date
      ORDER BY customer_health_scores_daily_summary__transition_end_date DESC
      LIMIT 5
    `;

    const references = await getDatasetColumnsFromSQL(sql);

    expect(
      references['system.customer_health_scores_daily_summary'].sort()
    ).toEqual(
      [
        'account_id',
        'category',
        'metric_set_id',
        'record_date',
        'score_id',
        'score_value',
      ].sort()
    );

    expect(references['system.dim_account'].sort()).toEqual(
      ['custom_fields', 'id', 'owned_by', 'tier'].sort()
    );
  });
});
