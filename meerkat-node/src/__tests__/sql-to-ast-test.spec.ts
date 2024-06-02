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
});
