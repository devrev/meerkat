/**
 * Synthetic Schema Setup
 * 
 * Creates comprehensive test tables with 1M+ rows covering all data types
 * and patterns used in production Meerkat queries.
 */

import { duckdbExec } from '../../duckdb-exec';

/**
 * Create the comprehensive fact_all_types table
 * This table contains ~100 columns covering all normalized DB types
 */
export async function createFactAllTypesTable(): Promise<void> {
  console.log('Creating fact_all_types table with 1M rows...');
  
  await duckdbExec(`
    CREATE TABLE IF NOT EXISTS fact_all_types AS
    SELECT
      -- Keys/IDs
      i AS id_bigint,
      'inc_' || (i % 100000) AS incident_id,
      'user_' || (i % 10000) AS user_id,
      'part_' || (i % 5000) AS part_id,
      
      -- Numeric types: BIGINT
      CAST(i * 10 AS BIGINT) AS metric_bigint,
      CAST((i % 1000) AS BIGINT) AS small_bigint,
      
      -- Numeric types: NUMERIC/DECIMAL
      CAST((i % 1000) / 10.0 AS DECIMAL(18,2)) AS metric_numeric,
      CAST((i % 10000) / 100.0 AS DECIMAL(18,4)) AS precise_numeric,
      
      -- Numeric types: DOUBLE/FLOAT
      (i % 1000) / 3.0 AS metric_double,
      (i % 1000) * 1.5 AS metric_float,
      
      -- Boolean
      (i % 10 = 0) AS is_deleted,
      (i % 3 = 0) AS flag_boolean,
      (i % 2 = 0) AS is_active,
      
      -- Strings/enums (VARCHAR)
      CASE (i % 5)
        WHEN 0 THEN 'high'
        WHEN 1 THEN 'medium'
        WHEN 2 THEN 'low'
        WHEN 3 THEN 'critical'
        ELSE 'unknown'
      END AS priority,
      
      CASE (i % 4)
        WHEN 0 THEN 'open'
        WHEN 1 THEN 'in_progress'
        WHEN 2 THEN 'resolved'
        ELSE 'closed'
      END AS status,
      
      CASE (i % 3)
        WHEN 0 THEN 'P0'
        WHEN 1 THEN 'P1'
        ELSE 'P2'
      END AS severity_label,
      
      CASE (i % 4)
        WHEN 0 THEN 'production'
        WHEN 1 THEN 'staging'
        WHEN 2 THEN 'development'
        ELSE 'test'
      END AS environment,
      
      'Title ' || i AS title,
      'Description for incident ' || i AS description,
      
      -- Dates (cast to INTEGER for date arithmetic)
      -- Using % 1460 for ~4 year cycle, % 366 for full year including leap day
      DATE '2020-01-01' + CAST((i % 1460) AS INTEGER) AS record_date,
      DATE '2020-01-01' + CAST((i % 366) AS INTEGER) AS created_date,
      DATE '2020-01-01' + CAST(((i + 30) % 1460) AS INTEGER) AS mitigated_date,
      DATE '2019-01-01' + CAST((i % 730) AS INTEGER) AS partition_record_date,
      
      -- Timestamps
      TIMESTAMP '2020-01-01 00:00:00' + INTERVAL (CAST((i % 1460) AS INTEGER)) DAY + INTERVAL (CAST((i % 86400) AS INTEGER)) SECOND AS created_timestamp,
      TIMESTAMP '2020-01-01 00:00:00' + INTERVAL (CAST(((i + 100) % 1460) AS INTEGER)) DAY AS identified_timestamp,
      TIMESTAMP '2020-01-01 00:00:00' + INTERVAL (CAST((i % 366) AS INTEGER)) DAY AS deployment_time,
      TIMESTAMP '2020-01-01 00:00:00' + INTERVAL (CAST((i % 1460) AS INTEGER)) DAY AS partition_record_ts,
      
      -- Arrays (VARCHAR[])
      CASE 
        WHEN i % 4 = 0 THEN ARRAY['tag1', 'tag2', 'tag3']
        WHEN i % 4 = 1 THEN ARRAY['tag1']
        WHEN i % 4 = 2 THEN ARRAY['tag2', 'tag4']
        ELSE ARRAY[]::VARCHAR[]
      END AS tags,
      
      CASE
        WHEN i % 3 = 0 THEN ARRAY['user_' || ((i % 1000) + 1), 'user_' || ((i % 1000) + 2)]
        WHEN i % 3 = 1 THEN ARRAY['user_' || (i % 1000)]
        ELSE ARRAY[]::VARCHAR[]
      END AS owned_by_ids,
      
      CASE
        WHEN i % 5 = 0 THEN ARRAY['part_' || (i % 500), 'part_' || ((i % 500) + 1), 'part_' || ((i % 500) + 2)]
        WHEN i % 5 = 1 THEN ARRAY['part_' || (i % 500)]
        ELSE ARRAY[]::VARCHAR[]
      END AS part_ids,
      
      -- JSON-like VARCHAR (to test JSON extraction)
      '{"severity_id": ' || (i % 5) || ', "impact": "' || 
        CASE (i % 3) 
          WHEN 0 THEN 'high' 
          WHEN 1 THEN 'medium' 
          ELSE 'low' 
        END || 
        '", "reported_by": "' || 'user_' || (i % 100) || '"}' AS metadata_json,
      
      '{"stage": "' || 
        CASE (i % 4)
          WHEN 0 THEN 'investigation'
          WHEN 1 THEN 'mitigation'
          WHEN 2 THEN 'resolution'
          ELSE 'closed'
        END || '"}' AS stage_json,
      
      '{"customers": [' || 
        '"customer_' || (i % 1000) || '", ' ||
        '"customer_' || ((i % 1000) + 1) || '"' ||
      ']}' AS impact_json,
      
      -- Derived-style fields (mirroring real widget expressions)
      (i % 10000) AS mtti_seconds,
      MONTHNAME(DATE '2020-01-01' + CAST((i % 365) AS INTEGER)) AS created_month,
      (i % 5) AS severity_id_int,
      
      -- Special values for NULL testing
      CASE WHEN i % 10 = 0 THEN NULL ELSE 'value_' || i END AS nullable_string,
      CASE WHEN i % 15 = 0 THEN NULL ELSE (i % 1000) END AS nullable_int,
      CASE WHEN i % 20 = 0 THEN NULL ELSE DATE '2020-01-01' + CAST((i % 365) AS INTEGER) END AS nullable_date,
      
      -- Edge case values
      CASE
        WHEN i % 100 = 0 THEN 'value with "quotes"'
        WHEN i % 100 = 1 THEN 'value with ''apostrophe'''
        WHEN i % 100 = 2 THEN 'value with \\ backslash'
        WHEN i % 100 = 3 THEN ''
        ELSE 'normal_value_' || i
      END AS edge_case_string
      
    FROM range(0, 1000000) AS t(i)
  `);
  
  console.log('✅ Created fact_all_types with 1M rows');
}

/**
 * Create dimension table: dim_user
 */
export async function createDimUserTable(): Promise<void> {
  console.log('Creating dim_user table...');
  
  await duckdbExec(`
    CREATE TABLE IF NOT EXISTS dim_user AS
    SELECT
      'user_' || i AS user_id,
      'User ' || i AS user_name,
      'user' || i || '@example.com' AS user_email,
      CASE (i % 3)
        WHEN 0 THEN 'enterprise'
        WHEN 1 THEN 'pro'
        ELSE 'free'
      END AS user_segment,
      CASE (i % 4)
        WHEN 0 THEN 'engineering'
        WHEN 1 THEN 'product'
        WHEN 2 THEN 'support'
        ELSE 'sales'
      END AS user_department,
      DATE '2019-01-01' + CAST((i % 1095) AS INTEGER) AS user_created_date,
      (i % 2 = 0) AS is_active_user
    FROM range(0, 10000) AS t(i)
  `);
  
  console.log('✅ Created dim_user with 10K rows');
}

/**
 * Create dimension table: dim_part
 */
export async function createDimPartTable(): Promise<void> {
  console.log('Creating dim_part table...');
  
  await duckdbExec(`
    CREATE TABLE IF NOT EXISTS dim_part AS
    SELECT
      'part_' || i AS part_id,
      'Part ' || i AS part_name,
      CASE (i % 5)
        WHEN 0 THEN 'electronics'
        WHEN 1 THEN 'furniture'
        WHEN 2 THEN 'clothing'
        WHEN 3 THEN 'food'
        ELSE 'other'
      END AS product_category,
      CASE (i % 3)
        WHEN 0 THEN 'premium'
        WHEN 1 THEN 'standard'
        ELSE 'budget'
      END AS product_tier,
      (i % 1000) / 10.0 AS weight,
      (i % 10000) / 100.0 AS price,
      (i % 2 = 0) AS in_stock
    FROM range(0, 5000) AS t(i)
  `);
  
  console.log('✅ Created dim_part with 5K rows');
}

/**
 * Create all synthetic tables in one go
 */
export async function createAllSyntheticTables(): Promise<void> {
  const startTime = Date.now();
  
  await createFactAllTypesTable();
  await createDimUserTable();
  await createDimPartTable();
  
  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`\n✅ All synthetic tables created in ${duration}s`);
}

/**
 * Drop all synthetic tables (for cleanup)
 */
export async function dropSyntheticTables(): Promise<void> {
  console.log('Dropping synthetic tables...');
  
  await duckdbExec('DROP TABLE IF EXISTS fact_all_types');
  await duckdbExec('DROP TABLE IF EXISTS dim_user');
  await duckdbExec('DROP TABLE IF EXISTS dim_part');
  
  console.log('✅ Synthetic tables dropped');
}

/**
 * Verify tables exist and have data
 */
export async function verifySyntheticTables(): Promise<void> {
  const factCount = await duckdbExec('SELECT COUNT(*) as count FROM fact_all_types');
  const userCount = await duckdbExec('SELECT COUNT(*) as count FROM dim_user');
  const partCount = await duckdbExec('SELECT COUNT(*) as count FROM dim_part');
  
  console.log('\n📊 Synthetic Table Verification:');
  console.log(`   fact_all_types: ${factCount[0].count} rows`);
  console.log(`   dim_user: ${userCount[0].count} rows`);
  console.log(`   dim_part: ${partCount[0].count} rows`);
}

