/**
 * TableSchema Definitions for Synthetic Test Tables
 * 
 * These schemas match the synthetic tables created in schema-setup.ts
 * and follow the same conventions as production Meerkat schemas.
 */

export const FACT_ALL_TYPES_SCHEMA = {
  name: 'fact_all_types',
  sql: 'SELECT * FROM fact_all_types',
  
  measures: [
    // Numeric measures
    { name: 'metric_bigint', sql: 'metric_bigint', type: 'number' },
    { name: 'small_bigint', sql: 'small_bigint', type: 'number' },
    { name: 'metric_numeric', sql: 'metric_numeric', type: 'number' },
    { name: 'precise_numeric', sql: 'precise_numeric', type: 'number' },
    { name: 'metric_double', sql: 'metric_double', type: 'number' },
    { name: 'metric_float', sql: 'metric_float', type: 'number' },
    { name: 'nullable_int', sql: 'nullable_int', type: 'number' },
    { name: 'mtti_seconds', sql: 'mtti_seconds', type: 'number' },
    { name: 'severity_id_int', sql: 'severity_id_int', type: 'number' },
    
    // Aggregate measures
    { name: 'count', sql: 'COUNT(*)', type: 'number' },
    { name: 'sum_metric_bigint', sql: 'SUM(metric_bigint)', type: 'number' },
    { name: 'avg_metric_double', sql: 'AVG(metric_double)', type: 'number' },
    { name: 'min_metric_numeric', sql: 'MIN(metric_numeric)', type: 'number' },
    { name: 'max_metric_numeric', sql: 'MAX(metric_numeric)', type: 'number' },
    { name: 'count_distinct_user_id', sql: 'COUNT(DISTINCT user_id)', type: 'number' },
  ],
  
  dimensions: [
    // ID fields
    { name: 'id_bigint', sql: 'id_bigint', type: 'number' },
    { name: 'incident_id', sql: 'incident_id', type: 'string' },
    { name: 'user_id', sql: 'user_id', type: 'string' },
    { name: 'part_id', sql: 'part_id', type: 'string' },
    
    // Boolean dimensions
    { name: 'is_deleted', sql: 'is_deleted', type: 'boolean' },
    { name: 'flag_boolean', sql: 'flag_boolean', type: 'boolean' },
    { name: 'is_active', sql: 'is_active', type: 'boolean' },
    
    // String/enum dimensions
    { name: 'priority', sql: 'priority', type: 'string' },
    { name: 'status', sql: 'status', type: 'string' },
    { name: 'severity_label', sql: 'severity_label', type: 'string' },
    { name: 'environment', sql: 'environment', type: 'string' },
    { name: 'title', sql: 'title', type: 'string' },
    { name: 'description', sql: 'description', type: 'string' },
    { name: 'nullable_string', sql: 'nullable_string', type: 'string' },
    { name: 'edge_case_string', sql: 'edge_case_string', type: 'string' },
    
    // Date dimensions
    { name: 'record_date', sql: 'record_date', type: 'time' },
    { name: 'created_date', sql: 'created_date', type: 'time' },
    { name: 'mitigated_date', sql: 'mitigated_date', type: 'time' },
    { name: 'partition_record_date', sql: 'partition_record_date', type: 'time' },
    { name: 'nullable_date', sql: 'nullable_date', type: 'time' },
    
    // Timestamp dimensions
    { name: 'created_timestamp', sql: 'created_timestamp', type: 'time' },
    { name: 'identified_timestamp', sql: 'identified_timestamp', type: 'time' },
    { name: 'deployment_time', sql: 'deployment_time', type: 'time' },
    { name: 'partition_record_ts', sql: 'partition_record_ts', type: 'time' },
    
    // Array dimensions
    { name: 'tags', sql: 'tags', type: 'string_array' },
    { name: 'owned_by_ids', sql: 'owned_by_ids', type: 'string_array' },
    { name: 'part_ids', sql: 'part_ids', type: 'string_array' },
    
    // JSON-derived dimensions (stored as VARCHAR)
    { name: 'metadata_json', sql: 'metadata_json', type: 'string' },
    { name: 'stage_json', sql: 'stage_json', type: 'string' },
    { name: 'impact_json', sql: 'impact_json', type: 'string' },
    
    // Derived dimensions
    { name: 'created_month', sql: 'created_month', type: 'string' },
    
    // JSON extracted dimensions (examples of how to extract JSON fields)
    { 
      name: 'severity_id_from_json', 
      sql: "CAST(json_extract_path_text(metadata_json, 'severity_id') AS INTEGER)", 
      type: 'number' 
    },
    { 
      name: 'impact_from_json', 
      sql: "json_extract_path_text(metadata_json, 'impact')", 
      type: 'string' 
    },
  ],
};

export const DIM_USER_SCHEMA = {
  name: 'dim_user',
  sql: 'SELECT * FROM dim_user',
  
  measures: [
    { name: 'count_users', sql: 'COUNT(*)', type: 'number' },
    { name: 'count_distinct_users', sql: 'COUNT(DISTINCT user_id)', type: 'number' },
  ],
  
  dimensions: [
    { name: 'user_id', sql: 'user_id', type: 'string' },
    { name: 'user_name', sql: 'user_name', type: 'string' },
    { name: 'user_email', sql: 'user_email', type: 'string' },
    { name: 'user_segment', sql: 'user_segment', type: 'string' },
    { name: 'user_department', sql: 'user_department', type: 'string' },
    { name: 'user_created_date', sql: 'user_created_date', type: 'time' },
    { name: 'is_active_user', sql: 'is_active_user', type: 'boolean' },
  ],
  
  joins: [
    {
      name: 'fact_all_types',
      relationship: 'belongsTo',
      sql: 'fact_all_types.user_id = dim_user.user_id',
    },
  ],
};

export const DIM_PART_SCHEMA = {
  name: 'dim_part',
  sql: 'SELECT * FROM dim_part',
  
  measures: [
    { name: 'count_parts', sql: 'COUNT(*)', type: 'number' },
    { name: 'avg_weight', sql: 'AVG(weight)', type: 'number' },
    { name: 'avg_price', sql: 'AVG(price)', type: 'number' },
    { name: 'sum_weight', sql: 'SUM(weight)', type: 'number' },
  ],
  
  dimensions: [
    { name: 'part_id', sql: 'part_id', type: 'string' },
    { name: 'part_name', sql: 'part_name', type: 'string' },
    { name: 'product_category', sql: 'product_category', type: 'string' },
    { name: 'product_tier', sql: 'product_tier', type: 'string' },
    { name: 'weight', sql: 'weight', type: 'number' },
    { name: 'price', sql: 'price', type: 'number' },
    { name: 'in_stock', sql: 'in_stock', type: 'boolean' },
  ],
  
  joins: [
    {
      name: 'fact_all_types',
      relationship: 'belongsTo',
      sql: 'fact_all_types.part_id = dim_part.part_id',
    },
  ],
};

/**
 * Helper to get all schemas as an array
 */
export function getAllSchemas() {
  return [FACT_ALL_TYPES_SCHEMA, DIM_USER_SCHEMA, DIM_PART_SCHEMA];
}

/**
 * Helper to get just the fact schema
 */
export function getFactSchema() {
  return FACT_ALL_TYPES_SCHEMA;
}

/**
 * Helper to get fact + one dimension
 */
export function getFactWithUserSchema() {
  return [FACT_ALL_TYPES_SCHEMA, DIM_USER_SCHEMA];
}

export function getFactWithPartSchema() {
  return [FACT_ALL_TYPES_SCHEMA, DIM_PART_SCHEMA];
}

