export interface DurationEvents {
  event_name:
    | 'query_execution_duration'
    | 'mount_file_buffer_duration'
    | 'unmount_file_buffer_duration'
    | 'query_queue_duration'
    | 'json_to_buffer_conversion_duration'
    | 'clone_buffer_duration';
  duration: number;
}

export interface QueueEvents {
  event_name: 'query_queue_length';
  value: number;
}

export type DBMEvent = (DurationEvents | QueueEvents) & { metadata?: object };
