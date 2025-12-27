export interface MeerkatQueryOptions {
    /**
     * When true, all column aliases use quoted dot delimiter ("orders.field")
     * instead of underscore format (orders__field).
     *
     * Affects all query layers for consistency.
     */
    isDotDelimiterEnabled: boolean;
}
