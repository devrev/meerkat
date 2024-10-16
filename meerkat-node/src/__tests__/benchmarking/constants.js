


const getOldQueries = (
    tableName
) => {
    return [
        {
            name: "Simple COUNT",
            sql: `
            SELECT COUNT(*) FROM (
                SELECT * FROM (
                    SELECT * FROM ${tableName}
                )
            )`
        },
        {
            name: "Aggregation with GROUP BY",
            sql: `
            SELECT ${tableName}__customer_id, SUM(total_amount) AS total_spent FROM (
                SELECT *, customer_id as ${tableName}__customer_id FROM (
                    SELECT * FROM ${tableName}
                )
            ) GROUP BY ${tableName}__customer_id ORDER BY total_spent DESC`
        },
        {
            name: "Multiple Aggregations with GROUP BY",
            sql: `SELECT product_id, product_name, SUM(quantity) AS total_quantity, AVG(product_price) AS avg_price FROM (
                SELECT *, product_id AS ${tableName}__product_id FROM (
                    SELECT * FROM ${tableName}
                )
            ) GROUP BY product_id, product_name ORDER BY total_quantity DESC`
        },
        {
            name: "Date Truncation and Aggregation",
            sql: `
            SELECT ${tableName}__month, SUM(total_amount) AS monthly_revenue FROM (
                SELECT *, DATE_TRUNC('month', order_date) AS ${tableName}__month FROM (
                    SELECT * FROM ${tableName}
                )
            ) GROUP BY ${tableName}__month ORDER BY ${tableName}__month`
        },
        {
            name: "Window Function",
            sql: `
                SELECT 
                    ${tableName}__order_id, 
                    ${tableName}__order_date, 
                    ${tableName}__total_amount,
                    SUM(total_amount) OVER (ORDER BY order_date) AS ${tableName}__cumulative_revenue
                FROM (
                    SELECT *, order_id AS ${tableName}__order_id, order_date AS ${tableName}__order_date, total_amount AS ${tableName}__total_amount FROM (
                        SELECT * FROM ${tableName}
                    )
                )
                ORDER BY ${tableName}__order_date
                LIMIT 100
            `
        }, {
            name: 'filters',
            sql: `
            SELECT 
                DATE_TRUNC('month', ${tableName}.order_date) AS ${tableName}__order_month,
                ${tableName}__customer_id,
                ${tableName}__product_id,
                ${tableName}__product_name,
                COUNT(DISTINCT ${tableName}.order_id) AS ${tableName}__total_orders,
                SUM(${tableName}.quantity) AS ${tableName}__quantity,
                AVG(${tableName}.product_price) AS ${tableName}__avg_product_price,
            FROM (
                select
                    *,
                    ${tableName}.customer_id AS ${tableName}__customer_id,
                    ${tableName}.product_id AS ${tableName}__product_id,
                    ${tableName}.product_name AS ${tableName}__product_name,
                    ${tableName}.order_date AS ${tableName}__order_date,
                from (
                    select * from ${tableName}
                ) AS ${tableName}
            ) AS ${tableName}
            WHERE 
                ${tableName}__order_date BETWEEN '2023-01-01 00:00:00' AND '2023-06-30 23:59:59'
                AND ${tableName}__product_id BETWEEN 100 AND 200
                AND ${tableName}__product_name LIKE '%Premium%'
            GROUP BY 
                ${tableName}__order_month,
                ${tableName}__customer_id,
                ${tableName}__product_id,
                ${tableName}__product_name
            HAVING 
                ${tableName}__total_orders > 10
            ORDER BY 
                ${tableName}__total_orders DESC
            `
        }
       
    ];
}

const getNewQueries = (tableName: string) => {
    return [
        {
            name: "Simple COUNT",
            sql: `SELECT COUNT(*) FROM ${tableName}`
        },
        {
            name: "Aggregation with GROUP BY",
            sql: `SELECT customer_id, SUM(total_amount) AS total_spent FROM ${tableName} GROUP BY customer_id ORDER BY total_spent DESC`
        },
        {
            name: "Multiple Aggregations with GROUP BY",
            sql: `SELECT product_id, product_name, SUM(quantity) AS total_quantity, AVG(product_price) AS avg_price FROM ${tableName} GROUP BY product_id, product_name ORDER BY total_quantity DESC`
        },
        {
            name: "Date Truncation and Aggregation",
            sql: `SELECT DATE_TRUNC('month', order_date) AS month, SUM(total_amount) AS monthly_revenue FROM ${tableName} GROUP BY month ORDER BY month`
        },
        {
            name: "Window Function",
            sql: `
                SELECT 
                    order_id, 
                    order_date, 
                    total_amount,
                    SUM(total_amount) OVER (ORDER BY order_date) AS cumulative_revenue
                FROM ${tableName}
                ORDER BY order_date
                LIMIT 100
            `
        },
        {
            name: 'filters',
            sql: `
                SELECT 
                DATE_TRUNC('month', ${tableName}.order_date),
                ${tableName}.customer_id,
                ${tableName}.product_id,
                ${tableName}.product_name,
                COUNT(DISTINCT ${tableName}.order_id) AS ${tableName}__total_orders,
                SUM(${tableName}.quantity) AS ${tableName}__quantity,
                AVG(${tableName}.product_price) AS ${tableName}__avg_product_price,
            FROM ${tableName}
            WHERE 
                DATE_TRUNC('month', ${tableName}.order_date) BETWEEN '2023-01-01 00:00:00' AND '2023-06-30 23:59:59'
                AND ${tableName}.product_id BETWEEN 100 AND 200
                AND ${tableName}.product_name LIKE '%Premium%'
            GROUP BY 
                DATE_TRUNC('month', ${tableName}.order_date),
                ${tableName}.customer_id,
                ${tableName}.product_id,
                ${tableName}.product_name
            HAVING 
                ${tableName}__total_orders > 10
            ORDER BY 
                ${tableName}__total_orders DESC
            `
        }
    ];
}



module.exports = {
    PARQUET_FILE_PATH: '/Users/zaidjan/Documents/Projects/meerkat/meerkat-node/src/__tests__/benchmarking/benchmarking.parquet',
    getOldQueries,
    getNewQueries
}




