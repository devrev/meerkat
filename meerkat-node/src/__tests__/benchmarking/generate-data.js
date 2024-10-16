const parquet = require('parquetjs');
const { PARQUET_FILE_PATH } = require('./constants');

function randomDate(start, end) {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function generateAdditionalColumns(baseSchema, additionalColumnCount) {
    const dataTypes = ['INT64', 'DOUBLE', 'BOOLEAN', 'UTF8', 'TIMESTAMP_MILLIS'];
    const prefixes = ['extra', 'aux', 'meta', 'sup', 'add'];

    for (let i = 0; i < additionalColumnCount; i++) {
        const prefix = prefixes[Math.floor(Math.random() * prefixes.length)];
        const columnName = `${prefix}_${i + 1}`;
        const dataType = dataTypes[Math.floor(Math.random() * dataTypes.length)];
        baseSchema[columnName] = { type: dataType };
    }

    return new parquet.ParquetSchema(baseSchema);
}

function generateRandomValue(dataType) {
    switch (dataType) {
        case 'INT64':
            return Math.floor(Math.random() * 1000000);
        case 'DOUBLE':
            return parseFloat((Math.random() * 1000).toFixed(2));
        case 'BOOLEAN':
            return Math.random() > 0.5;
        case 'UTF8':
            return `RandomString_${Math.random().toString(36).substring(2, 10)}`;
        case 'TIMESTAMP_MILLIS':
            return randomDate(new Date(2020, 0, 1), new Date(2023, 11, 31));
        default:
            return null;
    }
}

function generateRandomProductName() {
    const adjectives = ['Premium', 'Deluxe', 'Basic', 'Super', 'Ultra', 'Eco', 'Smart', 'Pro'];
    const nouns = ['Widget', 'Gadget', 'Tool', 'Device', 'Accessory', 'Appliance', 'Gizmo', 'Component'];
    return `${adjectives[Math.floor(Math.random() * adjectives.length)]} ${nouns[Math.floor(Math.random() * nouns.length)]}`;
}

async function generateData(numRows, additionalColumnCount) {
    const baseSchema = {
        order_id: { type: 'INT64' },
        order_date: { type: 'TIMESTAMP_MILLIS' },
        customer_id: { type: 'INT64' },
        product_id: { type: 'INT64' },
        product_name: { type: 'UTF8' },
        quantity: { type: 'INT64' },
        product_price: { type: 'DOUBLE' },
        total_amount: { type: 'DOUBLE' }
    };

    const schema = generateAdditionalColumns(baseSchema, additionalColumnCount);

    // Create a new Parquet file writer
    const writer = await parquet.ParquetWriter.openFile(schema, PARQUET_FILE_PATH);

    const startDate = new Date(2020, 0, 1);
    const endDate = new Date(2023, 11, 31);

    for (let i = 0; i < numRows; i++) {
        const orderDate = randomDate(startDate, endDate);
        const quantity = Math.floor(Math.random() * 20) + 1;
        const productPrice = parseFloat((Math.random() * 999 + 1).toFixed(2));
        const totalAmount = quantity * productPrice;

        const row = {
            order_id: i + 1,
            order_date: orderDate,
            customer_id: Math.floor(Math.random() * 1000000) + 1,
            product_id: Math.floor(Math.random() * 10000) + 1,
            product_name: generateRandomProductName(),
            quantity: quantity,
            product_price: productPrice,
            total_amount: totalAmount
        };

        // Generate values for additional columns
        for (const [columnName, columnDef] of Object.entries(schema.fields)) {
            if (!(columnName in row)) {
                row[columnName] = generateRandomValue(columnDef.type);
            }
        }

        await writer.appendRow(row);

        if (i % 100000 === 0) {
            console.log(`Processed ${i} records`);
        }
    }
    await writer.close();
    console.log(`Parquet file "benchmark_data.parquet" has been generated with ${numRows} rows and ${Object.keys(schema.fields).length} columns.`);
    console.log('Schema:', schema.fields);
}

// Example usage
const numRows = 1000000; // 10 million rows
const additionalColumnCount = 0; // 10 additional columns

generateData(numRows, additionalColumnCount).catch(console.error);