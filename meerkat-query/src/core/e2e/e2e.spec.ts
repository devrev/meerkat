import { cubeQueryToSQL } from '@devrev/meerkat-node';
import { QueryBuilderImpl } from '../../query/implementations/query-builder-impl';
import { queryBuilderToCubeQuery } from '../../query/implementations/query-converter';
import { VisibilityType } from '../enums/visibility-type';
import { DatabaseImpl } from '../implementations/database-impl';
import { FieldImpl } from '../implementations/field-impl';
import { ModelImpl } from '../implementations/model-impl';
import { TableImpl } from '../implementations/table-impl';
import { FieldType, SemanticType } from '../interfaces/field';
import { RelationshipType } from '../interfaces/relationship';

describe('Database, Tables, and Models E2E Test', () => {
  let database: DatabaseImpl;
  let ordersTable: TableImpl;
  let customersTable: TableImpl;
  let revenueModel: ModelImpl;

  beforeAll(() => {
    // Step 1: Create database
    database = new DatabaseImpl({ id: 1, name: 'TestDB' });

    // Step 2: Add tables and models
    ordersTable = new TableImpl({
      id: 'orders',
      databaseId: 1,
      schema: 'public',
      name: 'orders',
      displayName: 'Orders',
      description: 'Table containing order information',
      entityType: 'Order',
      active: true,
      visibilityType: VisibilityType.NORMAL,
      fields: [
        new FieldImpl({
          id: 1,
          tableId: 1,
          tableName: 'orders',
          name: 'id',
          displayName: 'ID',
          description: 'Order ID',
          baseType: FieldType.INTEGER,
          semanticType: SemanticType.TYPE_PK,
          active: true,
          visibilityType: VisibilityType.NORMAL,
        }),
        new FieldImpl({
          id: 2,
          tableId: 1,
          tableName: 'orders',
          name: 'customer_id',
          displayName: 'Customer ID',
          description: 'Customer ID',
          baseType: FieldType.INTEGER,
          semanticType: SemanticType.TYPE_FK,
          active: true,
          visibilityType: VisibilityType.NORMAL,
        }),
        new FieldImpl({
          id: 3,
          tableId: 1,
          tableName: 'orders',
          name: 'order_date',
          displayName: 'Order Date',
          description: 'Date of the order',
          baseType: FieldType.DATE,
          semanticType: null,
          active: true,
          visibilityType: VisibilityType.NORMAL,
        }),
        new FieldImpl({
          id: 4,
          tableId: 1,
          tableName: 'orders',
          name: 'order_amount',
          displayName: 'Order Amount',
          description: 'Total amount of the order',
          baseType: FieldType.FLOAT,
          semanticType: null,
          active: true,
          visibilityType: VisibilityType.NORMAL,
        }),
      ],
      relationships: [],
    });

    customersTable = new TableImpl({
      id: 'customers',
      databaseId: 1,
      schema: 'public',
      name: 'customers',
      displayName: 'Customers',
      description: 'Table containing customer information',
      entityType: 'Customer',
      active: true,
      visibilityType: VisibilityType.NORMAL,
      fields: [
        new FieldImpl({
          id: 5,
          tableId: 2,
          tableName: 'customers',
          name: 'id',
          displayName: 'ID',
          description: 'Customer ID',
          baseType: FieldType.INTEGER,
          semanticType: SemanticType.TYPE_PK,
          active: true,
          visibilityType: VisibilityType.NORMAL,
        }),
        new FieldImpl({
          id: 6,
          tableId: 2,
          tableName: 'customers',
          name: 'name',
          displayName: 'Name',
          description: 'Customer name',
          baseType: FieldType.STRING,
          semanticType: null,
          active: true,
          visibilityType: VisibilityType.NORMAL,
        }),
        new FieldImpl({
          id: 7,
          tableId: 2,
          tableName: 'customers',
          name: 'email',
          displayName: 'Email',
          description: 'Customer email',
          baseType: FieldType.STRING,
          semanticType: null,
          active: true,
          visibilityType: VisibilityType.NORMAL,
        }),
      ],
      relationships: [],
    });

    revenueModel = new ModelImpl({
      id: 'revenue_model',
      databaseId: 1,
      schema: 'public',
      name: 'revenue_model',
      displayName: 'Revenue Model',
      description: 'Model for revenue analysis',
      entityType: 'Revenue',
      active: true,
      visibilityType: VisibilityType.NORMAL,
      fields: [
        new FieldImpl({
          id: 8,
          tableId: 3,
          tableName: 'revenue_model',
          name: 'total_revenue',
          displayName: 'Total Revenue',
          description: 'Sum of all order amounts',
          baseType: FieldType.FLOAT,
          semanticType: null,
          active: true,
          visibilityType: VisibilityType.NORMAL,
        }),
        new FieldImpl({
          id: 9,
          tableId: 3,
          tableName: 'revenue_model',
          name: 'order_count',
          displayName: 'Order Count',
          description: 'Number of orders',
          baseType: FieldType.INTEGER,
          semanticType: null,
          active: true,
          visibilityType: VisibilityType.NORMAL,
        }),
      ],
      relationships: [],
      query: new QueryBuilderImpl({
        sourceId: 'orders',
        sourceType: 'table',
        measures: [
          { type: 'sum', fieldId: 'order_amount' },
          { type: 'count', fieldId: 'id' },
        ],
        dimensions: [{ fieldId: 'order_date' }],
      }),
      lastMaterialized: new Date(),
    });

    database.addTable(ordersTable);
    database.addTable(customersTable);
    database.addTable(revenueModel);
  });

  test('List tables and models', () => {
    // Step 3: List tables & models
    const allTables = database.getAllTables();
    expect(allTables).toHaveLength(3);
    expect(allTables.map((t) => t.name)).toEqual([
      'orders',
      'customers',
      'revenue_model',
    ]);
  });

  test('Create query and add table', () => {
    // Step 4: Pick one table and add to query
    const queryBuilder = new QueryBuilderImpl({
      sourceId: 'orders',
      sourceType: 'table',
    });

    expect(queryBuilder.sourceId).toBe('orders');
    expect(queryBuilder.sourceType).toBe('table');
  });

  test('Add fields to query', () => {
    // Step 5: Pick fields of tables
    const queryBuilder = new QueryBuilderImpl({
      sourceId: 'orders',
      sourceType: 'table',
      measures: [{ type: 'sum', fieldId: 'order_amount' }],
      dimensions: [{ fieldId: 'order_date' }],
    });

    expect(queryBuilder.measures).toHaveLength(1);
    expect(queryBuilder.dimensions).toHaveLength(1);
  });

  test('List relationships between tables', () => {
    // Step 6: List relationship between 2 tables
    const ordersTable = database.getTable('orders');
    const customersTable = database.getTable('customers');

    if (ordersTable && customersTable) {
      const relationship = database.createRelationship(
        ordersTable.id,
        customersTable.id,
        'customer_id',
        'id',
        RelationshipType.MANY_TO_MANY
      );

      expect(relationship).not.toBeNull();
      expect(ordersTable.relationships).toHaveLength(1);
      expect(customersTable.relationships).toHaveLength(1);
    } else {
      fail('Tables not found');
    }
  });

  test('Join table in the query', () => {
    // Step 7: Join table in the query
    const queryBuilder = new QueryBuilderImpl({
      sourceId: 'orders',
      sourceType: 'table',
      measures: [{ type: 'sum', fieldId: 'order_amount' }],
      dimensions: [{ fieldId: 'order_date' }, { fieldId: 'customers.name' }],
      joins: [
        {
          sourceId: 'orders',
          sourceType: 'table',
          targetId: 'customers',
          targetType: 'table',
          type: 'inner',
          conditions: [
            { leftFieldId: 'customer_id', rightFieldId: 'id', operator: '=' },
          ],
        },
      ],
    });

    expect(queryBuilder.joins).toHaveLength(1);
    expect(queryBuilder.dimensions).toHaveLength(2);
    expect(queryBuilder.dimensions[1].fieldId).toBe('customers.name');
  });

  test('Convert QueryBuilder to CubeQuery and generate SQL', async () => {
    // Step 8: Create a complex query
    const queryBuilder = new QueryBuilderImpl({
      sourceId: ordersTable.id,
      sourceType: 'table',
      measures: [
        {
          type: 'sum',
          fieldId:
            ordersTable.getFieldByName('order_amount')?.referenceName || '',
        },
        {
          type: 'count',
          fieldId: ordersTable.getFieldByName('id')?.referenceName || '',
        },
      ],
      dimensions: [
        {
          fieldId:
            ordersTable.getFieldByName('order_date')?.referenceName || '',
        },
        {
          fieldId: customersTable.getFieldByName('name')?.referenceName || '',
        },
      ],
      joins: [
        {
          sourceId: ordersTable.id,
          sourceType: 'table',
          targetId: customersTable.id,
          targetType: 'table',
          type: 'inner',
          conditions: [
            {
              leftFieldId:
                ordersTable.getFieldByName('customer_id')?.referenceName || '',
              rightFieldId:
                customersTable.getFieldByName('id')?.referenceName || '',
              operator: '=',
            },
          ],
        },
      ],
      orderBy: [
        {
          fieldId:
            ordersTable.getFieldByName('order_amount')?.referenceName || '',
          direction: 'desc',
        },
      ],
      limit: 100,
    });

    console.info(JSON.stringify(queryBuilder.build(), null, 2));

    // Step 9: Convert QueryBuilder to CubeQuery
    const { query: cubeQuery, schemas } = queryBuilderToCubeQuery(
      queryBuilder.build()
    );

    console.info(JSON.stringify(cubeQuery, null, 2));
    console.info(JSON.stringify(schemas, null, 2));

    // Verify CubeQuery structure
    expect(cubeQuery.measures).toHaveLength(2);
    expect(cubeQuery.dimensions).toHaveLength(2);
    expect(cubeQuery.joinPaths).toHaveLength(1);
    expect(cubeQuery.order).toBeDefined();
    expect(cubeQuery.limit).toBe(100);

    // Verify schemas
    expect(schemas).toHaveLength(2); // orders and customers
    expect(schemas[0].name).toBe('orders');
    expect(schemas[1].name).toBe('customers');

    // Step 10: Generate SQL from CubeQuery
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    //@ts-ignore
    const sql = await cubeQueryToSQL(cubeQuery, schemas);

    const lowerCaseSql = sql.toLowerCase();

    // Verify SQL string
    expect(lowerCaseSql).toContain('select');
    expect(lowerCaseSql).toContain('from orders');
    expect(lowerCaseSql).toContain('join');
    expect(lowerCaseSql).toContain('group by');
    expect(lowerCaseSql).toContain('limit 100');

    console.log('Generated SQL:', lowerCaseSql);
  });
});
